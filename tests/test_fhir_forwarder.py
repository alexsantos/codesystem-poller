"""
Tests for the fhir_forwarder module.

post_bundle() and on_message() are tested with mocks — no real HTTP or
RabbitMQ connections are made.
"""

import json
from unittest.mock import MagicMock, call, patch

import os
import uuid

import httpx
import pika
import pika.exceptions
import pytest

import src.fhir_forwarder as fwd
from src.delivery import DeliveryResult, Outcome
from src.fhir_forwarder import _redact_url, on_message, post_bundle

OK = DeliveryResult(Outcome.OK, "HTTP 200")
RETRY = DeliveryResult(Outcome.RETRY, "HTTP 503: down")
REJECTED = DeliveryResult(Outcome.REJECTED, "HTTP 422: invalid code")


# ── Fixtures ──────────────────────────────────────────────────────────────

VALID_BUNDLE = json.dumps({
    "resourceType": "Bundle",
    "type": "message",
    "id": "test-bundle-id",
    "entry": [
        {"resource": {"resourceType": "MessageHeader"}},
        {"resource": {"resourceType": "Parameters"}},
    ],
}).encode()


def _mock_channel(delivery_tag: int = 1):
    channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = delivery_tag
    method.routing_key = "codesystem.test.changed"
    return channel, method


def _make_response(status_code: int, text: str = "") -> httpx.Response:
    return httpx.Response(status_code, text=text)


# ── _redact_url ───────────────────────────────────────────────────────────

class TestRedactUrl:
    def test_amqp_credentials_stripped(self):
        url = "amqp://user:secret@rabbitmq:5672/"
        result = _redact_url(url)
        assert "user" not in result
        assert "secret" not in result
        assert "rabbitmq" in result
        assert "5672" in result

    def test_http_credentials_stripped(self):
        url = "http://admin:pass@fhir.example.org/fhir"
        result = _redact_url(url)
        assert "admin" not in result
        assert "pass" not in result
        assert "fhir.example.org" in result

    def test_url_without_credentials_unchanged(self):
        url = "amqp://rabbitmq:5672/"
        assert _redact_url(url) == url

    def test_https_without_credentials_unchanged(self):
        url = "https://fhir.example.org/fhir/$process-message"
        assert _redact_url(url) == url


# ── post_bundle ───────────────────────────────────────────────────────────

class TestPostBundle:
    """Tests for post_bundle() with mocked httpx.Client and time.sleep."""

    def setup_method(self):
        # Patch module-level globals for isolation
        self._patches = [
            patch.object(fwd, "FHIR_TARGET_URL", "https://fhir.example.org/fhir/$process-message"),
            patch.object(fwd, "FHIR_AUTH_TOKEN", ""),
            patch.object(fwd, "FHIR_AUTH_USER", ""),
            patch.object(fwd, "FHIR_AUTH_PASSWORD", ""),
            patch.object(fwd, "MAX_RETRIES", 3),
            patch.object(fwd, "RETRY_DELAY", 0),  # no sleep in tests
            patch("src.fhir_forwarder.time.sleep"),
        ]
        for p in self._patches:
            p.start()

    def teardown_method(self):
        for p in self._patches:
            p.stop()

    def _mock_client(self, responses: list):
        """Return a context manager mock that yields successive responses."""
        client_mock = MagicMock()
        client_mock.__enter__ = MagicMock(return_value=client_mock)
        client_mock.__exit__ = MagicMock(return_value=False)
        client_mock.post.side_effect = responses
        return client_mock

    @pytest.mark.parametrize("status", [200, 201, 202, 204])
    def test_2xx_ok(self, status):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(status)])
            assert post_bundle(VALID_BUNDLE).outcome is Outcome.OK

    @pytest.mark.parametrize("status", [400, 409, 410, 413, 415, 422])
    def test_bundle_rejection_not_retried(self, status):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(status, "bad request")])
            result = post_bundle(VALID_BUNDLE)
            assert result.outcome is Outcome.REJECTED
            assert str(status) in result.detail and "bad request" in result.detail
            assert MockClient.return_value.post.call_count == 1

    def test_fatal_error_reply_rejected(self):
        reply = json.dumps({"resourceType": "Bundle", "type": "message", "entry": [
            {"resource": {"resourceType": "MessageHeader", "response": {"code": "fatal-error"}}}]})
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(200, reply)])
            assert post_bundle(VALID_BUNDLE).outcome is Outcome.REJECTED

    @pytest.mark.parametrize("status", [401, 403, 404, 429])
    def test_endpoint_4xx_retried_not_rejected(self, status):
        # Auth/URL/rate-limit problems affect every Bundle: retry, never dead-letter
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(status)] * 3)
            assert post_bundle(VALID_BUNDLE).outcome is Outcome.RETRY
            assert MockClient.return_value.post.call_count == 3

    def test_500_retries_and_succeeds(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([
                _make_response(500),
                _make_response(200),
            ])
            assert post_bundle(VALID_BUNDLE).outcome is Outcome.OK
            assert MockClient.return_value.post.call_count == 2

    def test_all_retries_exhausted_returns_retry(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([
                _make_response(500),
                _make_response(500),
                _make_response(500),
            ])
            assert post_bundle(VALID_BUNDLE).outcome is Outcome.RETRY
            assert MockClient.return_value.post.call_count == 3

    def test_request_error_retries(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([
                httpx.RequestError("connection refused"),
                _make_response(200),
            ])
            assert post_bundle(VALID_BUNDLE).outcome is Outcome.OK

    def test_request_error_all_retries_returns_retry(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([
                httpx.RequestError("timeout"),
                httpx.RequestError("timeout"),
                httpx.RequestError("timeout"),
            ])
            assert post_bundle(VALID_BUNDLE).outcome is Outcome.RETRY

    def test_bearer_token_added_to_headers(self):
        with patch.object(fwd, "FHIR_AUTH_TOKEN", "my-token"):
            with patch("src.fhir_forwarder.httpx.Client") as MockClient:
                MockClient.return_value = self._mock_client([_make_response(200)])
                post_bundle(VALID_BUNDLE)
                _, kwargs = MockClient.return_value.post.call_args
                headers = kwargs.get("headers", {})
                assert headers.get("Authorization") == "Bearer my-token"

    def test_basic_auth_passed_to_client(self):
        with patch.object(fwd, "FHIR_AUTH_USER", "user"), \
             patch.object(fwd, "FHIR_AUTH_PASSWORD", "pass"):
            with patch("src.fhir_forwarder.httpx.Client") as MockClient:
                MockClient.return_value = self._mock_client([_make_response(200)])
                post_bundle(VALID_BUNDLE)
                _, kwargs = MockClient.call_args
                assert kwargs.get("auth") == ("user", "pass")

    def test_no_auth_when_not_configured(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(200)])
            post_bundle(VALID_BUNDLE)
            _, kwargs = MockClient.call_args
            assert kwargs.get("auth") is None

    def test_content_type_header_set(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(200)])
            post_bundle(VALID_BUNDLE)
            _, kwargs = MockClient.return_value.post.call_args
            assert kwargs["headers"]["Content-Type"] == "application/fhir+json"


# ── on_message ────────────────────────────────────────────────────────────

class TestOnMessage:
    """Tests for on_message() with mocked channel, method, and post_bundle."""

    def test_valid_bundle_success_acks(self):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle", return_value=OK):
            on_message(channel, method, None, VALID_BUNDLE)
        channel.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)
        channel.basic_nack.assert_not_called()
        channel.basic_publish.assert_not_called()

    def test_transient_failure_nacks_with_requeue(self):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle", return_value=RETRY):
            on_message(channel, method, None, VALID_BUNDLE)
        channel.basic_nack.assert_called_once_with(
            delivery_tag=method.delivery_tag, requeue=True
        )
        channel.basic_ack.assert_not_called()
        channel.basic_publish.assert_not_called()

    def test_rejected_bundle_is_dead_lettered_then_acked(self):
        channel, method = _mock_channel()
        props = pika.BasicProperties(message_id="test-bundle-id", headers={"keep": "me"})
        with patch("src.fhir_forwarder.post_bundle", return_value=REJECTED):
            on_message(channel, method, props, VALID_BUNDLE)
        kwargs = channel.basic_publish.call_args.kwargs
        assert kwargs["exchange"] == ""
        assert kwargs["routing_key"] == fwd.RABBITMQ_DEAD_QUEUE
        assert kwargs["body"] == VALID_BUNDLE
        assert kwargs["mandatory"] is True
        out = kwargs["properties"]
        assert out.message_id == "test-bundle-id"
        assert out.delivery_mode == 2
        assert out.headers["x-reject-reason"] == "HTTP 422: invalid code"
        assert out.headers["x-original-routing-key"] == "codesystem.test.changed"
        assert out.headers["keep"] == "me"
        channel.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)
        channel.basic_nack.assert_not_called()
        # Published before ACK
        names = [c[0] for c in channel.mock_calls]
        assert names.index("basic_publish") < names.index("basic_ack")

    @pytest.mark.parametrize("error", [
        pika.exceptions.UnroutableError([]),
        pika.exceptions.NackError([]),
    ])
    def test_rejected_but_dead_letter_unconfirmed_requeues(self, error):
        channel, method = _mock_channel()
        channel.basic_publish.side_effect = error
        with patch("src.fhir_forwarder.post_bundle", return_value=REJECTED):
            on_message(channel, method, None, VALID_BUNDLE)
        channel.basic_ack.assert_not_called()
        channel.basic_nack.assert_called_once_with(
            delivery_tag=method.delivery_tag, requeue=True
        )

    def test_invalid_json_is_dead_lettered(self):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle") as mock_post:
            on_message(channel, method, None, b"not-json{{{")
        mock_post.assert_not_called()
        assert channel.basic_publish.call_args.kwargs["properties"].headers["x-reject-reason"] == "not valid JSON"
        channel.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)

    @pytest.mark.parametrize("body", [
        {"resourceType": "Patient", "type": "message"},
        {"resourceType": "Bundle", "type": "collection"},
        ["not", "an", "object"],
    ])
    def test_non_message_bundle_is_dead_lettered(self, body):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle") as mock_post:
            on_message(channel, method, None, json.dumps(body).encode())
        mock_post.assert_not_called()
        reason = channel.basic_publish.call_args.kwargs["properties"].headers["x-reject-reason"]
        assert "not a FHIR message Bundle" in reason
        channel.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)

    def test_unexpected_exception_nacks_with_requeue(self):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle", side_effect=RuntimeError("boom")):
            on_message(channel, method, None, VALID_BUNDLE)
        channel.basic_nack.assert_called_once_with(
            delivery_tag=method.delivery_tag, requeue=True
        )

    def test_connection_error_propagates_for_reconnect(self):
        channel, method = _mock_channel()
        channel.basic_publish.side_effect = pika.exceptions.StreamLostError("gone")
        with patch("src.fhir_forwarder.post_bundle", return_value=REJECTED):
            with pytest.raises(pika.exceptions.AMQPError):
                on_message(channel, method, None, VALID_BUNDLE)
        channel.basic_ack.assert_not_called()

    def test_post_bundle_called_with_raw_body(self):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle", return_value=OK) as mock_post:
            on_message(channel, method, None, VALID_BUNDLE)
        mock_post.assert_called_once_with(VALID_BUNDLE)


# ── Optional: against a real RabbitMQ (TEST_RABBITMQ_URL) ────────────────

class TestDeadLetterReal:
    def test_rejected_bundle_lands_in_dead_queue(self):
        url = os.getenv("TEST_RABBITMQ_URL")
        if not url:
            pytest.skip("TEST_RABBITMQ_URL not set")
        suffix = uuid.uuid4().hex[:8]
        queue, dead = f"test-fwd-{suffix}", f"test-fwd-{suffix}.dead"
        conn = pika.BlockingConnection(pika.URLParameters(url))
        ch = conn.channel()
        try:
            ch.queue_declare(queue=queue, durable=True)
            ch.queue_declare(queue=dead, durable=True)
            ch.confirm_delivery()  # as run_consumer does, on the consuming channel
            ch.basic_publish("", queue, VALID_BUNDLE,
                             pika.BasicProperties(message_id="test-bundle-id"))

            method, props, body = ch.basic_get(queue)
            with patch.object(fwd, "RABBITMQ_DEAD_QUEUE", dead), \
                 patch("src.fhir_forwarder.post_bundle", return_value=REJECTED):
                on_message(ch, method, props, body)

            assert ch.basic_get(queue, auto_ack=True)[0] is None  # original ACKed
            d_method, d_props, d_body = ch.basic_get(dead, auto_ack=True)
            assert d_body == VALID_BUNDLE
            assert d_props.message_id == "test-bundle-id"
            assert d_props.headers["x-reject-reason"] == "HTTP 422: invalid code"
        finally:
            ch.queue_delete(queue)
            ch.queue_delete(dead)
            conn.close()
