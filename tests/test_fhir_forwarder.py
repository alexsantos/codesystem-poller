"""
Tests for the fhir_forwarder module.

post_bundle() and on_message() are tested with mocks — no real HTTP or
RabbitMQ connections are made.
"""

import json
from unittest.mock import MagicMock, call, patch

import httpx
import pytest

import src.fhir_forwarder as fwd
from src.fhir_forwarder import _redact_url, on_message, post_bundle


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


def _make_response(status_code: int, text: str = "") -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    return resp


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

    def test_200_returns_true(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(200)])
            assert post_bundle(VALID_BUNDLE) is True

    def test_201_returns_true(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(201)])
            assert post_bundle(VALID_BUNDLE) is True

    def test_400_returns_false_no_retry(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(400, "bad request")])
            assert post_bundle(VALID_BUNDLE) is False
            # Only one attempt made
            assert MockClient.return_value.post.call_count == 1

    def test_401_returns_false_no_retry(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([_make_response(401)])
            assert post_bundle(VALID_BUNDLE) is False
            assert MockClient.return_value.post.call_count == 1

    def test_500_retries_and_succeeds(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([
                _make_response(500),
                _make_response(200),
            ])
            assert post_bundle(VALID_BUNDLE) is True
            assert MockClient.return_value.post.call_count == 2

    def test_all_retries_exhausted_returns_false(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([
                _make_response(500),
                _make_response(500),
                _make_response(500),
            ])
            assert post_bundle(VALID_BUNDLE) is False
            assert MockClient.return_value.post.call_count == 3

    def test_request_error_retries(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([
                httpx.RequestError("connection refused"),
                _make_response(200),
            ])
            assert post_bundle(VALID_BUNDLE) is True

    def test_request_error_all_retries_returns_false(self):
        with patch("src.fhir_forwarder.httpx.Client") as MockClient:
            MockClient.return_value = self._mock_client([
                httpx.RequestError("timeout"),
                httpx.RequestError("timeout"),
                httpx.RequestError("timeout"),
            ])
            assert post_bundle(VALID_BUNDLE) is False

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
        with patch("src.fhir_forwarder.post_bundle", return_value=True):
            on_message(channel, method, None, VALID_BUNDLE)
        channel.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)
        channel.basic_nack.assert_not_called()

    def test_valid_bundle_failure_nacks_with_requeue(self):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle", return_value=False):
            on_message(channel, method, None, VALID_BUNDLE)
        channel.basic_nack.assert_called_once_with(
            delivery_tag=method.delivery_tag, requeue=True
        )
        channel.basic_ack.assert_not_called()

    def test_invalid_json_acks_and_discards(self):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle") as mock_post:
            on_message(channel, method, None, b"not-json{{{")
        channel.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)
        mock_post.assert_not_called()

    def test_wrong_resource_type_acks_and_skips(self):
        channel, method = _mock_channel()
        body = json.dumps({"resourceType": "Patient", "type": "message"}).encode()
        with patch("src.fhir_forwarder.post_bundle") as mock_post:
            on_message(channel, method, None, body)
        channel.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)
        mock_post.assert_not_called()

    def test_wrong_bundle_type_acks_and_skips(self):
        channel, method = _mock_channel()
        body = json.dumps({"resourceType": "Bundle", "type": "collection"}).encode()
        with patch("src.fhir_forwarder.post_bundle") as mock_post:
            on_message(channel, method, None, body)
        channel.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)
        mock_post.assert_not_called()

    def test_unexpected_exception_nacks_with_requeue(self):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle", side_effect=RuntimeError("boom")):
            on_message(channel, method, None, VALID_BUNDLE)
        channel.basic_nack.assert_called_once_with(
            delivery_tag=method.delivery_tag, requeue=True
        )

    def test_post_bundle_called_with_raw_body(self):
        channel, method = _mock_channel()
        with patch("src.fhir_forwarder.post_bundle", return_value=True) as mock_post:
            on_message(channel, method, None, VALID_BUNDLE)
        mock_post.assert_called_once_with(VALID_BUNDLE)
