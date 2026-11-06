"""Tests for delivery targets: outcome classification (no real network)."""

from __future__ import annotations

import json
import os
import uuid
from unittest.mock import MagicMock, patch

import httpx
import pika.exceptions
import pytest

from src import delivery
from src.config import settings
from src.delivery import FhirTarget, Outcome, RabbitMQTarget

URL = "https://downstream.example/fhir/$process-message"
BODY = b'{"resourceType":"Bundle","type":"message","id":"b1"}'


def fhir_target(handler, monkeypatch=None) -> tuple[FhirTarget, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def wrapped(request):
        seen.append(request)
        return handler(request)

    return FhirTarget(URL, transport=httpx.MockTransport(wrapped)), seen


def reply_bundle(code: str) -> dict:
    return {
        "resourceType": "Bundle", "type": "message",
        "entry": [{"resource": {"resourceType": "MessageHeader", "response": {"identifier": "b1", "code": code}}}],
    }


class TestFhirTarget:
    def test_requires_url(self, monkeypatch):
        monkeypatch.setattr(settings, "fhir_target_url", "")
        with pytest.raises(ValueError):
            FhirTarget()

    def test_posts_body_with_fhir_headers(self):
        target, seen = fhir_target(lambda r: httpx.Response(200))
        assert target.send("b1", "sys", BODY).outcome is Outcome.OK
        assert seen[0].method == "POST"
        assert str(seen[0].url) == URL
        assert seen[0].content == BODY
        assert seen[0].headers["content-type"] == "application/fhir+json"

    def test_bearer_token(self, monkeypatch):
        monkeypatch.setattr(settings, "fhir_auth_token", "tok")
        target, seen = fhir_target(lambda r: httpx.Response(200))
        target.send("b1", "sys", BODY)
        assert seen[0].headers["authorization"] == "Bearer tok"

    def test_basic_auth(self, monkeypatch):
        monkeypatch.setattr(settings, "fhir_auth_user", "u")
        monkeypatch.setattr(settings, "fhir_auth_password", "p")
        target, seen = fhir_target(lambda r: httpx.Response(200))
        target.send("b1", "sys", BODY)
        assert seen[0].headers["authorization"].startswith("Basic ")

    @pytest.mark.parametrize("status", [200, 201, 202, 204])
    def test_2xx_ok(self, status):
        target, _ = fhir_target(lambda r: httpx.Response(status))
        assert target.send("b1", "sys", BODY).outcome is Outcome.OK

    @pytest.mark.parametrize("status", [400, 409, 410, 413, 415, 422])
    def test_bundle_rejections(self, status):
        target, _ = fhir_target(lambda r: httpx.Response(status, text="bad bundle"))
        result = target.send("b1", "sys", BODY)
        assert result.outcome is Outcome.REJECTED
        assert str(status) in result.detail and "bad bundle" in result.detail

    @pytest.mark.parametrize("status", [401, 403, 404, 405, 408, 429, 500, 502, 503, 504])
    def test_endpoint_and_server_errors_are_retried(self, status):
        target, _ = fhir_target(lambda r: httpx.Response(status))
        assert target.send("b1", "sys", BODY).outcome is Outcome.RETRY

    def test_network_error_retried(self):
        def boom(request):
            raise httpx.ConnectError("refused")
        target, _ = fhir_target(boom)
        assert target.send("b1", "sys", BODY).outcome is Outcome.RETRY

    def test_timeout_retried(self):
        def slow(request):
            raise httpx.ReadTimeout("timeout")
        target, _ = fhir_target(slow)
        assert target.send("b1", "sys", BODY).outcome is Outcome.RETRY

    def test_reply_transient_error_retried(self):
        target, _ = fhir_target(lambda r: httpx.Response(200, json=reply_bundle("transient-error")))
        assert target.send("b1", "sys", BODY).outcome is Outcome.RETRY

    def test_reply_fatal_error_rejected(self):
        target, _ = fhir_target(lambda r: httpx.Response(200, json=reply_bundle("fatal-error")))
        assert target.send("b1", "sys", BODY).outcome is Outcome.REJECTED

    def test_reply_ok(self):
        target, _ = fhir_target(lambda r: httpx.Response(200, json=reply_bundle("ok")))
        assert target.send("b1", "sys", BODY).outcome is Outcome.OK


class TestRabbitMQTargetMocked:
    @pytest.fixture
    def channel(self):
        with patch.object(delivery.pika, "BlockingConnection") as conn_cls:
            ch = MagicMock()
            ch.is_closed = False
            conn_cls.return_value.channel.return_value = ch
            yield ch

    def test_publish_uses_confirms_mandatory_and_message_id(self, channel):
        target = RabbitMQTarget("amqp://x", "ex")
        result = target.send("b1", "https://a.example/CodeSystem/X", BODY)
        assert result.outcome is Outcome.OK
        channel.confirm_delivery.assert_called_once()
        kwargs = channel.basic_publish.call_args.kwargs
        assert kwargs["mandatory"] is True
        assert kwargs["routing_key"] == "codesystem.https---a-example-codesystem-x.changed"
        assert kwargs["properties"].message_id == "b1"
        assert kwargs["properties"].delivery_mode == 2

    def test_unroutable_is_retried(self, channel):
        channel.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        result = RabbitMQTarget("amqp://x", "ex").send("b1", "sys", BODY)
        assert result.outcome is Outcome.RETRY
        assert "unroutable" in result.detail

    def test_nack_is_retried(self, channel):
        channel.basic_publish.side_effect = pika.exceptions.NackError([])
        assert RabbitMQTarget("amqp://x", "ex").send("b1", "sys", BODY).outcome is Outcome.RETRY

    def test_connection_failure_is_retried(self):
        with patch.object(delivery.pika, "BlockingConnection",
                          side_effect=pika.exceptions.AMQPConnectionError("down")):
            assert RabbitMQTarget("amqp://x", "ex").send("b1", "sys", BODY).outcome is Outcome.RETRY


# ── Optional: against a real RabbitMQ (TEST_RABBITMQ_URL) ────────────────

@pytest.fixture
def rabbit_url():
    url = os.getenv("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("TEST_RABBITMQ_URL not set")
    return url


class TestRabbitMQTargetReal:
    def test_unroutable_then_routed(self, rabbit_url):
        exchange = f"test.{uuid.uuid4().hex[:8]}"
        target = RabbitMQTarget(rabbit_url, exchange)
        try:
            # Nothing bound yet: must NOT count as delivered
            assert target.send("b1", "https://a.example/CS", BODY).outcome is Outcome.RETRY

            conn = pika.BlockingConnection(pika.URLParameters(rabbit_url))
            ch = conn.channel()
            q = ch.queue_declare(queue="", exclusive=True).method.queue
            ch.queue_bind(queue=q, exchange=exchange, routing_key="codesystem.#")

            assert target.send("b1", "https://a.example/CS", BODY).outcome is Outcome.OK
            method, props, body = ch.basic_get(q, auto_ack=True)
            assert body == BODY
            assert props.message_id == "b1"
            ch.exchange_delete(exchange)
            conn.close()
        finally:
            target.close()

    def test_unreachable_broker_is_retried(self):
        target = RabbitMQTarget("amqp://guest:guest@127.0.0.1:1/", "ex")
        assert target.send("b1", "sys", BODY).outcome is Outcome.RETRY
