"""
Delivery targets for the outbox relay.

Each target sends one frozen FHIR message Bundle and classifies the outcome:

  OK        the target has durably accepted the Bundle
  RETRY     transient failure; the same Bundle is retried later (forever)
  REJECTED  the target refused this Bundle; retrying it unchanged will not help

Targets never raise for delivery problems; they return a DeliveryResult.
"""

from __future__ import annotations

import enum
import json
import logging
import re
from dataclasses import dataclass
from typing import Protocol

import httpx
import pika
import pika.exceptions

from src.config import settings

logger = logging.getLogger(__name__)


class Outcome(enum.Enum):
    OK = "ok"
    RETRY = "retry"
    REJECTED = "rejected"


@dataclass
class DeliveryResult:
    outcome: Outcome
    detail: str = ""


class DeliveryTarget(Protocol):
    name: str

    def send(self, bundle_id: str, system_url: str, body: bytes) -> DeliveryResult: ...

    def close(self) -> None: ...


def slugify(url: str) -> str:
    """Turn a canonical URL into a routing-key-safe slug."""
    return re.sub(r"[^a-zA-Z0-9]", "-", url).strip("-").lower()


# ── RabbitMQ ─────────────────────────────────────────────────────────────

class RabbitMQTarget:
    """
    Publishes to the topic exchange with publisher confirms and mandatory=True:
    OK only once the broker has confirmed the message AND routed it to at
    least one queue. An unroutable message (no queue bound) is retried rather
    than silently dropped.
    """

    name = "rabbitmq"

    def __init__(self, url: str | None = None, exchange: str | None = None) -> None:
        self._url = url or settings.rabbitmq_url
        self._exchange = exchange or settings.rabbitmq_exchange
        self._connection: pika.BlockingConnection | None = None
        self._channel = None

    def _ensure_channel(self):
        if self._channel is None or self._channel.is_closed:
            self.close()
            self._connection = pika.BlockingConnection(pika.URLParameters(self._url))
            channel = self._connection.channel()
            channel.exchange_declare(exchange=self._exchange, exchange_type="topic", durable=True)
            channel.confirm_delivery()
            self._channel = channel
        return self._channel

    def send(self, bundle_id: str, system_url: str, body: bytes) -> DeliveryResult:
        routing_key = f"codesystem.{slugify(system_url)}.changed"
        try:
            channel = self._ensure_channel()
            channel.basic_publish(
                exchange=self._exchange,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(
                    content_type="application/fhir+json",
                    delivery_mode=2,  # persistent
                    message_id=bundle_id,
                ),
                mandatory=True,
            )
        except pika.exceptions.UnroutableError:
            return DeliveryResult(
                Outcome.RETRY,
                f"unroutable: no queue bound to {self._exchange} for routing key {routing_key}",
            )
        except pika.exceptions.NackError:
            return DeliveryResult(Outcome.RETRY, "broker nacked the message")
        except pika.exceptions.AMQPError as exc:
            self.close()
            return DeliveryResult(Outcome.RETRY, f"RabbitMQ error: {type(exc).__name__}: {exc}")
        except OSError as exc:
            self.close()
            return DeliveryResult(Outcome.RETRY, f"RabbitMQ connection error: {exc}")
        return DeliveryResult(Outcome.OK, f"{self._exchange}/{routing_key}")

    def close(self) -> None:
        conn, self._connection, self._channel = self._connection, None, None
        if conn is not None and conn.is_open:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Error closing RabbitMQ connection: %s", exc)


# ── FHIR $process-message ────────────────────────────────────────────────

# 4xx statuses that mean "this Bundle is not acceptable"; resending it will
# not help. Any other 4xx (401/403/404/405/408/429, ...) points at the
# endpoint or credentials, affects every Bundle equally, and is retried.
REJECT_STATUSES = frozenset({400, 409, 410, 413, 415, 422})


def _message_response_code(text: str) -> str | None:
    """MessageHeader.response.code of a synchronous $process-message reply, if any."""
    try:
        reply = json.loads(text)
    except ValueError:
        return None
    if not isinstance(reply, dict) or reply.get("resourceType") != "Bundle":
        return None
    for entry in reply.get("entry") or []:
        resource = entry.get("resource") or {}
        if resource.get("resourceType") == "MessageHeader":
            return (resource.get("response") or {}).get("code")
    return None


def classify_response(resp: httpx.Response) -> DeliveryResult:
    """
    Classify a $process-message HTTP response (shared by the relay's FhirTarget
    and the RabbitMQ forwarder).

    2xx is OK unless the reply's MessageHeader.response.code says otherwise.
    REJECT_STATUSES mean the Bundle itself was refused. Every other status
    (401/403/404/408/429/5xx…) is an endpoint or configuration problem that
    affects all Bundles alike, so it is retried rather than dead-lettered.
    """
    status = resp.status_code
    snippet = resp.text[:500]
    if 200 <= status < 300:
        code = _message_response_code(resp.text)
        if code == "transient-error":
            return DeliveryResult(Outcome.RETRY, f"HTTP {status}, response code transient-error: {snippet}")
        if code == "fatal-error":
            return DeliveryResult(Outcome.REJECTED, f"HTTP {status}, response code fatal-error: {snippet}")
        return DeliveryResult(Outcome.OK, f"HTTP {status}")
    if status in REJECT_STATUSES:
        return DeliveryResult(Outcome.REJECTED, f"HTTP {status}: {snippet}")
    return DeliveryResult(Outcome.RETRY, f"HTTP {status}: {snippet}")


class FhirTarget:
    """POSTs the Bundle to a FHIR $process-message endpoint."""

    name = "fhir"

    def __init__(
        self,
        url: str | None = None,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._url = url or settings.fhir_target_url
        if not self._url:
            raise ValueError("FHIR_TARGET_URL must be set when RELAY_TARGET=fhir")
        headers = {"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"}
        if settings.fhir_auth_token:
            headers["Authorization"] = f"Bearer {settings.fhir_auth_token}"
        auth = None
        if settings.fhir_auth_user and settings.fhir_auth_password:
            auth = (settings.fhir_auth_user, settings.fhir_auth_password)
        self._client = httpx.Client(
            timeout=settings.http_timeout, headers=headers, auth=auth, transport=transport,
        )

    def send(self, bundle_id: str, system_url: str, body: bytes) -> DeliveryResult:
        try:
            resp = self._client.post(self._url, content=body)
        except httpx.RequestError as exc:
            return DeliveryResult(Outcome.RETRY, f"{type(exc).__name__}: {exc}")

        return classify_response(resp)

    def close(self) -> None:
        self._client.close()


def create_target() -> DeliveryTarget:
    if settings.relay_target == "fhir":
        return FhirTarget()
    return RabbitMQTarget()
