"""
FHIR Message Forwarder: consumes FHIR message Bundles from RabbitMQ
and POSTs them to a downstream FHIR $process-message endpoint.

This is a standalone consumer — run it alongside the poller service.
It binds to the same RabbitMQ exchange and forwards every Bundle it
receives to one or more FHIR endpoints.

Failure handling (same classification as the relay's FhirTarget):
  - 2xx                     → ACK
  - Bundle rejected         → copied to the dead-letter queue <RABBITMQ_QUEUE>.dead
    (400/409/410/413/415/     with the reason in headers, then ACKed. The ACK
    422, fatal-error)         happens only after the broker confirms the copy,
                              so a rejected Bundle is never lost.
  - Anything else (5xx,     → retried MAX_RETRIES times in-process, then NACKed
    401/403/404/429,          with requeue (the endpoint, not the Bundle, is
    network errors)           the problem; order is kept with prefetch 1)
  - Not a FHIR message Bundle / not JSON → dead-lettered as well

Environment variables:
    RABBITMQ_URL            amqp://guest:guest@rabbitmq:5672/
    RABBITMQ_EXCHANGE       codesystem.changes
    RABBITMQ_QUEUE          fhir-forwarder
    RABBITMQ_ROUTING_KEY    codesystem.#
    RABBITMQ_DEAD_QUEUE     <RABBITMQ_QUEUE>.dead
    FHIR_TARGET_URL         https://downstream.example/fhir/$process-message
    FHIR_AUTH_TOKEN         (optional) Bearer token for the downstream endpoint
    MAX_RETRIES             3
    RETRY_DELAY             5  (seconds, doubles on each retry)
    LOG_LEVEL               INFO
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from urllib.parse import urlparse

import httpx
import pika
import pika.exceptions

from src.delivery import DeliveryResult, Outcome, classify_response

# ── Configuration ────────────────────────────────────────────────────────

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "codesystem.changes")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "fhir-forwarder")
RABBITMQ_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY", "codesystem.#")
RABBITMQ_DEAD_QUEUE = os.getenv("RABBITMQ_DEAD_QUEUE", "") or f"{RABBITMQ_QUEUE}.dead"
FHIR_TARGET_URL = os.getenv("FHIR_TARGET_URL", "")
FHIR_AUTH_TOKEN = os.getenv("FHIR_AUTH_TOKEN", "")
FHIR_AUTH_USER = os.getenv("FHIR_AUTH_USER", "")
FHIR_AUTH_PASSWORD = os.getenv("FHIR_AUTH_PASSWORD", "")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("fhir-forwarder")


def _redact_url(url: str) -> str:
    """Strip credentials from an AMQP/HTTP URL before logging."""
    parsed = urlparse(url)
    if parsed.username or parsed.password:
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        redacted = parsed._replace(netloc=f"{host}{port}")
        return redacted.geturl()
    return url


# ── FHIR POST with retries ──────────────────────────────────────────────

def post_bundle(bundle_json: bytes) -> DeliveryResult:
    """
    POST a FHIR message Bundle to the downstream $process-message endpoint.

    Retries RETRY outcomes with exponential backoff, up to MAX_RETRIES attempts.
    Returns OK, REJECTED (do not retry this Bundle) or RETRY (attempts exhausted).
    """
    headers = {
        "Content-Type": "application/fhir+json",
        "Accept": "application/fhir+json",
    }
    if FHIR_AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {FHIR_AUTH_TOKEN}"

    auth = (FHIR_AUTH_USER, FHIR_AUTH_PASSWORD) if FHIR_AUTH_USER and FHIR_AUTH_PASSWORD else None

    delay = RETRY_DELAY
    result = DeliveryResult(Outcome.RETRY, "no attempt made")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=30, auth=auth) as client:
                resp = client.post(
                    FHIR_TARGET_URL,
                    content=bundle_json,
                    headers=headers,
                )
            result = classify_response(resp)
        except httpx.RequestError as exc:
            result = DeliveryResult(Outcome.RETRY, f"{type(exc).__name__}: {exc}")

        if result.outcome is Outcome.OK:
            logger.info("Bundle accepted by downstream (%s)", result.detail)
            return result
        if result.outcome is Outcome.REJECTED:
            logger.error("Downstream rejected Bundle: %s", result.detail)
            return result

        if attempt < MAX_RETRIES:
            logger.warning(
                "Delivery failed (attempt %d/%d): %s, retrying in %ds",
                attempt, MAX_RETRIES, result.detail, delay,
            )
            time.sleep(delay)
            delay *= 2  # exponential backoff

    logger.error("All %d attempts failed, Bundle will be requeued: %s", MAX_RETRIES, result.detail)
    return result


# ── Dead-lettering ───────────────────────────────────────────────────────

def dead_letter(channel, method, properties, body: bytes, reason: str) -> bool:
    """
    Copy a message to the dead-letter queue with the reason in its headers.
    Returns True once the broker has confirmed it (the caller may then ACK the
    original), False if it could not be stored (the caller must NOT ACK).
    """
    props = properties if isinstance(properties, pika.BasicProperties) else None
    headers = dict(getattr(props, "headers", None) or {})
    headers.update({
        "x-reject-reason": reason[:1000],
        "x-original-exchange": method.exchange or "",
        "x-original-routing-key": method.routing_key or "",
        "x-rejected-at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })
    try:
        channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_DEAD_QUEUE,
            body=body,
            properties=pika.BasicProperties(
                content_type=getattr(props, "content_type", None) or "application/fhir+json",
                message_id=getattr(props, "message_id", None),
                delivery_mode=2,
                headers=headers,
            ),
            mandatory=True,
        )
    except (pika.exceptions.UnroutableError, pika.exceptions.NackError) as exc:
        logger.error("Could not dead-letter message to %s: %s", RABBITMQ_DEAD_QUEUE, exc)
        return False
    logger.error("Message dead-lettered to %s: %s", RABBITMQ_DEAD_QUEUE, reason)
    return True


# ── RabbitMQ consumer ────────────────────────────────────────────────────

def _reject(channel, method, properties, body: bytes, reason: str) -> None:
    """Dead-letter, then ACK; if dead-lettering failed, requeue instead (never drop)."""
    if dead_letter(channel, method, properties, body, reason):
        channel.basic_ack(delivery_tag=method.delivery_tag)
    else:
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def on_message(channel, method, properties, body):
    """
    Callback for each RabbitMQ message.
    ACK on success, dead-letter + ACK on rejection, NACK (requeue) on
    transient failure.
    """
    try:
        try:
            parsed = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            _reject(channel, method, properties, body, "not valid JSON")
            return

        resource_type = parsed.get("resourceType") if isinstance(parsed, dict) else None
        bundle_type = parsed.get("type") if isinstance(parsed, dict) else None
        if resource_type != "Bundle" or bundle_type != "message":
            _reject(
                channel, method, properties, body,
                f"not a FHIR message Bundle (resourceType={resource_type}, type={bundle_type})",
            )
            return

        logger.info(
            "Received Bundle (id=%s, %d entries) via routing key '%s'",
            parsed.get("id", "?"), len(parsed.get("entry", [])), method.routing_key,
        )

        result = post_bundle(body)

        if result.outcome is Outcome.OK:
            channel.basic_ack(delivery_tag=method.delivery_tag)
        elif result.outcome is Outcome.REJECTED:
            _reject(channel, method, properties, body, result.detail)
        else:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    except pika.exceptions.AMQPError:
        raise  # connection-level problem: let run_consumer reconnect (message is redelivered)
    except Exception as exc:
        logger.error("Unexpected error processing message: %s", exc, exc_info=True)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def run_consumer() -> None:
    """Connect to RabbitMQ, declare queue, bind, and start consuming."""
    if not FHIR_TARGET_URL:
        logger.error("FHIR_TARGET_URL is not set — cannot start forwarder")
        sys.exit(1)

    logger.info("FHIR Forwarder starting")
    logger.info("  RabbitMQ:     %s", _redact_url(RABBITMQ_URL))
    logger.info("  Exchange:     %s", RABBITMQ_EXCHANGE)
    logger.info("  Queue:        %s", RABBITMQ_QUEUE)
    logger.info("  Routing key:  %s", RABBITMQ_ROUTING_KEY)
    logger.info("  Dead letters: %s", RABBITMQ_DEAD_QUEUE)
    logger.info("  Target:       %s", FHIR_TARGET_URL)

    while True:
        connection = None
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            # Ensure the exchange exists (idempotent)
            channel.exchange_declare(
                exchange=RABBITMQ_EXCHANGE,
                exchange_type="topic",
                durable=True,
            )

            # Declare a durable queue for this consumer
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

            # Bind to the exchange with the configured routing key pattern
            channel.queue_bind(
                queue=RABBITMQ_QUEUE,
                exchange=RABBITMQ_EXCHANGE,
                routing_key=RABBITMQ_ROUTING_KEY,
            )

            # Rejected Bundles are copied here (with publisher confirms) before ACK
            channel.queue_declare(queue=RABBITMQ_DEAD_QUEUE, durable=True)
            channel.confirm_delivery()

            # Prefetch 1 — process one message at a time to avoid
            # overwhelming the downstream endpoint
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=on_message,
            )

            logger.info("Waiting for messages...")
            channel.start_consuming()

        except pika.exceptions.AMQPError as exc:
            logger.warning("RabbitMQ connection lost: %s, reconnecting in 5s", exc)
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutting down")
            break
        finally:
            if connection is not None and not connection.is_closed:
                try:
                    connection.close()
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Error closing RabbitMQ connection: %s", exc)


if __name__ == "__main__":
    run_consumer()
