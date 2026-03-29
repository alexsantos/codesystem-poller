"""
FHIR Message Forwarder: consumes FHIR message Bundles from RabbitMQ
and POSTs them to a downstream FHIR $process-message endpoint.

This is a standalone consumer — run it alongside the poller service.
It binds to the same RabbitMQ exchange and forwards every Bundle it
receives to one or more FHIR endpoints.

Environment variables:
    RABBITMQ_URL            amqp://guest:guest@rabbitmq:5672/
    RABBITMQ_EXCHANGE       codesystem.changes
    RABBITMQ_QUEUE          fhir-forwarder
    RABBITMQ_ROUTING_KEY    codesystem.#
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

# ── Configuration ────────────────────────────────────────────────────────

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "codesystem.changes")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "fhir-forwarder")
RABBITMQ_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY", "codesystem.#")
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

def post_bundle(bundle_json: bytes) -> bool:
    """
    POST a FHIR message Bundle to the downstream $process-message endpoint.
    Retries with exponential backoff on transient failures.
    Returns True if accepted (2xx), False otherwise.
    """
    headers = {
        "Content-Type": "application/fhir+json",
        "Accept": "application/fhir+json",
    }
    if FHIR_AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {FHIR_AUTH_TOKEN}"

    auth = (FHIR_AUTH_USER, FHIR_AUTH_PASSWORD) if FHIR_AUTH_USER and FHIR_AUTH_PASSWORD else None

    delay = RETRY_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=30, auth=auth) as client:
                resp = client.post(
                    FHIR_TARGET_URL,
                    content=bundle_json,
                    headers=headers,
                )

            if 200 <= resp.status_code < 300:
                logger.info(
                    "Bundle accepted by downstream (HTTP %d)", resp.status_code
                )
                return True

            # 4xx errors are not retryable (bad request, auth failure, etc.)
            if 400 <= resp.status_code < 500:
                logger.error(
                    "Downstream rejected Bundle (HTTP %d): %s",
                    resp.status_code,
                    resp.text[:500],
                )
                return False

            # 5xx — transient, retry
            logger.warning(
                "Downstream returned HTTP %d (attempt %d/%d), retrying in %ds",
                resp.status_code, attempt, MAX_RETRIES, delay,
            )

        except httpx.RequestError as exc:
            logger.warning(
                "Request to downstream failed (attempt %d/%d): %s, retrying in %ds",
                attempt, MAX_RETRIES, exc, delay,
            )

        time.sleep(delay)
        delay *= 2  # exponential backoff

    logger.error("All %d attempts failed, Bundle will be nacked", MAX_RETRIES)
    return False


# ── RabbitMQ consumer ────────────────────────────────────────────────────

def on_message(channel, method, properties, body):
    """
    Callback for each RabbitMQ message.
    Attempts to forward the Bundle to the FHIR endpoint.
    ACKs on success, NACKs (with requeue) on failure.
    """
    try:
        # Quick validation — is this actually a FHIR Bundle?
        parsed = json.loads(body)
        resource_type = parsed.get("resourceType")
        bundle_type = parsed.get("type")
        entry_count = len(parsed.get("entry", []))

        if resource_type != "Bundle" or bundle_type != "message":
            logger.warning(
                "Skipping non-Bundle message: resourceType=%s, type=%s",
                resource_type, bundle_type,
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        logger.info(
            "Received Bundle (id=%s, %d entries) via routing key '%s'",
            parsed.get("id", "?"), entry_count, method.routing_key,
        )

        success = post_bundle(body)

        if success:
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            # Nack with requeue — the message goes back to the queue.
            # A dead-letter exchange (DLX) is recommended in production
            # to avoid infinite requeue loops (see README).
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    except json.JSONDecodeError:
        logger.error("Received non-JSON message, discarding")
        channel.basic_ack(delivery_tag=method.delivery_tag)
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

            # Prefetch 1 — process one message at a time to avoid
            # overwhelming the downstream endpoint
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=on_message,
            )

            logger.info("Waiting for messages...")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as exc:
            logger.warning("RabbitMQ connection lost: %s, reconnecting in 5s", exc)
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutting down")
            break
        finally:
            if connection is not None and not connection.is_closed:
                try:
                    connection.close()
                except Exception:
                    pass


if __name__ == "__main__":
    run_consumer()
