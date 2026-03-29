"""
Outbox Relay: polls the change_outbox table for unpublished rows,
groups them by CodeSystem, builds a FHIR message Bundle per group,
publishes to RabbitMQ, and marks the rows as published.
"""

from __future__ import annotations

import json
import logging
import re
import time
from collections import defaultdict

import pika

from src.config import settings
from src.db import get_connection
from src.fhir_bundle import build_change_bundle

logger = logging.getLogger(__name__)


def _slugify(url: str) -> str:
    """Turn a canonical URL into a routing-key-safe slug."""
    return re.sub(r"[^a-zA-Z0-9]", "-", url).strip("-").lower()


def _get_rabbitmq_channel():
    """Open a blocking RabbitMQ connection and declare the exchange."""
    params = pika.URLParameters(settings.rabbitmq_url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(
        exchange=settings.rabbitmq_exchange,
        exchange_type="topic",
        durable=True,
    )
    return connection, channel


def _fetch_unpublished(cur) -> list[dict]:
    """Fetch all unpublished outbox rows, oldest first."""
    cur.execute(
        """
        SELECT id, system_url, change_type, code, old_value, new_value, created_at
        FROM poller.change_outbox
        WHERE published = false
        ORDER BY id ASC
        LIMIT 500
        """
    )
    return cur.fetchall()


def _group_by_system(rows: list[dict]) -> dict[str, list[dict]]:
    """Group outbox rows by system_url."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        groups[row["system_url"]].append(row)
    return groups


def _rows_to_bundle_inputs(rows: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Convert outbox rows back into the (added, modified, removed) shape
    expected by build_change_bundle.
    """
    added, modified, removed = [], [], []

    for row in rows:
        ct = row["change_type"]
        new_val = row["new_value"] or {}
        old_val = row["old_value"] or {}

        if ct == "concept_added":
            added.append(new_val)
        elif ct == "concept_removed":
            removed.append(old_val)
        elif ct == "concept_modified":
            # Reconstruct the changed_fields from old/new values
            changed_fields = {}
            for field in ("display", "definition", "properties", "parent_code"):
                ov = old_val.get(field)
                nv = new_val.get(field)
                if ov != nv:
                    changed_fields[field] = (ov, nv)
            modified.append({
                "code": row["code"],
                "old_concept": old_val,
                "new_concept": new_val,
                "changed_fields": changed_fields,
            })

    return added, modified, removed


def _mark_published(cur, row_ids: list[int]) -> None:
    """Mark outbox rows as published."""
    if not row_ids:
        return
    cur.execute(
        """
        UPDATE poller.change_outbox
        SET published = true, published_at = now()
        WHERE id = ANY(%s)
        """,
        (row_ids,),
    )


def relay_once() -> int:
    """
    Run one relay cycle:
      1. Fetch unpublished outbox rows
      2. Group by CodeSystem
      3. Build a FHIR Bundle per group
      4. Publish to RabbitMQ
      5. Mark as published

    Returns the number of rows published.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            rows = _fetch_unpublished(cur)
            if not rows:
                return 0

            logger.info("Relay: %d unpublished outbox rows", len(rows))
            groups = _group_by_system(rows)

            # Get RabbitMQ channel
            rmq_conn, channel = _get_rabbitmq_channel()
            try:
                published_ids: list[int] = []

                for system_url, group_rows in groups.items():
                    added, modified, removed = _rows_to_bundle_inputs(group_rows)

                    # We don't store version in the outbox; fetch from sync_state
                    cur.execute(
                        "SELECT version FROM poller.codesystem_sync_state WHERE system_url = %s",
                        (system_url,),
                    )
                    version_row = cur.fetchone()
                    version = version_row["version"] if version_row else None

                    bundle = build_change_bundle(system_url, version, added, modified, removed)
                    if bundle is None:
                        continue

                    routing_key = f"codesystem.{_slugify(system_url)}.changed"
                    body = json.dumps(bundle, ensure_ascii=False)

                    channel.basic_publish(
                        exchange=settings.rabbitmq_exchange,
                        routing_key=routing_key,
                        body=body.encode("utf-8"),
                        properties=pika.BasicProperties(
                            content_type="application/fhir+json",
                            delivery_mode=2,  # persistent
                        ),
                    )
                    logger.info(
                        "Published FHIR Bundle to %s/%s (%d entries)",
                        settings.rabbitmq_exchange,
                        routing_key,
                        len(bundle["entry"]),
                    )

                    published_ids.extend(r["id"] for r in group_rows)

                # Mark all published rows in a single UPDATE
                _mark_published(cur, published_ids)
                conn.commit()

                return len(published_ids)

            finally:
                try:
                    rmq_conn.close()
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Error closing RabbitMQ connection: %s", exc)
    except Exception as exc:
        logger.error("Relay cycle failed: %s", exc, exc_info=True)
        conn.rollback()
        return 0
    finally:
        conn.close()


def run_relay_loop() -> None:
    """Blocking loop that runs relay_once every OUTBOX_POLL_INTERVAL seconds."""
    logger.info(
        "Outbox relay started (interval=%ds)", settings.outbox_poll_interval
    )
    while True:
        try:
            relay_once()
        except Exception as exc:
            logger.error("Relay loop error: %s", exc, exc_info=True)
        time.sleep(settings.outbox_poll_interval)
