"""
Outbox Relay: guaranteed (at-least-once) delivery of change_outbox rows.

Each cycle has two steps:

1. Seal. Unpublished rows not yet in a batch are grouped by CodeSystem
   (oldest first, at most RELAY_BATCH_SIZE rows each) into an outbox_batch.
   The FHIR message Bundle is built once and frozen in the batch, with
   Bundle.id = MessageHeader.id = batch id. Sealing is one transaction.

2. Deliver. For each CodeSystem, batches are sent strictly in seal order
   to the configured target (RabbitMQ or a FHIR $process-message endpoint):
     - OK       → batch 'delivered', its rows published = true
     - RETRY    → attempts + 1, next_attempt_at pushed back with exponential
                  backoff; later batches of that CodeSystem wait (ordering)
     - REJECTED → batch 'dead' (parked for inspection, see src.outbox_admin);
                  later batches continue (RELAY_ON_REJECT=dead_letter) or
                  wait until it is requeued/discarded (RELAY_ON_REJECT=block)

Why this guarantees delivery: a row is only marked published after the
target confirmed the Bundle, and all retry state is in PostgreSQL. A crash
between "target accepted" and "batch marked delivered" resends the same
frozen Bundle with the same id, which the receiver can drop as a duplicate.

A PostgreSQL advisory lock ensures only one relay works at a time, even if
several poller instances run against the same database.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections import defaultdict
from typing import Callable

from psycopg.types.json import Jsonb

from src.config import settings
from src.db import get_connection
from src.delivery import DeliveryResult, DeliveryTarget, Outcome, create_target, slugify
from src.fhir_bundle import build_change_bundle

logger = logging.getLogger(__name__)

RELAY_LOCK_KEY = 0x63737270          # arbitrary constant ("csrp")
MAX_BATCHES_PER_SYSTEM_PER_CYCLE = 100

_slugify = slugify  # backwards-compatible alias

_target: DeliveryTarget | None = None


def get_target() -> DeliveryTarget:
    """The process-wide delivery target (connections are reused across cycles)."""
    global _target
    if _target is None:
        _target = create_target()
    return _target


def close_target() -> None:
    global _target
    if _target is not None:
        _target.close()
        _target = None


# ── Row → Bundle conversion ─────────────────────────────────────────────

def _group_by_system(rows: list[dict]) -> dict[str, list[dict]]:
    """Group outbox rows by system_url, keeping their order."""
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


# ── Step 1: seal ─────────────────────────────────────────────────────────

def seal_batches(conn) -> int:
    """Freeze all unsealed, unpublished outbox rows into batches. Returns batches created."""
    size = max(1, settings.relay_batch_size)
    created = 0
    while True:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, system_url, change_type, code, old_value, new_value
                FROM poller.change_outbox
                WHERE published = false AND batch_id IS NULL
                ORDER BY id
                LIMIT %s
                FOR UPDATE
                """,
                (size * 20,),
            )
            rows = cur.fetchall()
            if not rows:
                return created

            for system_url, group in _group_by_system(rows).items():
                cur.execute(
                    "SELECT version FROM poller.codesystem_sync_state WHERE system_url = %s",
                    (system_url,),
                )
                version_row = cur.fetchone()
                version = version_row["version"] if version_row else None

                for start in range(0, len(group), size):
                    chunk = group[start:start + size]
                    batch_id = str(uuid.uuid4())
                    added, modified, removed = _rows_to_bundle_inputs(chunk)
                    bundle = build_change_bundle(
                        system_url, version, added, modified, removed, bundle_id=batch_id,
                    )
                    cur.execute(
                        """
                        INSERT INTO poller.outbox_batch (id, system_url, bundle, row_count)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (batch_id, system_url, Jsonb(bundle), len(chunk)),
                    )
                    cur.execute(
                        "UPDATE poller.change_outbox SET batch_id = %s WHERE id = ANY(%s)",
                        (batch_id, [r["id"] for r in chunk]),
                    )
                    created += 1
                    logger.info(
                        "Sealed batch %s for %s (%d change(s))", batch_id, system_url, len(chunk),
                    )


# ── Step 2: deliver ──────────────────────────────────────────────────────

def _next_head(conn, system_url: str) -> dict | None:
    """Oldest open batch of a CodeSystem (dead ones count only in block mode)."""
    statuses = ["pending", "dead"] if settings.relay_on_reject == "block" else ["pending"]
    return conn.execute(
        """
        SELECT id, status, bundle::text AS body, row_count, attempts,
               next_attempt_at <= now() AS due
        FROM poller.outbox_batch
        WHERE system_url = %s AND status = ANY(%s)
        ORDER BY seq
        LIMIT 1
        """,
        (system_url, statuses),
    ).fetchone()


def _mark_delivered(conn, batch_id, target_name: str) -> None:
    with conn.transaction():
        conn.execute(
            """
            UPDATE poller.outbox_batch
            SET status = 'delivered', delivered_at = now(), delivered_to = %s,
                attempts = attempts + 1, last_attempt_at = now(), last_error = NULL
            WHERE id = %s
            """,
            (target_name, batch_id),
        )
        conn.execute(
            "UPDATE poller.change_outbox SET published = true, published_at = now() WHERE batch_id = %s",
            (batch_id,),
        )


def _schedule_retry(conn, batch_id, error: str) -> None:
    conn.execute(
        """
        UPDATE poller.outbox_batch
        SET attempts = attempts + 1,
            last_attempt_at = now(),
            last_error = %s,
            next_attempt_at = now() + make_interval(
                secs => LEAST(%s * power(2, LEAST(attempts, 30)), %s))
        WHERE id = %s
        """,
        (error, settings.relay_retry_base, settings.relay_retry_max, batch_id),
    )


def _mark_dead(conn, batch_id, error: str) -> None:
    conn.execute(
        """
        UPDATE poller.outbox_batch
        SET status = 'dead', attempts = attempts + 1, last_attempt_at = now(), last_error = %s
        WHERE id = %s
        """,
        (error, batch_id),
    )


def deliver_due(
    conn,
    target: DeliveryTarget,
    should_stop: Callable[[], bool] = lambda: False,
) -> int:
    """Deliver due batches in order per CodeSystem. Returns outbox rows delivered."""
    delivered_rows = 0
    systems = [
        r["system_url"]
        for r in conn.execute(
            "SELECT DISTINCT system_url FROM poller.outbox_batch WHERE status = 'pending' ORDER BY system_url"
        ).fetchall()
    ]

    for system_url in systems:
        for _ in range(MAX_BATCHES_PER_SYSTEM_PER_CYCLE):
            if should_stop():
                return delivered_rows
            head = _next_head(conn, system_url)
            if head is None:
                break
            if head["status"] == "dead":
                logger.warning(
                    "Delivery for %s blocked by dead batch %s (RELAY_ON_REJECT=block); "
                    "requeue or discard it with: python -m src.outbox_admin",
                    system_url, head["id"],
                )
                break
            if not head["due"]:
                break  # in backoff; later batches of this CodeSystem wait

            batch_id = head["id"]
            try:
                result = target.send(str(batch_id), system_url, head["body"].encode("utf-8"))
            except Exception as exc:  # a bug in a target must not skip the backoff
                logger.error("Delivery target raised for batch %s", batch_id, exc_info=True)
                result = DeliveryResult(Outcome.RETRY, f"unexpected error: {type(exc).__name__}: {exc}")

            if result.outcome is Outcome.OK:
                _mark_delivered(conn, batch_id, target.name)
                delivered_rows += head["row_count"]
                logger.info(
                    "Delivered batch %s for %s (%d change(s)) to %s: %s",
                    batch_id, system_url, head["row_count"], target.name, result.detail,
                )
                continue

            if result.outcome is Outcome.RETRY:
                _schedule_retry(conn, batch_id, result.detail)
                logger.warning(
                    "Delivery of batch %s for %s to %s failed (attempt %d), will retry: %s",
                    batch_id, system_url, target.name, head["attempts"] + 1, result.detail,
                )
                break

            # Outcome.REJECTED
            _mark_dead(conn, batch_id, result.detail)
            logger.error(
                "Batch %s for %s was REJECTED by %s and parked as dead (%d change(s) not delivered): %s. "
                "Inspect/requeue with: python -m src.outbox_admin",
                batch_id, system_url, target.name, head["row_count"], result.detail,
            )
            if settings.relay_on_reject == "block":
                break

    return delivered_rows


# ── Cycle ────────────────────────────────────────────────────────────────

def relay_once(
    target: DeliveryTarget | None = None,
    should_stop: Callable[[], bool] = lambda: False,
) -> int:
    """
    Run one relay cycle (seal + deliver). Returns the number of outbox rows
    delivered. Never raises: errors are logged and retried next cycle.
    """
    try:
        conn = get_connection()
    except Exception as exc:
        logger.error("Relay cycle skipped, database unavailable: %s", exc)
        return 0
    try:
        conn.autocommit = True
        locked = conn.execute("SELECT pg_try_advisory_lock(%s) AS ok", (RELAY_LOCK_KEY,)).fetchone()["ok"]
        if not locked:
            logger.debug("Another relay holds the lock; skipping this cycle")
            return 0
        try:
            seal_batches(conn)
            return deliver_due(conn, target or get_target(), should_stop)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (RELAY_LOCK_KEY,))
    except Exception as exc:
        logger.error("Relay cycle failed: %s", exc, exc_info=True)
        return 0
    finally:
        conn.close()


def run_relay_loop() -> None:
    """Blocking loop that runs relay_once every OUTBOX_POLL_INTERVAL seconds."""
    logger.info(
        "Outbox relay started (target=%s, interval=%ds)",
        settings.relay_target, settings.outbox_poll_interval,
    )
    try:
        while True:
            relay_once()
            time.sleep(settings.outbox_poll_interval)
    finally:
        close_target()
