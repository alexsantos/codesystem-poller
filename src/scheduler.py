"""
Scheduler: orchestrates the poll → hash check → diff → persist cycle.
Uses APScheduler with a cron trigger.

Each run is recorded in poller.sync_run (counters + per-CodeSystem details).
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass
from typing import Literal

from src.config import CodeSystemEntry, load_codesystems, settings
from src.db import transaction
from src.differ import (
    diff_concepts,
    flatten_concepts,
    load_stored_concepts,
    persist_state,
    touch_last_seen,
    upsert_sync_state,
)
from src.poller import compute_hash, fetch_codesystem, get_stored_hash

logger = logging.getLogger(__name__)

PollStatus = Literal["unchanged", "changed", "skipped", "failed"]


@dataclass
class PollResult:
    system_url: str
    status: PollStatus
    added: int = 0
    modified: int = 0
    removed: int = 0
    error: str | None = None


def check_completeness(parsed: dict, fresh_concepts: dict[str, dict]) -> str | None:
    """Return a reason to skip this response, or None if it is safe to diff."""
    content = parsed.get("content")
    if content is not None and content != "complete":
        return f"CodeSystem.content is '{content}', not 'complete'"

    count = parsed.get("count")
    if count is not None and count != len(fresh_concepts) and not settings.allow_count_mismatch:
        return (
            f"CodeSystem.count is {count} but {len(fresh_concepts)} concepts were returned "
            "(set ALLOW_COUNT_MISMATCH=true if the source's count is unreliable)"
        )
    return None


def _poll_one(entry: CodeSystemEntry) -> PollResult:
    """Execute one full poll-diff-persist cycle for a single CodeSystem."""
    system_url = entry.canonical_url
    logger.info("Poll cycle starting for %s", system_url)

    # ── Step 1: Fetch ────────────────────────────────────────────────────
    result = fetch_codesystem(entry.url)
    if result is None:
        logger.warning("Poll cycle aborted for %s: fetch failed", system_url)
        return PollResult(system_url, "failed", error="fetch failed")

    raw, parsed = result

    # ── Step 2: Hash check ───────────────────────────────────────────────
    current_hash = compute_hash(raw, parsed)

    with transaction() as cur:
        stored_hash = get_stored_hash(cur, system_url)

        if stored_hash == current_hash:
            logger.info("No change detected for %s (hash match), skipping diff", system_url)
            touch_last_seen(cur, system_url, entry.url)
            return PollResult(system_url, "unchanged")

        logger.info("Hash changed for %s: stored=%s, current=%s", system_url, stored_hash, current_hash)

        # ── Step 3: Flatten + guard against partial content ──────────────
        concepts_list = parsed.get("concept", [])
        fresh_concepts = flatten_concepts(concepts_list)

        skip_reason = check_completeness(parsed, fresh_concepts)
        if skip_reason:
            # Diffing a partial CodeSystem would report most concepts as removed.
            logger.warning("Skipping %s, state left untouched: %s", system_url, skip_reason)
            return PollResult(system_url, "skipped", error=skip_reason)

        # ── Step 4: Diff ─────────────────────────────────────────────────
        stored_concepts = load_stored_concepts(cur, system_url)
        added, modified, removed = diff_concepts(fresh_concepts, stored_concepts)

        if not added and not modified and not removed:
            # Hash changed (e.g., metadata shift) but no concept-level changes.
            # Still update the hash so we don't re-parse next time.
            logger.info("Resource hash changed but no concept diffs for %s; updating hash only", system_url)
            upsert_sync_state(cur, system_url, current_hash, parsed, entry.url)
            return PollResult(system_url, "unchanged")

        # ── Step 5: Persist (state + outbox) in one transaction ──────────
        persist_state(
            cur, system_url, parsed.get("version"), current_hash, parsed,
            fresh_concepts, added, modified, removed, endpoint_url=entry.url,
        )

    logger.info(
        "Poll cycle complete for %s: +%d ~%d -%d concepts",
        system_url, len(added), len(modified), len(removed),
    )
    return PollResult(system_url, "changed", len(added), len(modified), len(removed))


# ── Run bookkeeping (each in its own short transaction) ─────────────────

def _start_run(discovered: int) -> int | None:
    try:
        with transaction() as cur:
            cur.execute(
                "INSERT INTO poller.sync_run (discovered) VALUES (%s) RETURNING id",
                (discovered,),
            )
            return cur.fetchone()["id"]
    except Exception as exc:
        logger.error("Could not record sync_run start: %s", exc)
        return None


def _record_result(run_id: int | None, result: PollResult) -> None:
    if run_id is None:
        return
    column = result.status  # unchanged | changed | skipped | failed (fixed set, see PollStatus)
    try:
        with transaction() as cur:
            cur.execute(
                f"""
                UPDATE poller.sync_run
                SET {column} = {column} + 1,
                    details = details || %s::jsonb
                WHERE id = %s
                """,  # nosec B608 - column comes from the PollStatus literal, not user input
                (json.dumps([asdict(result)]), run_id),
            )
    except Exception as exc:
        logger.error("Could not record sync_run result for %s: %s", result.system_url, exc)


def _finish_run(run_id: int | None) -> None:
    if run_id is None:
        return
    try:
        with transaction() as cur:
            cur.execute("UPDATE poller.sync_run SET finished_at = now() WHERE id = %s", (run_id,))
    except Exception as exc:
        logger.error("Could not record sync_run finish: %s", exc)


def _deactivate_unconfigured(configured: list[str]) -> None:
    """
    Mark CodeSystems that are no longer in codesystems.yml as inactive, so they
    stop counting as stale. Their concepts are left as they are and no events
    are emitted. They are reactivated on their next successful poll.
    """
    try:
        with transaction() as cur:
            cur.execute(
                """
                UPDATE poller.codesystem_sync_state
                SET active = false, removed_at = now()
                WHERE active AND NOT (system_url = ANY(%s))
                RETURNING system_url
                """,
                (configured,),
            )
            for row in cur.fetchall():
                logger.warning("CodeSystem %s is no longer configured; marked inactive", row["system_url"])
    except Exception as exc:
        logger.error("Could not update inactive CodeSystems: %s", exc)


def run_poll_cycle() -> list[PollResult]:
    """Load CodeSystems from config, poll each one, and record the run."""
    started = time.monotonic()
    codesystems = load_codesystems()
    logger.info("Starting poll cycle for %d CodeSystem(s)", len(codesystems))
    run_id = _start_run(len(codesystems))

    results: list[PollResult] = []
    for entry in codesystems:
        try:
            result = _poll_one(entry)
        except Exception as exc:
            logger.error("Poll cycle failed for %s: %s", entry.canonical_url, exc, exc_info=True)
            result = PollResult(entry.canonical_url, "failed", error=f"{type(exc).__name__}: {exc}")
        results.append(result)
        _record_result(run_id, result)

    _deactivate_unconfigured([e.canonical_url for e in codesystems])
    _finish_run(run_id)

    counts = {s: sum(1 for r in results if r.status == s) for s in ("unchanged", "changed", "skipped", "failed")}
    logger.info(
        "Run %s done: discovered=%d unchanged=%d changed=%d skipped=%d failed=%d in %.1fs",
        run_id if run_id is not None else "?", len(codesystems),
        counts["unchanged"], counts["changed"], counts["skipped"], counts["failed"],
        time.monotonic() - started,
    )
    return results
