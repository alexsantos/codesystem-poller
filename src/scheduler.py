"""
Scheduler: orchestrates the poll → hash check → diff → persist cycle.
Uses APScheduler with a cron trigger.
"""

from __future__ import annotations

import logging

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


def _poll_one(entry: CodeSystemEntry) -> None:
    """Execute one full poll-diff-persist cycle for a single CodeSystem."""
    system_url = entry.canonical_url
    logger.info("Poll cycle starting for %s", system_url)

    # ── Step 1: Fetch ────────────────────────────────────────────────────
    result = fetch_codesystem(entry.url)
    if result is None:
        logger.warning("Poll cycle aborted for %s: fetch failed", system_url)
        return

    raw, parsed = result

    # ── Step 2: Hash check ───────────────────────────────────────────────
    current_hash = compute_hash(raw, parsed)

    with transaction() as cur:
        stored_hash = get_stored_hash(cur, system_url)

        if stored_hash == current_hash:
            logger.info("No change detected for %s (hash match), skipping diff", system_url)
            touch_last_seen(cur, system_url, entry.url)
            return

        logger.info("Hash changed for %s: stored=%s, current=%s", system_url, stored_hash, current_hash)

        # ── Step 3: Flatten + guard against partial content ──────────────
        concepts_list = parsed.get("concept", [])
        fresh_concepts = flatten_concepts(concepts_list)

        skip_reason = check_completeness(parsed, fresh_concepts)
        if skip_reason:
            # Diffing a partial CodeSystem would report most concepts as removed.
            logger.warning("Skipping %s, state left untouched: %s", system_url, skip_reason)
            return

        # ── Step 4: Diff ─────────────────────────────────────────────────
        stored_concepts = load_stored_concepts(cur, system_url)
        added, modified, removed = diff_concepts(fresh_concepts, stored_concepts)

        if not added and not modified and not removed:
            # Hash changed (e.g., metadata shift) but no concept-level changes.
            # Still update the hash so we don't re-parse next time.
            logger.info("Resource hash changed but no concept diffs for %s; updating hash only", system_url)
            upsert_sync_state(cur, system_url, current_hash, parsed, entry.url)
            return

        # ── Step 5: Persist (state + outbox) in one transaction ──────────
        persist_state(
            cur, system_url, parsed.get("version"), current_hash, parsed,
            fresh_concepts, added, modified, removed, endpoint_url=entry.url,
        )

    logger.info(
        "Poll cycle complete for %s: +%d ~%d -%d concepts",
        system_url, len(added), len(modified), len(removed),
    )


def run_poll_cycle() -> None:
    """Load CodeSystems from config and poll each one."""
    codesystems = load_codesystems()
    logger.info("Starting poll cycle for %d CodeSystem(s)", len(codesystems))
    for entry in codesystems:
        try:
            _poll_one(entry)
        except Exception as exc:
            logger.error("Poll cycle failed for %s: %s", entry.canonical_url, exc, exc_info=True)
