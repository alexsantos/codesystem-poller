"""
Scheduler: orchestrates the poll → hash check → diff → persist cycle.
Uses APScheduler with a cron trigger.
"""

from __future__ import annotations

import json
import logging

from src.config import settings
from src.db import transaction
from src.differ import diff_concepts, flatten_concepts, load_stored_concepts, persist_state
from src.poller import compute_hash, fetch_codesystem, get_stored_hash

logger = logging.getLogger(__name__)


def run_poll_cycle() -> None:
    """
    Execute one full poll-diff-persist cycle.

    1. Fetch CodeSystem from the FHIR API
    2. Hash the response and compare against stored hash
    3. If different: flatten concepts, diff, persist state + outbox rows
    """
    system_url = settings.codesystem_canonical_url
    logger.info("Poll cycle starting for %s", system_url)

    # ── Step 1: Fetch ────────────────────────────────────────────────────
    result = fetch_codesystem()
    if result is None:
        logger.warning("Poll cycle aborted: fetch failed")
        return

    raw, parsed = result

    # ── Step 2: Hash check ───────────────────────────────────────────────
    current_hash = compute_hash(raw, parsed)

    with transaction() as cur:
        stored_hash = get_stored_hash(cur, system_url)

        if stored_hash == current_hash:
            logger.info("No change detected (hash match), skipping diff")
            return

        logger.info("Hash changed: stored=%s, current=%s", stored_hash, current_hash)

        # ── Step 3: Flatten + diff ───────────────────────────────────────
        concepts_list = parsed.get("concept", [])
        fresh_concepts = flatten_concepts(concepts_list)
        stored_concepts = load_stored_concepts(cur, system_url)

        added, modified, removed = diff_concepts(fresh_concepts, stored_concepts)

        if not added and not modified and not removed:
            # Hash changed (e.g., metadata shift) but no concept-level changes.
            # Still update the hash so we don't re-parse next time.
            logger.info("Resource hash changed but no concept diffs; updating hash only")
            cur.execute(
                """
                INSERT INTO poller.codesystem_sync_state (system_url, version, resource_hash, resource_json, synced_at)
                VALUES (%s, %s, %s, %s::jsonb, now())
                ON CONFLICT (system_url) DO UPDATE SET
                    version = EXCLUDED.version,
                    resource_hash = EXCLUDED.resource_hash,
                    resource_json = EXCLUDED.resource_json,
                    synced_at = now()
                """,
                (system_url, parsed.get("version"), current_hash, json.dumps(parsed)),
            )
            return

        # ── Step 4: Persist (state + outbox) in one transaction ──────────
        version = parsed.get("version")
        persist_state(
            cur, system_url, version, current_hash, parsed,
            fresh_concepts, added, modified, removed,
        )

    logger.info(
        "Poll cycle complete: +%d ~%d -%d concepts",
        len(added), len(modified), len(removed),
    )
