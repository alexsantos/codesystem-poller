"""
End-to-end poll cycle tests against a real PostgreSQL (see conftest.pg).

The HTTP fetch is patched; everything from hashing to the DB is real.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from src import scheduler
from src.config import CodeSystemEntry

SYSTEM = "https://fhir.example/fhir/CodeSystem/Test"
ENTRY = CodeSystemEntry(url=SYSTEM, canonical_url=SYSTEM)


def codesystem(*codes: str, **extra) -> tuple[bytes, dict]:
    body = {
        "resourceType": "CodeSystem",
        "id": "Test",
        "url": SYSTEM,
        "name": "Test",
        "content": "complete",
        "count": len(codes),
        "concept": [{"code": c, "display": f"Display {c}"} for c in codes],
    }
    body.update(extra)
    return json.dumps(body).encode(), body


def poll(response):
    with patch.object(scheduler, "fetch_codesystem", return_value=response):
        return scheduler._poll_one(ENTRY)


def concept_rows(pg) -> dict[str, dict]:
    pg.execute(
        "SELECT code, active, removed_at FROM poller.codesystem_concept_state WHERE system_url = %s",
        (SYSTEM,),
    )
    return {r["code"]: r for r in pg.fetchall()}


def outbox(pg) -> list[tuple[str, str]]:
    pg.execute("SELECT change_type, code FROM poller.change_outbox ORDER BY id")
    return [(r["change_type"], r["code"]) for r in pg.fetchall()]


# ── Phase 3: soft delete ─────────────────────────────────────────────────

class TestSoftDelete:
    def test_removed_concept_kept_inactive(self, pg):
        poll(codesystem("A", "B"))
        poll(codesystem("A"))

        rows = concept_rows(pg)
        assert rows["A"]["active"] is True
        assert rows["B"]["active"] is False
        assert rows["B"]["removed_at"] is not None
        assert outbox(pg)[-1] == ("concept_removed", "B")

    def test_reappearing_concept_revived_and_reported_added(self, pg):
        poll(codesystem("A", "B"))
        poll(codesystem("A"))
        poll(codesystem("A", "B"))

        rows = concept_rows(pg)
        assert rows["B"]["active"] is True
        assert rows["B"]["removed_at"] is None
        assert outbox(pg) == [
            ("concept_added", "A"),
            ("concept_added", "B"),
            ("concept_removed", "B"),
            ("concept_added", "B"),
        ]

    def test_diff_ignores_inactive_rows(self, pg):
        poll(codesystem("A", "B"))
        poll(codesystem("A"))
        # B is inactive; a further change elsewhere must not re-report B as removed
        poll(codesystem("A", "C"))
        assert outbox(pg)[-1] == ("concept_added", "C")
        assert outbox(pg).count(("concept_removed", "B")) == 1


# ── Phase 4: partial content guard + metadata ───────────────────────────

def sync_state(pg) -> dict | None:
    pg.execute("SELECT * FROM poller.codesystem_sync_state WHERE system_url = %s", (SYSTEM,))
    return pg.fetchone()


class TestCompletenessGuard:
    def test_fragment_skipped_no_db_writes(self, pg):
        poll(codesystem("A", content="fragment"))
        assert sync_state(pg) is None
        assert concept_rows(pg) == {}
        assert outbox(pg) == []

    def test_fragment_after_complete_leaves_state_untouched(self, pg):
        poll(codesystem("A", "B"))
        before = sync_state(pg)
        poll(codesystem("A", content="fragment"))
        after = sync_state(pg)
        assert after["resource_hash"] == before["resource_hash"]
        assert all(r["active"] for r in concept_rows(pg).values())
        assert ("concept_removed", "B") not in outbox(pg)

    def test_count_mismatch_skipped(self, pg):
        poll(codesystem("A", "B", count=5000))
        assert sync_state(pg) is None
        assert outbox(pg) == []

    def test_count_mismatch_allowed_by_setting(self, pg, monkeypatch):
        monkeypatch.setattr(scheduler.settings, "allow_count_mismatch", True)
        poll(codesystem("A", "B", count=5000))
        assert len(outbox(pg)) == 2

    def test_missing_content_and_count_is_accepted(self, pg):
        raw, body = codesystem("A")
        del body["content"], body["count"]
        poll((json.dumps(body).encode(), body))
        assert outbox(pg) == [("concept_added", "A")]


class TestMetadata:
    def test_metadata_stored(self, pg):
        poll(codesystem("A", title="Test CS"))
        row = sync_state(pg)
        assert row["name"] == "Test"
        assert row["title"] == "Test CS"
        assert row["content"] == "complete"
        assert row["endpoint_url"] == SYSTEM
        assert row["last_seen_at"] is not None

    def test_last_seen_moves_on_hash_match_but_synced_at_does_not(self, pg):
        poll(codesystem("A"))
        pg.execute(
            "UPDATE poller.codesystem_sync_state SET last_seen_at = now() - interval '2 days', "
            "synced_at = now() - interval '2 days'"
        )
        poll(codesystem("A"))
        row = sync_state(pg)
        assert row["last_seen_at"] > row["synced_at"]
