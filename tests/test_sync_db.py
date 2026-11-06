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


# ── Phase 8: run summary ─────────────────────────────────────────────────

OTHER = "https://fhir.example/fhir/CodeSystem/Other"


class TestRunSummary:
    def _run(self, entries, responses):
        """responses: dict url -> fetch result (or Exception to raise)."""
        def fake_fetch(url):
            r = responses[url]
            if isinstance(r, Exception):
                raise r
            return r
        with patch.object(scheduler, "load_codesystems", return_value=entries), \
             patch.object(scheduler, "fetch_codesystem", side_effect=fake_fetch):
            return scheduler.run_poll_cycle()

    def _last_run(self, pg):
        pg.execute("SELECT * FROM poller.sync_run ORDER BY id DESC LIMIT 1")
        return pg.fetchone()

    def test_counters_and_details(self, pg):
        entries = [ENTRY, CodeSystemEntry(url=OTHER, canonical_url=OTHER),
                   CodeSystemEntry(url=OTHER + "2", canonical_url=OTHER + "2")]
        self._run(entries, {SYSTEM: codesystem("A"), OTHER: None, OTHER + "2": RuntimeError("kaboom")})
        self._run(entries, {SYSTEM: codesystem("A"), OTHER: codesystem("A", content="fragment"),
                            OTHER + "2": None})

        run = self._last_run(pg)
        assert run["finished_at"] is not None
        assert run["discovered"] == 3
        assert (run["unchanged"], run["changed"], run["skipped"], run["failed"]) == (1, 0, 1, 1)
        statuses = {d["system_url"]: d["status"] for d in run["details"]}
        assert statuses == {SYSTEM: "unchanged", OTHER: "skipped", OTHER + "2": "failed"}

        pg.execute("SELECT * FROM poller.sync_run ORDER BY id LIMIT 1")
        first = pg.fetchone()
        assert (first["changed"], first["failed"]) == (1, 2)
        detail = next(d for d in first["details"] if d["system_url"] == SYSTEM)
        assert (detail["added"], detail["modified"], detail["removed"]) == (1, 0, 0)
        assert any("kaboom" in (d["error"] or "") for d in first["details"])

    def test_unconfigured_codesystem_marked_inactive_then_revived(self, pg):
        other = CodeSystemEntry(url=OTHER, canonical_url=OTHER)
        self._run([ENTRY, other], {SYSTEM: codesystem("A"), OTHER: codesystem("B")})
        self._run([ENTRY], {SYSTEM: codesystem("A")})

        pg.execute("SELECT system_url, active, removed_at FROM poller.codesystem_sync_state")
        rows = {r["system_url"]: r for r in pg.fetchall()}
        assert rows[SYSTEM]["active"] is True
        assert rows[OTHER]["active"] is False and rows[OTHER]["removed_at"] is not None
        # concepts of the inactive CodeSystem are untouched and no events emitted
        assert outbox(pg).count(("concept_removed", "B")) == 0

        # back in config with unchanged content (hash match) → reactivated
        self._run([ENTRY, other], {SYSTEM: codesystem("A"), OTHER: codesystem("B")})
        pg.execute("SELECT active, removed_at FROM poller.codesystem_sync_state WHERE system_url = %s", (OTHER,))
        row = pg.fetchone()
        assert row["active"] is True and row["removed_at"] is None

    def test_staleness_query(self, pg):
        self._run([ENTRY], {SYSTEM: codesystem("A")})
        stale_sql = """
            SELECT system_url FROM poller.codesystem_sync_state
            WHERE active AND (last_seen_at IS NULL OR last_seen_at < now() - interval '26 hours')
        """
        pg.execute(stale_sql)
        assert pg.fetchall() == []
        pg.execute("UPDATE poller.codesystem_sync_state SET last_seen_at = now() - interval '3 days'")
        pg.execute(stale_sql)
        assert [r["system_url"] for r in pg.fetchall()] == [SYSTEM]
