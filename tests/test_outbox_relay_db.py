"""
Outbox relay delivery guarantees, against a real PostgreSQL (see conftest.pg).

A scripted FakeTarget stands in for RabbitMQ / the FHIR endpoint, except in
the end-to-end test, which uses the real FhirTarget over httpx.MockTransport.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import httpx
import psycopg
import pytest

from src import outbox_admin, outbox_relay
from src.config import settings
from src.delivery import DeliveryResult, FhirTarget, Outcome

A = "https://fhir.example/fhir/CodeSystem/A"
B = "https://fhir.example/fhir/CodeSystem/B"


class FakeTarget:
    """Returns scripted outcomes (default OK) and records every send."""

    name = "fake"

    def __init__(self, *outcomes: Outcome):
        self.outcomes = list(outcomes)
        self.sent: list[dict] = []

    def send(self, bundle_id, system_url, body):
        self.sent.append({"id": bundle_id, "system": system_url, "body": body, "bundle": json.loads(body)})
        outcome = self.outcomes.pop(0) if self.outcomes else Outcome.OK
        return DeliveryResult(outcome, f"scripted {outcome.value}")

    def close(self):
        pass


def add_changes(pg, system_url: str, *codes: str, change_type: str = "concept_added") -> None:
    pg.execute(
        """
        INSERT INTO poller.codesystem_sync_state (system_url, resource_hash, resource_json)
        VALUES (%s, 'h', '{}') ON CONFLICT DO NOTHING
        """,
        (system_url,),
    )
    for code in codes:
        concept = {"code": code, "display": f"D {code}", "properties": {}, "parent_code": None}
        old, new = (None, concept) if change_type == "concept_added" else (concept, None)
        pg.execute(
            """
            INSERT INTO poller.change_outbox (system_url, change_type, code, old_value, new_value)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (system_url, change_type, code,
             json.dumps(old) if old else None, json.dumps(new) if new else None),
        )


def batches(pg, system_url: str | None = None) -> list[dict]:
    pg.execute(
        """
        SELECT id, system_url, status, attempts, row_count, last_error, delivered_to,
               next_attempt_at > now() AS in_backoff
        FROM poller.outbox_batch
        WHERE %s::text IS NULL OR system_url = %s
        ORDER BY seq
        """,
        (system_url, system_url),
    )
    return pg.fetchall()


def unpublished(pg) -> list[str]:
    pg.execute("SELECT code FROM poller.change_outbox WHERE NOT published ORDER BY id")
    return [r["code"] for r in pg.fetchall()]


def make_due(pg) -> None:
    pg.execute("UPDATE poller.outbox_batch SET next_attempt_at = now() WHERE status = 'pending'")


def codes_in(sent: dict) -> list[str]:
    out = []
    for entry in sent["bundle"]["entry"][1:]:
        for p in entry["resource"]["parameter"]:
            if p["name"] == "code":
                out.append(p["valueCode"])
    return out


# ── Sealing ──────────────────────────────────────────────────────────────

class TestSeal:
    def test_rows_grouped_per_codesystem_and_chunked(self, pg, monkeypatch):
        monkeypatch.setattr(settings, "relay_batch_size", 2)
        add_changes(pg, A, "a1", "a2", "a3")
        add_changes(pg, B, "b1")
        target = FakeTarget()
        assert outbox_relay.relay_once(target) == 4

        assert [(b["system_url"], b["row_count"]) for b in batches(pg)] == [(A, 2), (A, 1), (B, 1)]
        assert [codes_in(s) for s in target.sent] == [["a1", "a2"], ["a3"], ["b1"]]
        assert unpublished(pg) == []

    def test_bundle_id_is_batch_id_and_header_id(self, pg):
        add_changes(pg, A, "a1")
        target = FakeTarget()
        outbox_relay.relay_once(target)
        sent = target.sent[0]
        batch = batches(pg)[0]
        assert sent["id"] == str(batch["id"])
        assert sent["bundle"]["id"] == str(batch["id"])
        assert sent["bundle"]["entry"][0]["resource"]["resourceType"] == "MessageHeader"
        assert sent["bundle"]["entry"][0]["resource"]["id"] == str(batch["id"])

    def test_rows_added_later_go_to_a_new_batch(self, pg):
        add_changes(pg, A, "a1")
        target = FakeTarget(Outcome.RETRY)
        outbox_relay.relay_once(target)
        add_changes(pg, A, "a2")
        make_due(pg)
        outbox_relay.relay_once(target)
        assert [codes_in(s) for s in target.sent] == [["a1"], ["a1"], ["a2"]]
        assert [b["row_count"] for b in batches(pg)] == [1, 1]


# ── Retries and ordering ─────────────────────────────────────────────────

class TestRetry:
    def test_transient_failure_keeps_rows_and_backs_off(self, pg):
        add_changes(pg, A, "a1")
        target = FakeTarget(Outcome.RETRY)
        assert outbox_relay.relay_once(target) == 0

        (b,) = batches(pg)
        assert b["status"] == "pending" and b["attempts"] == 1 and b["in_backoff"]
        assert b["last_error"] == "scripted retry"
        assert unpublished(pg) == ["a1"]

        # Not due yet: nothing is sent
        outbox_relay.relay_once(target)
        assert len(target.sent) == 1

    def test_retry_resends_identical_bundle(self, pg):
        add_changes(pg, A, "a1", "a2")
        target = FakeTarget(Outcome.RETRY, Outcome.RETRY)
        for _ in range(3):
            outbox_relay.relay_once(target)
            make_due(pg)
        assert len(target.sent) == 3
        assert len({s["body"] for s in target.sent}) == 1
        assert len({s["id"] for s in target.sent}) == 1
        assert batches(pg)[0]["status"] == "delivered"
        assert unpublished(pg) == []

    def test_backoff_grows_and_is_capped(self, pg, monkeypatch):
        monkeypatch.setattr(settings, "relay_retry_base", 10)
        monkeypatch.setattr(settings, "relay_retry_max", 60)
        add_changes(pg, A, "a1")
        target = FakeTarget(*[Outcome.RETRY] * 5)
        delays = []
        for _ in range(5):
            outbox_relay.relay_once(target)
            pg.execute(
                "SELECT extract(epoch FROM next_attempt_at - last_attempt_at)::int AS d FROM poller.outbox_batch"
            )
            delays.append(pg.fetchone()["d"])
            make_due(pg)
        assert delays == [10, 20, 40, 60, 60]

    def test_failing_batch_holds_back_later_batches_of_same_codesystem_only(self, pg, monkeypatch):
        monkeypatch.setattr(settings, "relay_batch_size", 1)
        add_changes(pg, A, "a1", "a2")
        add_changes(pg, B, "b1")
        target = FakeTarget(Outcome.RETRY)  # first send (A/a1) fails, rest OK
        outbox_relay.relay_once(target)

        assert [codes_in(s) for s in target.sent] == [["a1"], ["b1"]]
        assert unpublished(pg) == ["a1", "a2"]

        make_due(pg)
        outbox_relay.relay_once(target)
        assert [codes_in(s) for s in target.sent][2:] == [["a1"], ["a2"]]
        assert unpublished(pg) == []

    def test_crash_after_send_before_commit_redelivers_same_bundle(self, pg):
        """At-least-once: the target accepted it, but we died before recording that."""
        add_changes(pg, A, "a1")
        target = FakeTarget()
        with patch.object(outbox_relay, "_mark_delivered", side_effect=RuntimeError("crash")):
            outbox_relay.relay_once(target)
        assert unpublished(pg) == ["a1"]
        assert batches(pg)[0]["status"] == "pending"

        outbox_relay.relay_once(target)
        assert len(target.sent) == 2
        assert target.sent[0]["id"] == target.sent[1]["id"]  # receiver can deduplicate
        assert target.sent[0]["body"] == target.sent[1]["body"]
        assert unpublished(pg) == []

    def test_target_exception_does_not_lose_rows(self, pg):
        add_changes(pg, A, "a1")

        class Exploding(FakeTarget):
            def send(self, *args):
                raise RuntimeError("bug in target")

        assert outbox_relay.relay_once(Exploding()) == 0
        assert unpublished(pg) == ["a1"]
        (b,) = batches(pg)
        assert b["attempts"] == 1 and b["in_backoff"] and "bug in target" in b["last_error"]
        make_due(pg)
        outbox_relay.relay_once(FakeTarget())
        assert unpublished(pg) == []

    def test_should_stop_interrupts_between_deliveries(self, pg, monkeypatch):
        monkeypatch.setattr(settings, "relay_batch_size", 1)
        add_changes(pg, A, "a1", "a2", "a3")
        target = FakeTarget()
        outbox_relay.relay_once(target, should_stop=lambda: len(target.sent) >= 1)
        assert len(target.sent) == 1
        assert unpublished(pg) == ["a2", "a3"]


# ── Rejections (poison Bundles) ──────────────────────────────────────────

class TestRejected:
    def test_dead_letter_parks_batch_and_continues(self, pg, monkeypatch):
        monkeypatch.setattr(settings, "relay_batch_size", 1)
        monkeypatch.setattr(settings, "relay_on_reject", "dead_letter")
        add_changes(pg, A, "a1", "a2")
        target = FakeTarget(Outcome.REJECTED)
        outbox_relay.relay_once(target)

        assert [b["status"] for b in batches(pg)] == ["dead", "delivered"]
        assert unpublished(pg) == ["a1"]  # parked, not lost
        outbox_relay.relay_once(target)
        assert len(target.sent) == 2  # dead batch is not retried automatically

    def test_block_holds_later_batches_until_requeued(self, pg, monkeypatch):
        monkeypatch.setattr(settings, "relay_batch_size", 1)
        monkeypatch.setattr(settings, "relay_on_reject", "block")
        add_changes(pg, A, "a1", "a2")
        add_changes(pg, B, "b1")
        target = FakeTarget(Outcome.REJECTED)
        outbox_relay.relay_once(target)
        outbox_relay.relay_once(target)

        assert [codes_in(s) for s in target.sent] == [["a1"], ["b1"]]
        assert [b["status"] for b in batches(pg, A)] == ["dead", "pending"]

        dead_id = str(batches(pg, A)[0]["id"])
        outbox_admin.main(["requeue", dead_id])
        outbox_relay.relay_once(target)
        assert [codes_in(s) for s in target.sent][2:] == [["a1"], ["a2"]]
        assert unpublished(pg) == []

    def test_discard_unblocks(self, pg, monkeypatch):
        monkeypatch.setattr(settings, "relay_batch_size", 1)
        monkeypatch.setattr(settings, "relay_on_reject", "block")
        add_changes(pg, A, "a1", "a2")
        target = FakeTarget(Outcome.REJECTED)
        outbox_relay.relay_once(target)
        outbox_admin.main(["discard", str(batches(pg, A)[0]["id"])])
        outbox_relay.relay_once(target)
        assert [b["status"] for b in batches(pg, A)] == ["discarded", "delivered"]
        assert unpublished(pg) == ["a1"]

    def test_requeue_all_dead(self, pg, monkeypatch, capsys):
        monkeypatch.setattr(settings, "relay_batch_size", 1)
        add_changes(pg, A, "a1", "a2")
        outbox_relay.relay_once(FakeTarget(Outcome.REJECTED, Outcome.REJECTED))
        outbox_admin.main(["dead"])
        assert capsys.readouterr().out.count(A) == 2
        outbox_admin.main(["requeue", "--all-dead"])
        outbox_relay.relay_once(FakeTarget())
        assert unpublished(pg) == []


# ── Concurrency ──────────────────────────────────────────────────────────

def test_second_relay_skips_while_lock_is_held(pg, pg_url):
    add_changes(pg, A, "a1")
    with psycopg.connect(pg_url, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(%s)", (outbox_relay.RELAY_LOCK_KEY,))
        target = FakeTarget()
        assert outbox_relay.relay_once(target) == 0
        assert target.sent == []
    outbox_relay.relay_once(target)
    assert len(target.sent) == 1


def test_database_down_does_not_raise(monkeypatch):
    monkeypatch.setattr(settings, "database_url", "postgresql://x:y@127.0.0.1:1/none?connect_timeout=1")
    assert outbox_relay.relay_once(FakeTarget()) == 0


# ── End to end: poll → outbox → relay → FHIR $process-message ────────────

def test_end_to_end_direct_fhir(pg):
    from src import scheduler
    from src.config import CodeSystemEntry

    body = {"resourceType": "CodeSystem", "url": A, "content": "complete", "count": 2,
            "concept": [{"code": "X", "display": "x"}, {"code": "Y", "display": "y"}]}
    with patch.object(scheduler, "fetch_codesystem", return_value=(json.dumps(body).encode(), body)):
        scheduler._poll_one(CodeSystemEntry(url=A, canonical_url=A))

    received = []
    responses = iter([httpx.Response(503), httpx.Response(200)])

    def handler(request):
        received.append(json.loads(request.content))
        return next(responses)

    target = FhirTarget("https://downstream.example/fhir/$process-message",
                        transport=httpx.MockTransport(handler))
    outbox_relay.relay_once(target)
    assert unpublished(pg) == ["X", "Y"]
    make_due(pg)
    outbox_relay.relay_once(target)

    assert unpublished(pg) == []
    assert len(received) == 2 and received[0] == received[1]
    bundle = received[0]
    assert bundle["type"] == "message"
    assert bundle["entry"][0]["resource"]["resourceType"] == "MessageHeader"
    assert len(bundle["entry"]) == 3
    (b,) = batches(pg)
    assert b["delivered_to"] == "fhir" and b["attempts"] == 2
