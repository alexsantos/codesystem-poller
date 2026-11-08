# Plan: daily sync of all IMTHOM CodeSystems to the local database

Status: implemented on `feature/daily-sync` (2026-11-06), except Phases 6 and 7,
which Phase 0 ruled out (see Findings). Added beyond the original plan: guaranteed
outbox delivery with a direct FHIR `$process-message` target (see "Delivery" below).

## Goal

Keep a local, queryable copy of **every** CodeSystem published by the IMTHOM FHIR
server (`https://thom-fhir.apis.sat.jmslab.pt/fhir`, production `thom-fhir.apis.cuf.pt`),
refreshed once a day, in the existing `poller.*` tables.

The current pipeline (fetch → hash → flatten → diff → single-transaction persist →
outbox) already does the hard part. This plan extends it rather than replacing it.

## Ground rules

- **Current behaviour stays the default.** Every new behaviour is switched on by an
  env var. An existing deployment that only pulls this code and restarts must behave
  exactly as before (except the soft-delete change in Phase 3, see the decision there).
- **Never flood downstream.** No change in this plan may cause a full re-emission of
  `concept_added` events for CodeSystems that are already synced (see Phase 7).
- **Migrations must be idempotent.** The `migrate` service in `docker-compose.yml`
  re-runs every `migrations/*.sql` on each startup (without `ON_ERROR_STOP`), so new
  files use `IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS` / guarded `DO $$` blocks.
- Work on a branch (`feature/daily-sync`), one commit per phase, tests per phase.

## Phase 0 — Probe the server (manual, before writing code)

The design of Phase 6 depends on what the server supports. Run against SAT and
record the answers in this file under "Findings".

```bash
BASE=https://thom-fhir.apis.sat.jmslab.pt/fhir

# 1. Does search on CodeSystem work at all, and how many are there?
curl -s "$BASE/CodeSystem?_summary=count" -H 'Accept: application/fhir+json' | jq '.total'

# 2. Summary listing (should NOT include concept[]); check paging links
curl -s "$BASE/CodeSystem?_summary=true&_count=50" -H 'Accept: application/fhir+json' \
  | jq '{total, link: [.link[] | {relation, url}], entries: [.entry[] | {fullUrl, id: .resource.id, url: .resource.url, version: .resource.version, content: .resource.content, count: .resource.count}]}'

# 3. Is the `url` field the canonical (e.g. https://cuf.pt/fhir/CodeSystem/...)
#    or the server endpoint?
curl -s "$BASE/CodeSystem/Product" -H 'Accept: application/fhir+json' | jq '{id, url, version, content, count, n: (.concept | length)}'

# 4. Is the JSON deterministic? (decides CANONICAL_HASH)
for i in 1 2; do curl -s "$BASE/CodeSystem/Product" | sha256sum; done

# 5. Does any CodeSystem return content != "complete", or count != concepts returned?
```

Decision points:
- Search unsupported → skip Phase 6; the YAML list stays the source of truth.
- `_summary=true` ignored (concepts come back in the listing) → try `_elements=url,name,version,content,count`.
- `url` is the endpoint, not a canonical → Phase 7 is a no-op; skip it.

## Phase 1 — Database-only mode

Today `main.py` always starts the outbox relay thread, and the `poller` service
`depends_on` RabbitMQ. The forwarder is already optional (compose profile `forwarder`).

- `src/config.py`: add `relay_enabled: bool = True`.
- `src/main.py`: start the relay thread only if `settings.relay_enabled`; log the mode.
  Outbox rows are still written (they are the change history), just not published.
- `docker-compose.yml`: put `rabbitmq` behind a profile (e.g. `messaging`) and drop the
  hard `depends_on: rabbitmq` from `poller` (keep `migrate`). Document
  `docker compose --profile messaging up -d` for the current RabbitMQ setup.
  ⚠ This changes the default `docker compose up` for existing deployments — check
  how production is started (compose vs `codesystem-poller.service`) before merging.
- `.env.example`: `RELAY_ENABLED=true` and a daily example `POLL_CRON="0 3 * * *"`.

Tests: `main` does not start the relay when disabled (patch `threading.Thread`).

## Phase 2 — Migration `002_daily_sync.sql`

All idempotent.

```sql
-- CodeSystem-level metadata and lifecycle
ALTER TABLE poller.codesystem_sync_state
    ADD COLUMN IF NOT EXISTS endpoint_url  TEXT,
    ADD COLUMN IF NOT EXISTS name          TEXT,
    ADD COLUMN IF NOT EXISTS title         TEXT,
    ADD COLUMN IF NOT EXISTS content       TEXT,
    ADD COLUMN IF NOT EXISTS active        BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS last_seen_at  TIMESTAMPTZ,   -- last successful fetch, even if unchanged
    ADD COLUMN IF NOT EXISTS removed_at    TIMESTAMPTZ;

-- Concept soft delete
ALTER TABLE poller.codesystem_concept_state
    ADD COLUMN IF NOT EXISTS active      BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS removed_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_concept_state_active
    ON poller.codesystem_concept_state(system_url) WHERE active;

-- One row per scheduled run, for monitoring
CREATE TABLE IF NOT EXISTS poller.sync_run (
    id           BIGSERIAL PRIMARY KEY,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    discovered   INT,
    unchanged    INT NOT NULL DEFAULT 0,
    changed      INT NOT NULL DEFAULT 0,
    skipped      INT NOT NULL DEFAULT 0,
    failed       INT NOT NULL DEFAULT 0,
    details      JSONB NOT NULL DEFAULT '[]'   -- [{system_url, status, added, modified, removed, error}]
);
```

Note `last_seen_at` vs `synced_at`: `synced_at` only moves when content changes, so it
cannot be used for staleness alerts. `last_seen_at` is updated on every successful
fetch, including the hash-match early return in `scheduler._poll_one`.

## Phase 3 — Concept soft delete

`src/differ.py`:
- `load_stored_concepts`: add `AND active` so diffing only sees live concepts.
- `persist_state` step 2a (upsert): also set `active = true, removed_at = NULL`, so a
  concept that reappears is revived (it is emitted as `concept_added`, which is correct).
- `persist_state` step 2b: replace `DELETE` with
  `UPDATE ... SET active = false, removed_at = now(), updated_at = now()`.
- Outbox events are unchanged.

**Decision needed:** make soft delete unconditional, or behind `SOFT_DELETE=true`?
Recommendation: unconditional — nothing outside this service is known to read
`codesystem_concept_state`, and keeping history is strictly safer. Confirm no other
consumer reads that table directly before merging.

Tests: removed → row kept with `active=false`; reappearing code → revived and reported
as added; diff ignores inactive rows.

## Phase 4 — Guard against partial content

In `scheduler._poll_one`, after fetch and before diffing:
- If `parsed["content"]` is present and not `"complete"` → log a warning, record
  `skipped` in the run summary, **do not touch state**. A `fragment` / `not-present`
  CodeSystem diffed as-is would mark most concepts as removed.
- If `parsed["count"]` is present and differs from the number of flattened concepts →
  log a warning and skip (server truncation/paging). Make this check overridable with
  `ALLOW_COUNT_MISMATCH=false` in case IMTHOM's `count` turns out to be unreliable.
- Also store `name`, `title`, `content`, `endpoint_url` on `codesystem_sync_state`.

Tests: `fragment` → skipped and no DB writes; count mismatch → skipped.

## Phase 5 — Fetch retries

Daily means one failed fetch leaves data a day staler. `src/poller.py`
`fetch_codesystem`: retry up to `HTTP_RETRIES` (default 3) with backoff (2s, 8s, 30s)
on `httpx.RequestError` and 5xx/429. No retry on 4xx. Keep returning `None` on final
failure so the existing control flow is unchanged.

Tests: with `httpx.MockTransport` — 503, 503, 200 → success; 404 → no retry.

## Phase 6 — Automatic discovery (skip if Phase 0 says search is unsupported)

Settings:
- `codesystem_discovery: bool = False`
- `fhir_base_url: str | None = None` (e.g. `https://thom-fhir.apis.sat.jmslab.pt/fhir`)

New module `src/discovery.py`:
- `discover_codesystems(base_url) -> list[CodeSystemEntry]`:
  `GET {base}/CodeSystem?_summary=true&_count=100`, follow `link[relation=next]`
  until absent (with a page cap, e.g. 100, to avoid loops). For each entry,
  `url` = `fullUrl` (or `{base}/CodeSystem/{id}`), `canonical_url` = `resource.url`
  (see Phase 7 for when this is actually used as the key).
- Raise on any HTTP failure or on an empty result — the caller must treat
  "discovery failed" very differently from "server has zero CodeSystems".

YAML becomes an optional filter when discovery is on:

```yaml
discovery:
  include: []            # canonical URL globs; empty = everything
  exclude:
    - "*/FhirServerOutMessages"
codesystems: []          # still used verbatim when discovery is off
```

`config.load_codesystems()`: stop raising on an empty `codesystems:` list when
discovery is enabled.

`scheduler.run_poll_cycle()`:
1. If discovery on → `discover_codesystems()`; on failure log an error, record the run
   as failed, and **return without marking anything removed**.
2. Apply include/exclude.
3. Poll each entry (existing `_poll_one`).
4. CodeSystems present in `codesystem_sync_state` (`active`) but absent from a
   *successful* discovery → `active = false, removed_at = now()`. Their concepts are
   left as they are; no `concept_removed` events are emitted (a CodeSystem
   disappearing is an operational event, not N concept changes). Log it at WARNING.
   If one later reappears → set `active = true, removed_at = NULL`.

`main.py` startup log: print "discovery from <base>" instead of the YAML list.

Tests: paging follows `next`; empty result raises; missing CodeSystem marked inactive
only after a successful discovery; include/exclude filtering.

## Phase 7 — Key CodeSystems by the resource's own `url` (opt-in)

Today the DB key is `canonical_url` from YAML, which in `codesystems.yml` is the
endpoint URL. Consumers will look codes up by `Coding.system`, which is the resource's
`url` (e.g. `https://cuf.pt/fhir/CodeSystem/Service`, as seen in `simh-integration`).

- Setting `system_key: Literal["config", "resource"] = "config"`.
- With `resource`, `_poll_one` uses `parsed["url"]` as `system_url` (fall back to the
  config value if missing).
- **Re-key existing rows instead of re-syncing them**, otherwise the first run sees an
  empty state for the new key and emits every concept as `concept_added`:
  in one transaction, if a row exists under the old key and none under the new key:
  1. `INSERT` a copy of the `codesystem_sync_state` row under the new key,
  2. `UPDATE codesystem_concept_state SET system_url = new WHERE system_url = old`,
  3. `UPDATE change_outbox SET system_url = new WHERE system_url = old AND NOT published`,
  4. `DELETE` the old `codesystem_sync_state` row.
  (The FK has no `ON UPDATE CASCADE`, hence insert-new → move children → delete-old.)
  Leave published outbox history under the old key.
- Changing the key changes RabbitMQ routing keys (`codesystem.<slug>.changed`) and the
  `system` in emitted Bundles. Coordinate with any queue bindings before enabling it
  where the relay is on.

Tests: re-key moves rows and emits no events; a second run is a no-op.

## Phase 8 — Run summary and staleness

- `run_poll_cycle` creates a `poller.sync_run` row at start, updates counters and
  `details` per CodeSystem, and sets `finished_at` at the end (in its own short
  transactions, independent of the per-CodeSystem ones).
- `_poll_one` returns a small result (`unchanged | changed | skipped | failed` plus
  +/~/- counts) instead of `None`.
- One INFO log line per run:
  `Run 42 done: discovered=37 unchanged=35 changed=1 skipped=1 failed=0 in 12.4s`.
- Staleness query, documented in README (for Grafana/cron alert):

```sql
SELECT system_url, last_seen_at
FROM poller.codesystem_sync_state
WHERE active AND (last_seen_at IS NULL OR last_seen_at < now() - interval '26 hours');
```

## Phase 9 — Docs

- `CLAUDE.md` and `README.md`: new env vars table rows (`RELAY_ENABLED`,
  `CODESYSTEM_DISCOVERY`, `FHIR_BASE_URL`, `SYSTEM_KEY`, `HTTP_RETRIES`,
  `ALLOW_COUNT_MISMATCH`), the DB-only deployment, soft-delete semantics
  (readers must filter `WHERE active`), `sync_run`, and the staleness query.
- `codesystems.yml.example`: show both modes.
- `.env.example`: daily cron example.

## Suggested `.env` for the target setup

```
CODESYSTEM_DISCOVERY=true
FHIR_BASE_URL=https://thom-fhir.apis.sat.jmslab.pt/fhir
SYSTEM_KEY=resource          # only if Phase 0 confirms url is a real canonical
POLL_CRON=0 3 * * *
RELAY_ENABLED=false          # true if RabbitMQ consumers should still get events
HTTP_RETRIES=3
```

## Delivery (added during implementation)

Requirement: the relay must be able to deliver straight to a FHIR
`$process-message` endpoint (no RabbitMQ) with a delivery guarantee, handling
(1) Bundles the endpoint rejects without blocking everything behind them, and
(2) retries without sleeping inside the relay loop.

Implemented (migration `003_outbox_delivery.sql`, `src/delivery.py`,
`src/outbox_relay.py`, `src/outbox_admin.py`):
- `RELAY_TARGET=rabbitmq|fhir`. The FHIR target reuses the forwarder's `FHIR_*` variables.
- Unpublished rows are sealed into `outbox_batch` rows with the Bundle frozen;
  `Bundle.id` = `MessageHeader.id` = batch id, identical on every retry →
  at-least-once with receiver-side deduplication.
- Rows are marked published only after the target confirms. RabbitMQ now uses
  publisher confirms + `mandatory` (unroutable = retry, not silently dropped).
- Transient failures: retried forever, exponential backoff in Postgres
  (`RELAY_RETRY_BASE`/`RELAY_RETRY_MAX`); later batches of that CodeSystem wait.
- Rejections (400/409/410/413/415/422, `fatal-error` replies) → `dead`;
  `RELAY_ON_REJECT=dead_letter` (continue) or `block` (hold the CodeSystem).
  Requeue/discard with `python -m src.outbox_admin`.
- Advisory lock: one active relay per database.

## Out of scope

- Multiple versions of the same CodeSystem side by side (we keep only the latest).
- ValueSet `$expand` paging for very large CodeSystems — only if Phase 0/4 finds one
  that is not `complete`.
- A query API over the local tables; consumers read Postgres directly for now.

## Open questions

1. How is production started — `docker compose` or `codesystem-poller.service`? (Phase 1)
   The repo's unit runs plain `docker compose up`, so existing deployments must add
   `COMPOSE_PROFILES=messaging` to `.env` to keep RabbitMQ. **Still to confirm.**
2. Does anything besides this service read `codesystem_concept_state`? (Phase 3)
   Soft delete was made unconditional as recommended. **Still to confirm** — such a
   reader must now filter `WHERE active`.
3. Are there RabbitMQ consumers bound to the current routing keys? (Phase 7)
   Moot: Phase 7 skipped, routing keys unchanged.
4. SAT only, or production `thom-fhir.apis.cuf.pt` too (two deployments)? Open.

## Findings (fill in during Phase 0)

Probed SAT on 2026-11-06.

- Search supported: **no.** `GET /CodeSystem` (with or without `_summary`) returns
  400 `NotImplementedException`; the CapabilityStatement lists only `read` (plus
  `$lookup`, `$find-matches`) for CodeSystem. Server: "I'm THOM", FHIR 4.0.1.
  → **Phase 6 skipped**; `codesystems.yml` stays the source of truth.
  (CodeSystems removed from the YAML are marked `active = false`.)
- Number of CodeSystems: unknown (cannot enumerate without search).
- `url` is canonical: **no.** `url` is the endpoint, e.g.
  `https://thom-fhir.apis.sat.jmslab.pt/fhir/CodeSystem/Product`. → **Phase 7 skipped.**
- Deterministic JSON: **yes** (two fetches, identical SHA-256) → `CANONICAL_HASH=false`.
- Non-`complete` CodeSystems: none among those checked. `Product`: complete,
  count 11726 = 11726 flat concepts (no hierarchy); `FunctionalHealthUnitForSchedule`:
  complete, count 360 = 360. The `count` check is reliable here.