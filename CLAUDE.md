# CodeSystem Change Poller

## Purpose

This service polls an external FHIR R4 API that exposes a `CodeSystem` resource,
keeps a local queryable copy of its concepts in PostgreSQL, detects changes (new,
modified, or removed concepts), and delivers FHIR R4 message Bundles — to RabbitMQ
or directly to a FHIR `$process-message` endpoint — at least once. The relay can be
disabled for a database-only deployment. It is designed for environments where the source system
provides **no push notifications, no usable ETag, and no `meta.lastUpdated`**.

## Architecture

```
┌───────────────┐
│  Scheduler    │  APScheduler runs N times/day (configurable via POLL_CRON)
│  (cron)       │
└──────┬────────┘
       │
       ▼
┌───────────────┐
│  Poller       │  HTTP GET to the FHIR CodeSystem endpoint
│  (httpx)      │  No conditional headers (ETag/Last-Modified are useless here)
└──────┬────────┘
       │ raw bytes
       ▼
┌───────────────┐
│  Hash check   │  SHA-256 of raw response body vs stored resource_hash
│               │  If identical → skip, log "no change", done
└──────┬────────┘
       │ hash differs
       ▼
┌───────────────┐
│  Differ       │  Flatten concept[] hierarchy → dict keyed by code
│               │  Compare against codesystem_concept_state in PostgreSQL
│               │  Produce: added[], modified[], removed[]
└──────┬────────┘
       │ change lists
       ▼
┌───────────────────────────────────────────────────────────┐
│  PostgreSQL transaction (single tx, atomic)               │
│  1. UPSERT codesystem_sync_state (new hash, json, etc.)  │
│  2. UPSERT / soft-delete codesystem_concept_state rows    │
│  3. INSERT change_outbox rows (one per change)            │
└──────┬────────────────────────────────────────────────────┘
       │
       ▼
┌───────────────┐
│  Outbox Relay │  RELAY_ENABLED (default true). Seals unpublished rows into
│  (background) │  outbox_batch (frozen Bundle, id = batch id), delivers in order
│               │  per CodeSystem, retries with backoff, marks rows published
│               │  only after the target confirms
└──────┬────────┘
       │ RELAY_TARGET
       ├── rabbitmq ─▶ Topic exchange codesystem.changes,
       │               routing key codesystem.<system_url_slug>.changed
       │               (publisher confirms + mandatory)
       └── fhir ─────▶ POST FHIR_TARGET_URL ($process-message), no RabbitMQ
```

Before the diff, a response with `content != complete` or a `count` that differs
from the flattened concepts is skipped without touching state (Phase 4 guard).
Every run is recorded in `poller.sync_run`.

## Key Design Decisions

### Why poll + diff (not subscription)?
The source API provides no FHIR Subscription, no webhook, no usable ETag (returns
`W/""`), and no `meta.lastUpdated`. Polling is the only option.

### Why hash the raw body first?
The full response is ~300 KB. SHA-256 over those bytes takes microseconds. If the
hash matches the stored one, we skip JSON parsing entirely. This makes most poll
cycles essentially free.

> **Caveat**: If the server returns non-deterministic JSON (shuffled keys, varying
> whitespace), the raw-body hash will produce false positives. In that case, switch
> to canonical-form hashing by setting `CANONICAL_HASH=true`. This parses JSON once,
> sorts keys, serialises with compact separators, then hashes. Slightly more work
> but eliminates phantom diffs.

### Why the transactional outbox pattern?
The state update (new snapshot + concept rows) and the change event records must be
atomic. If we published directly to RabbitMQ and crashed before committing the new
state to PG, the next cycle would re-detect the same changes and emit duplicates.
The outbox table lives in PG, so the INSERT of outbox rows happens in the same
transaction as the state update. A separate relay process picks up unpublished rows
and delivers them (RabbitMQ or FHIR), then marks them as published.

### How is delivery guaranteed?
At-least-once, ordered per CodeSystem. The relay seals rows into `outbox_batch`
with the Bundle frozen in the row; `Bundle.id` = `MessageHeader.id` = batch id,
so a redelivery after a crash is byte-identical and receivers deduplicate by id.
Rows are marked published only after the target confirms (RabbitMQ publisher
confirm + routed; FHIR 2xx without a transient/fatal response code). Transient
failures retry forever with exponential backoff; rejected Bundles
(400/409/410/413/415/422, fatal-error) become `dead` and are handled per
`RELAY_ON_REJECT` (`dead_letter` continues, `block` holds the CodeSystem).
`python -m src.outbox_admin` lists/requeues/discards. An advisory lock keeps a
single active relay.

### Why FHIR message Bundles?
Downstream consumers stay in FHIR-land. The Bundle type is `message`, the first
entry is a `MessageHeader` with an event coding, and each subsequent entry is a
`Parameters` resource carrying one change (added/modified/removed) with the concept
code, old/new values, and the CodeSystem URL. Individual concepts are not standalone
FHIR resources, so `Parameters` is the correct FHIR carrier.

### Why Parameters instead of a full CodeSystem?
Sending the full CodeSystem would force consumers to re-diff. Parameters resources
carry just the delta, are self-describing, and each one maps to a single outbox row.

## Project Layout

```
codesystem-poller/
├── CLAUDE.md                  ← this file
├── README.md
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── codesystems.yml            ← list of CodeSystems to monitor (gitignored)
├── codesystems.yml.example    ← template committed to the repo
├── migrations/                ← re-applied on every start: 002+ must be idempotent
│   ├── 001_init.sql           ← PG schema: sync_state, concept_state, change_outbox
│   ├── 002_daily_sync.sql     ← metadata, last_seen_at, concept soft delete, sync_run
│   └── 003_outbox_delivery.sql ← outbox_batch (guaranteed delivery)
├── src/
│   ├── __init__.py
│   ├── config.py              ← pydantic-settings, env vars, CodeSystemEntry, load_codesystems()
│   ├── db.py                  ← psycopg3 connection pool, transaction helper
│   ├── poller.py              ← HTTP fetch (with retries) + raw hash check
│   ├── differ.py              ← concept flattening + diffing logic
│   ├── fhir_bundle.py         ← FHIR R4 message Bundle builder
│   ├── fhir_forwarder.py      ← RabbitMQ consumer → POST to FHIR $process-message
│   ├── outbox_relay.py        ← seals outbox into batches, delivers with retries
│   ├── delivery.py            ← delivery targets: RabbitMQ (confirms) / FHIR $process-message
│   ├── outbox_admin.py        ← CLI: delivery status, dead batches, requeue/discard
│   ├── scheduler.py           ← iterates CodeSystems, completeness guard, sync_run
│   └── main.py                ← boots scheduler + outbox relay
└── tests/                     ← unit tests; *_db tests need TEST_DATABASE_URL
```

## Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `CODESYSTEMS_CONFIG` | `/app/codesystems.yml` | Path inside the container to the CodeSystems YAML config file |
| `POLL_CRON` | `0 */4 * * *` | Cron expression for poll frequency |
| `CANONICAL_HASH` | `false` | Use canonical JSON hashing instead of raw body hash |
| `DATABASE_URL` | `postgresql://poller:poller@db:5432/codesystem_poller` | PostgreSQL DSN |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` | RabbitMQ connection string |
| `RABBITMQ_EXCHANGE` | `codesystem.changes` | Topic exchange name |
| `OUTBOX_POLL_INTERVAL` | `5` | Seconds between outbox relay cycles |
| `RELAY_ENABLED` | `true` | `false` = database-only (outbox kept as history, not delivered) |
| `RELAY_TARGET` | `rabbitmq` | `rabbitmq` or `fhir` (direct POST to `FHIR_TARGET_URL`) |
| `FHIR_TARGET_URL` | — | `$process-message` URL for `RELAY_TARGET=fhir` and the forwarder |
| `FHIR_AUTH_TOKEN` / `FHIR_AUTH_USER` / `FHIR_AUTH_PASSWORD` | — | Downstream auth |
| `RELAY_BATCH_SIZE` | `500` | Max changes per Bundle |
| `RELAY_RETRY_BASE` / `RELAY_RETRY_MAX` | `5` / `900` | Delivery backoff seconds (doubling, capped; never gives up) |
| `RELAY_ON_REJECT` | `dead_letter` | `dead_letter` or `block` for rejected Bundles |
| `ALLOW_COUNT_MISMATCH` | `false` | Accept responses whose `count` ≠ concepts returned |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `HTTP_TIMEOUT` | `30` | Seconds for the FHIR API request timeout |
| `HTTP_RETRIES` | `3` | Fetch retries on network errors, 5xx, 429 (2s, 8s, 30s) |
| `COMPOSE_PROFILES` | — | Compose only: `messaging` starts RabbitMQ, `forwarder` the forwarder |

## CodeSystems Config File

The list of CodeSystems to monitor is defined in `codesystems.yml` (mounted into
the container at `/app/codesystems.yml`). This file is gitignored — copy
`codesystems.yml.example` to get started.

```yaml
codesystems:
  - url: https://your-fhir-server.example/fhir/CodeSystem/FirstCodeSystem
    canonical_url: https://your-org.example/fhir/CodeSystem/FirstCodeSystem

  - url: https://your-fhir-server.example/fhir/CodeSystem/SecondCodeSystem
    # canonical_url omitted — defaults to url
```

- `url`: the HTTP endpoint to poll
- `canonical_url`: the CodeSystem's canonical identifier, used as the primary key in
  all state tables and as the `system` field in FHIR Bundles. Defaults to `url` if
  omitted.

Each CodeSystem gets its own rows in `codesystem_sync_state` and
`codesystem_concept_state`. The scheduler iterates over all entries sequentially
on each cron tick. CodeSystems removed from the file are marked `active = false`
(no events) and reactivated if they come back.

The IMTHOM server supports only `read` on CodeSystem (no search), and
`CodeSystem.url` is the endpoint URL, so automatic discovery and re-keying by
`CodeSystem.url` (PLAN.md Phases 6/7) are not implemented; the YAML is the
source of truth.

## Database Schema

Three tables in a `poller` schema:

- **`poller.codesystem_sync_state`** — one row per monitored CodeSystem. Stores the
  last seen resource_hash, the full FHIR JSON (for disaster recovery), name/title/
  content/endpoint_url, `synced_at` (last content change), `last_seen_at` (last
  successful fetch, for staleness alerts), and `active`.
- **`poller.codesystem_concept_state`** — one row per concept code per CodeSystem.
  Stores display, definition, concept_hash, properties (jsonb), and parent_code.
  Removed concepts are soft-deleted (`active = false`, `removed_at`); readers must
  filter `WHERE active`. This is the row-level comparison target (active rows only).
- **`poller.change_outbox`** — append-only. One row per detected change. Carries
  change_type, code, old/new values as JSONB, and `batch_id` once sealed. The
  `published` flag is flipped when its batch is confirmed by the target.
- **`poller.outbox_batch`** — one row per Bundle: frozen `bundle`, `status`
  (pending/delivered/dead/discarded), `attempts`, `next_attempt_at`, `last_error`.
- **`poller.sync_run`** — one row per scheduled run with counters and per-CodeSystem
  details.

## Diff Logic

1. Flatten the `concept[]` tree recursively. Each concept becomes a dict with
   `code`, `display`, `definition`, `properties` (merged from `property[]` and
   `designation[]`), and `parent_code`.
2. Index the flattened list by `code` → `dict[str, dict]`.
3. Load current `codesystem_concept_state` rows from PG → also index by code.
4. Three-way comparison:
   - **Added**: code in fresh, not in stored.
   - **Removed**: code in stored (active), not in fresh.
   - **Modified**: code in both, but `concept_hash` differs. Then field-level diff
     to identify exactly which fields changed (display, definition, properties,
     parent_code).
5. Results feed into the outbox INSERT and the Bundle builder.

## RabbitMQ Events

The outbox relay seals unpublished outbox rows, grouped by CodeSystem, into
batches of up to `RELAY_BATCH_SIZE` changes, one FHIR message Bundle each, and
publishes them to the topic exchange with routing key
`codesystem.<slugified_system_url>.changed` and `message_id` = Bundle id.
With `RELAY_TARGET=fhir` the same Bundles are POSTed to `FHIR_TARGET_URL` instead.

Note: the first poll of a CodeSystem emits every concept as `concept_added`.

Consumers bind queues to this exchange with the routing key pattern they care about.

## Running

```bash
cp codesystems.yml.example codesystems.yml   # edit with your CodeSystem URLs
cp .env.example .env                          # edit credentials
docker compose up -d
```

This starts PostgreSQL, RabbitMQ (when `COMPOSE_PROFILES=messaging`), runs the
migrations, and launches the poller service. Logs go to stdout (docker compose logs -f poller).

## Development Commands

```bash
# Run tests (DB tests need TEST_DATABASE_URL pointing at a THROWAWAY database)
docker compose run --rm poller python -m pytest tests/ -v

# Delivery status / dead batches / requeue
docker compose exec poller python -m src.outbox_admin status

# Run a manual poll cycle (bypasses scheduler)
docker compose run --rm poller python -c "from src.scheduler import run_poll_cycle; run_poll_cycle()"

# Check outbox state
docker compose exec db psql -U poller -d codesystem_poller -c "SELECT id, change_type, code, published, created_at FROM poller.change_outbox ORDER BY id DESC LIMIT 20;"

# Reset state (next poll re-emits every concept as concept_added)
docker compose exec db psql -U poller -d codesystem_poller -c "TRUNCATE poller.codesystem_sync_state, poller.codesystem_concept_state, poller.change_outbox, poller.outbox_batch, poller.sync_run;"
```

## Failure Modes

| Failure | Behaviour |
|---|---|
| FHIR API unreachable | Retried `HTTP_RETRIES` times, then logged as failed in `sync_run`; retry next cron tick. State untouched. |
| FHIR API returns non-200 | 5xx/429 retried as above; other 4xx not retried. |
| Partial CodeSystem | `content != complete` or `count` mismatch → skipped, state untouched. |
| PG transaction fails | Nothing committed — no partial state, no outbox rows. Next cycle retries. |
| RabbitMQ / FHIR target down | Batch stays `pending`, retried with exponential backoff forever. Delivered once the target recovers. |
| No queue bound to the exchange | Unroutable → retried, not dropped. |
| Target rejects a Bundle | Batch `dead` (ERROR log); `RELAY_ON_REJECT` decides whether later batches continue. Requeue/discard via `src.outbox_admin`. |
| Crash after target accepted, before commit | Same Bundle (same id) resent; receiver deduplicates. |
| Service restart | Reads last committed state from PG. Only diffs against that — no full-table re-notification. |
| Phantom diffs (non-deterministic JSON) | Set `CANONICAL_HASH=true` to eliminate. |
