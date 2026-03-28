# CodeSystem Change Poller

## Purpose

This service polls an external FHIR R4 API that exposes a `CodeSystem` resource,
detects changes (new, modified, or removed concepts), and emits FHIR R4 message
Bundles as RabbitMQ events. It is designed for environments where the source system
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
│  2. UPSERT/DELETE codesystem_concept_state rows           │
│  3. INSERT change_outbox rows (one per change)            │
└──────┬────────────────────────────────────────────────────┘
       │
       ▼
┌───────────────┐
│  Outbox Relay │  Polls change_outbox for unpublished rows
│  (background) │  Builds a FHIR R4 message Bundle
│               │  Publishes to RabbitMQ exchange
│               │  Marks outbox rows as published
└──────┬────────┘
       │
       ▼
┌───────────────┐
│  RabbitMQ     │  Topic exchange: codesystem.changes
│               │  Routing keys: codesystem.<system_url_slug>.changed
└───────────────┘
```

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
and publishes them to RabbitMQ, then marks them as published.

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
├── migrations/
│   └── 001_init.sql           ← PG schema: sync_state, concept_state, change_outbox
├── src/
│   ├── __init__.py
│   ├── config.py              ← pydantic-settings, env vars, CodeSystemEntry, load_codesystems()
│   ├── db.py                  ← psycopg3 connection pool, transaction helper
│   ├── poller.py              ← HTTP fetch + raw hash check (accepts url param)
│   ├── differ.py              ← concept flattening + diffing logic
│   ├── fhir_bundle.py         ← FHIR R4 message Bundle builder
│   ├── fhir_forwarder.py      ← RabbitMQ consumer → POST to FHIR $process-message
│   ├── outbox_relay.py        ← reads outbox, builds Bundle, publishes to RabbitMQ
│   ├── scheduler.py           ← iterates over CodeSystems, runs poll cycle per entry
│   └── main.py                ← boots scheduler + outbox relay
└── tests/
    └── test_differ.py         ← unit tests for the differ
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
| `LOG_LEVEL` | `INFO` | Python logging level |
| `HTTP_TIMEOUT` | `30` | Seconds for the FHIR API request timeout |

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
on each cron tick.

## Database Schema

Three tables in a `poller` schema:

- **`poller.codesystem_sync_state`** — one row per monitored CodeSystem. Stores the
  last seen resource_hash, the full FHIR JSON (for disaster recovery), and the
  sync timestamp.
- **`poller.codesystem_concept_state`** — one row per concept code per CodeSystem.
  Stores display, definition, concept_hash, properties (jsonb), and parent_code.
  This is the row-level comparison target.
- **`poller.change_outbox`** — append-only. One row per detected change. Carries
  change_type, code, old/new values as JSONB. The `published` flag is flipped by
  the outbox relay after successful RabbitMQ publish.

## Diff Logic

1. Flatten the `concept[]` tree recursively. Each concept becomes a dict with
   `code`, `display`, `definition`, `properties` (merged from `property[]` and
   `designation[]`), and `parent_code`.
2. Index the flattened list by `code` → `dict[str, dict]`.
3. Load current `codesystem_concept_state` rows from PG → also index by code.
4. Three-way comparison:
   - **Added**: code in fresh, not in stored.
   - **Removed**: code in stored, not in fresh.
   - **Modified**: code in both, but `concept_hash` differs. Then field-level diff
     to identify exactly which fields changed (display, definition, properties,
     parent_code).
5. Results feed into the outbox INSERT and the Bundle builder.

## RabbitMQ Events

The outbox relay collects all unpublished outbox rows, groups them by CodeSystem,
builds one FHIR message Bundle per CodeSystem per relay cycle, and publishes it
to the topic exchange with routing key:
`codesystem.<slugified_system_url>.changed`

Consumers bind queues to this exchange with the routing key pattern they care about.

## Running

```bash
cp codesystems.yml.example codesystems.yml   # edit with your CodeSystem URLs
cp .env.example .env                          # edit credentials
docker compose up -d
```

This starts PostgreSQL, RabbitMQ, runs the migration, and launches the poller
service. Logs go to stdout (docker compose logs -f poller).

## Development Commands

```bash
# Run tests
docker compose run --rm poller python -m pytest tests/ -v

# Run a manual poll cycle (bypasses scheduler)
docker compose run --rm poller python -c "from src.scheduler import run_poll_cycle; run_poll_cycle()"

# Check outbox state
docker compose exec db psql -U poller -d codesystem_poller -c "SELECT id, change_type, code, published, created_at FROM poller.change_outbox ORDER BY id DESC LIMIT 20;"

# Reset state (re-triggers full diff on next poll)
docker compose exec db psql -U poller -d codesystem_poller -c "TRUNCATE poller.codesystem_sync_state, poller.codesystem_concept_state, poller.change_outbox;"
```

## Failure Modes

| Failure | Behaviour |
|---|---|
| FHIR API unreachable | Log error, skip cycle, retry on next cron tick. State untouched. |
| FHIR API returns non-200 | Same as above. |
| PG transaction fails | Nothing committed — no partial state, no outbox rows. Next cycle retries. |
| RabbitMQ down | Outbox rows remain `published=false`. Relay retries every `OUTBOX_POLL_INTERVAL` seconds. Events are delivered once RabbitMQ recovers. |
| Service restart | Reads last committed state from PG. Only diffs against that — no full-table re-notification. |
| Phantom diffs (non-deterministic JSON) | Set `CANONICAL_HASH=true` to eliminate. |
