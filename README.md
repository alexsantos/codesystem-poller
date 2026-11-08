# CodeSystem Change Poller

A service that polls a FHIR R4 CodeSystem API endpoint, keeps a local queryable copy of its concepts in PostgreSQL, detects changes (additions, modifications, removals), and delivers standardised FHIR R4 message Bundles — to RabbitMQ, or directly to a FHIR `$process-message` endpoint — with an at-least-once delivery guarantee. It can also run database-only.

Built for environments where the source system provides **no push notifications**, **no webhooks**, **no usable ETag**, and **no `meta.lastUpdated`** — polling and diffing is the only option.

---

## Table of Contents

- [Problem](#problem)
- [How It Works](#how-it-works)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [FHIR Message Bundle Format](#fhir-message-bundle-format)
- [Consuming Events](#consuming-events)
- [Operations](#operations)
- [Failure Modes and Recovery](#failure-modes-and-recovery)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

---

## Problem

You depend on a master table exposed as a FHIR R4 `CodeSystem` resource. The system that owns it does not notify you when values change. The API returns a weak ETag (`W/""`) and no `meta.lastUpdated`, so HTTP conditional requests are useless.

You need to:

1. Detect when concepts are added, modified, or removed.
2. Emit change events so downstream systems can react.
3. Survive restarts without re-notifying every single concept in the table.

## How It Works

The service runs a **poll → hash → diff → persist → relay** pipeline on a configurable cron schedule:

1. **Poll** — HTTP GET to the FHIR CodeSystem endpoint (~300 KB, <1s response).
2. **Hash check** — SHA-256 of the raw response body compared against the last stored hash. If identical, the cycle ends immediately (no JSON parsing, no database work).
3. **Flatten** — The `concept[]` hierarchy is recursively flattened into a dictionary keyed by `code`, preserving parent-child relationships, properties, and designations.
4. **Diff** — The flattened snapshot is compared field-by-field against the stored concept state in PostgreSQL. Three change types are identified: `concept_added`, `concept_modified`, `concept_removed`.
5. **Guard** — A response whose `content` is not `complete`, or whose `count` differs from the number of concepts returned, is skipped without touching state (diffing a partial CodeSystem would report most concepts as removed).
6. **Persist** — In a **single PostgreSQL transaction**: the sync state is updated, concept rows are upserted (removed concepts are soft-deleted: kept with `active = false`), and change events are inserted into an outbox table. Either everything is committed or nothing is.
7. **Relay** (optional, `RELAY_ENABLED`) — A background thread seals unpublished outbox rows into batches with a frozen FHIR message Bundle and delivers them to the configured target (`RELAY_TARGET=rabbitmq` or `fhir`), retrying until the target confirms. See [Delivery Guarantees](#delivery-guarantees).

The **transactional outbox pattern** ensures that if the service crashes between committing state and delivering, the relay catches up on the next cycle. No lost events, no duplicate full-table notifications.

Every scheduled run is recorded in `poller.sync_run` (counts and per-CodeSystem status), and `codesystem_sync_state.last_seen_at` records the last successful fetch, for staleness alerts.

## Architecture

```
┌───────────────┐     ┌───────────────┐     ┌─────────────┐
│  Scheduler    │────▶│  Poller       │────▶│  Hash Check  │
│  (APScheduler │     │  (httpx GET)  │     │  (SHA-256)   │
│   cron)       │     │               │     │              │
└───────────────┘     └───────────────┘     └──────┬───────┘
                                                   │
                                         hash differs?
                                                   │ yes
                                                   ▼
                                            ┌─────────────┐
                                            │  Flatten +   │
                                            │  Diff        │
                                            └──────┬───────┘
                                                   │ added/modified/removed
                                                   ▼
                              ┌──────────────────────────────────────┐
                              │  PostgreSQL — single transaction     │
                              │  ┌────────────────────────────────┐  │
                              │  │ UPSERT codesystem_sync_state   │  │
                              │  │ UPSERT/soft-del concept rows   │  │
                              │  │ INSERT change_outbox rows      │  │
                              │  └────────────────────────────────┘  │
                              └───────────────────┬──────────────────┘
                                                  │
                                                  ▼
                              ┌──────────────────────────────────────┐
                              │  Outbox Relay (optional thread)      │
                              │  seal rows → outbox_batch (frozen    │
                              │  Bundle) → deliver, retry, confirm   │
                              └─────────┬──────────────────┬─────────┘
                    RELAY_TARGET=rabbitmq                  RELAY_TARGET=fhir
                                        ▼                  ▼
                 ┌──────────────────────────────┐  ┌──────────────────────────┐
                 │ RabbitMQ topic exchange      │  │ POST FHIR_TARGET_URL     │
                 │ codesystem.<slug>.changed    │  │ ($process-message)       │
                 └──────────────────────────────┘  └──────────────────────────┘
```

## Project Structure

```
codesystem-poller/
├── CLAUDE.md                  # Architecture context for Claude Code
├── README.md                  # This file
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── codesystems.yml.example    # Template — copy to codesystems.yml and edit
├── codesystem-poller.service  # systemd unit file for Linux VM deployment
├── migrations/
│   ├── 001_init.sql           # PostgreSQL schema
│   ├── 002_daily_sync.sql     # CodeSystem metadata, concept soft delete, sync_run
│   └── 003_outbox_delivery.sql # outbox_batch: guaranteed delivery state
├── src/
│   ├── __init__.py
│   ├── config.py              # Pydantic settings + CodeSystemEntry + load_codesystems()
│   ├── db.py                  # psycopg3 connection + transaction helper
│   ├── poller.py              # HTTP fetch + SHA-256 hash comparison
│   ├── differ.py              # Concept flattening, diffing, state persistence
│   ├── fhir_bundle.py         # FHIR R4 message Bundle builder
│   ├── fhir_forwarder.py      # RabbitMQ consumer → FHIR $process-message POST
│   ├── outbox_relay.py        # Outbox → seal batches → deliver (guaranteed)
│   ├── delivery.py            # Delivery targets: RabbitMQ (confirms) and FHIR $process-message
│   ├── outbox_admin.py        # CLI: delivery status, dead batches, requeue/discard
│   ├── scheduler.py           # Poll cycle orchestration (iterates all CodeSystems)
│   └── main.py                # Entry point (scheduler + relay)
└── tests/                     # unit tests + PostgreSQL integration tests (TEST_DATABASE_URL)
```

## Prerequisites

- **Docker** and **Docker Compose** (v2)
- Network access to the FHIR CodeSystem API endpoint from the machine running the service

No local Python installation is required — everything runs inside containers.

## Quick Start

1. **Clone the repository**

```bash
git clone <your-repo-url> codesystem-poller
cd codesystem-poller
```

2. **Create your environment file**

```bash
cp .env.example .env
```

Edit `.env` to set your PostgreSQL and RabbitMQ credentials, and choose a deployment mode (see [Deployment Modes](#deployment-modes)).

3. **Create your CodeSystems config**

```bash
cp codesystems.yml.example codesystems.yml
```

Edit `codesystems.yml` with the FHIR CodeSystem endpoints you want to monitor:

```yaml
codesystems:
  - url: https://your-fhir-server.example/fhir/CodeSystem/FirstCodeSystem
    canonical_url: https://your-org.example/fhir/CodeSystem/FirstCodeSystem

  - url: https://your-fhir-server.example/fhir/CodeSystem/SecondCodeSystem
```

Add as many entries as needed. `canonical_url` is optional and defaults to `url` if omitted.

4. **Start everything**

```bash
docker compose up -d
```

This will:

- Start PostgreSQL and wait for it to be healthy
- Run the SQL migrations (`migrations/*.sql`)
- Start RabbitMQ and wait for it to be healthy (only with the `messaging` profile, which `.env.example` enables via `COMPOSE_PROFILES`)
- Build and start the poller service

5. **Check the logs**

```bash
docker compose logs -f poller
```

On the first poll of a CodeSystem there is no stored state, so **every concept is emitted as `concept_added`** (in Bundles of up to `RELAY_BATCH_SIZE` changes). Subsequent poll cycles only emit events for actual changes. If downstream should not receive that initial load, start with `RELAY_ENABLED=false`, let the first poll run, mark the backlog as published (`UPDATE poller.change_outbox SET published = true WHERE NOT published;`), then enable the relay.

## Configuration

### CodeSystems (`codesystems.yml`)

The list of FHIR CodeSystems to monitor is defined in `codesystems.yml`, mounted
into the container at `/app/codesystems.yml`. This file is **gitignored** — use
`codesystems.yml.example` as a template.

```yaml
codesystems:
  - url: https://your-fhir-server.example/fhir/CodeSystem/A
    canonical_url: https://your-org.example/fhir/CodeSystem/A   # optional

  - url: https://your-fhir-server.example/fhir/CodeSystem/B
```

| Field | Required | Description |
|---|---|---|
| `url` | Yes | HTTP endpoint to poll |
| `canonical_url` | No | Primary key used in state tables and FHIR Bundles. Defaults to `url` if omitted |

To add a new CodeSystem, append an entry and restart the container.

### Environment Variables (`.env`)

| Variable | Required | Default | Description |
|---|---|---|---|
| `CODESYSTEMS_CONFIG` | No | `/app/codesystems.yml` | Path inside the container to the CodeSystems YAML file |
| `POSTGRES_USER` | Yes | — | PostgreSQL username |
| `POSTGRES_PASSWORD` | Yes | — | PostgreSQL password |
| `POSTGRES_DB` | Yes | — | PostgreSQL database name |
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `COMPOSE_PROFILES` | No | — | `messaging` starts RabbitMQ; add `forwarder` for the forwarder. Leave empty for database-only or `RELAY_TARGET=fhir` |
| `RABBITMQ_USER` | With RabbitMQ | — | RabbitMQ username |
| `RABBITMQ_PASSWORD` | With RabbitMQ | — | RabbitMQ password |
| `RABBITMQ_URL` | With RabbitMQ | — | RabbitMQ AMQP connection string |
| `POLL_CRON` | No | `0 */4 * * *` | Cron expression controlling poll frequency |
| `CANONICAL_HASH` | No | `false` | Set to `true` if the FHIR server returns non-deterministic JSON |
| `RABBITMQ_EXCHANGE` | No | `codesystem.changes` | Name of the RabbitMQ topic exchange |
| `RELAY_ENABLED` | No | `true` | `false` = database-only: outbox rows are kept as change history but not delivered |
| `RELAY_TARGET` | No | `rabbitmq` | `rabbitmq` (publish to the exchange) or `fhir` (POST to `FHIR_TARGET_URL`, no RabbitMQ) |
| `FHIR_TARGET_URL` | With `RELAY_TARGET=fhir` | — | Downstream `$process-message` URL. Write `$` as `$$` in `.env` |
| `FHIR_AUTH_TOKEN` / `FHIR_AUTH_USER` / `FHIR_AUTH_PASSWORD` | No | — | Bearer or Basic auth for the downstream endpoint |
| `OUTBOX_POLL_INTERVAL` | No | `5` | Seconds between outbox relay cycles |
| `RELAY_BATCH_SIZE` | No | `500` | Max changes per Bundle |
| `RELAY_RETRY_BASE` / `RELAY_RETRY_MAX` | No | `5` / `900` | Delivery retry backoff in seconds (doubles per attempt, capped). Retries never stop |
| `RELAY_ON_REJECT` | No | `dead_letter` | When a Bundle is rejected: `dead_letter` (park it, continue) or `block` (park it, hold back that CodeSystem) |
| `ALLOW_COUNT_MISMATCH` | No | `false` | Accept responses whose `count` differs from the concepts returned |
| `LOG_LEVEL` | No | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `HTTP_TIMEOUT` | No | `30` | Timeout in seconds for FHIR API HTTP requests |
| `HTTP_RETRIES` | No | `3` | Retries for a CodeSystem fetch on network errors, 5xx and 429 (backoff 2s, 8s, 30s) |

### Adjusting Poll Frequency

The `POLL_CRON` variable uses standard 5-field cron syntax:

```
┌───── minute (0-59)
│ ┌───── hour (0-23)
│ │ ┌───── day of month (1-31)
│ │ │ ┌───── month (1-12)
│ │ │ │ ┌───── day of week (0-6, Sun=0)
│ │ │ │ │
* * * * *
```

Examples:

```dotenv
POLL_CRON=0 */4 * * *      # Every 4 hours (default)
POLL_CRON=0 */1 * * *      # Every hour
POLL_CRON=*/30 * * * *     # Every 30 minutes
POLL_CRON=0 6,12,18 * * *  # Three times a day at 06:00, 12:00, 18:00
POLL_CRON=0 8 * * 1-5      # Once a day at 08:00, weekdays only
POLL_CRON=0 3 * * *        # Daily sync at 03:00
```

## Deployment

### Deployment Modes

| Mode | `.env` | Compose profiles |
|---|---|---|
| RabbitMQ events (default, previous behaviour) | `RELAY_TARGET=rabbitmq` | `COMPOSE_PROFILES=messaging` |
| RabbitMQ + forwarder to FHIR | `RELAY_TARGET=rabbitmq`, `FHIR_TARGET_URL=…` | `COMPOSE_PROFILES=messaging,forwarder` |
| Direct to FHIR `$process-message` (no RabbitMQ) | `RELAY_TARGET=fhir`, `FHIR_TARGET_URL=…` | `COMPOSE_PROFILES=` |
| Database-only (local copy, no events) | `RELAY_ENABLED=false` | `COMPOSE_PROFILES=` |

> **Upgrading an existing deployment:** RabbitMQ is now behind the `messaging` profile. Add `COMPOSE_PROFILES=messaging` to `.env` to keep it running with plain `docker compose up` (which is what `codesystem-poller.service` runs).

### Delivery Guarantees

When the relay is enabled, every change is delivered **at least once**, in order per CodeSystem:

- **Frozen batches.** The relay seals unpublished `change_outbox` rows into `poller.outbox_batch` rows (one CodeSystem, up to `RELAY_BATCH_SIZE` changes each) and stores the Bundle in the batch. Every attempt resends that exact Bundle, and `Bundle.id` = `MessageHeader.id` = batch id. If the service crashes after the target accepted a Bundle but before recording it, the same Bundle is sent again and the receiver can drop it as a duplicate by id.
- **Confirmed before published.** Rows become `published = true` only after the target confirms. RabbitMQ: publisher confirm, and the message was routed to at least one queue (`mandatory`); with no queue bound, delivery is retried rather than the message being dropped. FHIR: a 2xx response, and no `transient-error`/`fatal-error` in a synchronous response message.
- **Retries never stop for transient failures** (network errors, timeouts, 5xx, 408, 429, and 401/403/404/405, which point at configuration rather than the Bundle). The backoff is exponential (`RELAY_RETRY_BASE` doubling up to `RELAY_RETRY_MAX`), and retry state is in PostgreSQL so it survives restarts. While a batch is in backoff, later batches of the same CodeSystem wait; other CodeSystems are unaffected.
- **Rejected Bundles don't block delivery.** HTTP 400/409/410/413/415/422 (or a `fatal-error` response) marks the batch `dead`: it is kept, logged at ERROR, and not retried automatically. With `RELAY_ON_REJECT=dead_letter` (default) later batches continue; with `block` the CodeSystem waits until the dead batch is requeued or discarded.
- **One relay at a time.** The relay runs under a PostgreSQL advisory lock, so running two instances against one database is safe.

Manage delivery with the admin CLI:

```bash
docker compose exec poller python -m src.outbox_admin status            # counts, failing batches
docker compose exec poller python -m src.outbox_admin dead              # rejected batches + error
docker compose exec poller python -m src.outbox_admin show <batch_id>   # the frozen Bundle
docker compose exec poller python -m src.outbox_admin requeue <batch_id> # or --all-dead
docker compose exec poller python -m src.outbox_admin discard <batch_id> # give up (unblocks `block`)
```

### Docker Compose on a Linux VM (recommended)

The provided `docker-compose.yml` is production-ready for Linux VM deployments. It includes health checks, restart policies, and port binding restricted to `127.0.0.1` so PostgreSQL and RabbitMQ are not reachable from outside the VM.

```bash
docker compose up -d
```

---

### Auto-start with systemd

A `codesystem-poller.service` unit file is included in the repository. It integrates Docker Compose with systemd so the stack starts automatically on VM boot and is restarted if it crashes.

**1. Deploy the repository to the VM**

```bash
# On the VM — clone or copy the project
git clone <your-repo-url> /opt/codesystem-poller
cd /opt/codesystem-poller

# Set up config files
cp .env.example .env          # edit with real credentials
cp codesystems.yml.example codesystems.yml   # edit with your CodeSystem URLs
```

**2. Build the image**

```bash
docker compose build
```

**3. Install the systemd unit**

```bash
# Copy the unit file to the systemd directory
sudo cp codesystem-poller.service /etc/systemd/system/

# Edit WorkingDirectory if you deployed to a different path than /opt/codesystem-poller
sudo systemctl daemon-reload
```

**4. Enable and start**

```bash
sudo systemctl enable codesystem-poller   # start on boot
sudo systemctl start codesystem-poller    # start now
```

**5. Check status and logs**

```bash
sudo systemctl status codesystem-poller

# Follow live logs (all containers)
sudo journalctl -u codesystem-poller -f

# Or via Docker Compose directly
docker compose -f /opt/codesystem-poller/docker-compose.yml logs -f
```

**Common systemd commands**

```bash
sudo systemctl stop codesystem-poller      # graceful stop
sudo systemctl restart codesystem-poller   # restart all containers
sudo systemctl disable codesystem-poller   # remove from boot
```

**Note:** The unit file is configured for Docker. If you use Podman, replace `/usr/bin/docker` with `/usr/bin/podman` in the `ExecStart`/`ExecStop`/`ExecStartPre` lines, and change `Requires=docker.service` to `Requires=podman.service` (or remove it if using rootless Podman, which needs no daemon).

---

### Using External PostgreSQL and RabbitMQ

If you already have PostgreSQL and RabbitMQ infrastructure (as is the case if you are integrating this into an existing stack), you can run only the poller service:

1. **Run the migration** against your existing PostgreSQL instance:

```bash
for f in migrations/*.sql; do psql -h <pg-host> -U <pg-user> -d <pg-database> -f "$f"; done
```

2. **Build and run only the poller**:

```bash
docker build -t codesystem-poller .

docker run -d \
  --name codesystem-poller \
  --restart unless-stopped \
  -v ./codesystems.yml:/app/codesystems.yml:ro \
  -e DATABASE_URL=postgresql://user:pass@your-pg-host:5432/your_db \
  -e RABBITMQ_URL=amqp://user:pass@your-rabbitmq-host:5672/ \
  codesystem-poller
```

### Kubernetes / Cloud Run

The container is stateless (all state lives in PostgreSQL). It runs a single process with a scheduler thread and an outbox relay thread. Key points:

- **Replicas**: Run exactly **1 replica**. The relay is protected by an advisory lock, but multiple replicas would still run duplicate poll cycles. If you need HA, use an active-passive setup with leader election.
- **Health check**: The container logs to stdout. Use a liveness probe that checks for the process being alive. The service will log errors and retry on its own if PG or RabbitMQ are temporarily unreachable.
- **Resources**: This is very lightweight — 64 MB RAM and 0.1 CPU is more than enough for a 300 KB payload polled a few times per day.

## FHIR Message Bundle Format

Each change notification is a FHIR R4 Bundle of type `message`:

```
Bundle (type: message)
├── MessageHeader
│   ├── eventCoding:  system + code identifying the event type
│   ├── source:       name + endpoint of this polling service
│   ├── focus:        references to each Parameters resource below
│   └── definition:   canonical URL of the monitored CodeSystem
├── Parameters (one per change)
│   ├── changeType:   concept_added | concept_modified | concept_removed
│   ├── system:       CodeSystem canonical URL
│   ├── version:      CodeSystem version (if available)
│   ├── code:         the affected concept code
│   ├── display:      concept display text (for added/removed)
│   ├── definition:   concept definition (for added)
│   ├── properties:   JSON string of properties/designations (for added)
│   └── change[]:     for modified — each has field, oldValue, newValue
```

### Example: Modified Concept

```json
{
  "resourceType": "Parameters",
  "id": "4f1c…",
  "parameter": [
    { "name": "changeType", "valueString": "concept_modified" },
    { "name": "system", "valueUri": "https://your-org/fhir/CodeSystem/lab" },
    { "name": "code", "valueCode": "HBA1C" },
    {
      "name": "change",
      "part": [
        { "name": "field", "valueString": "display" },
        { "name": "oldValue", "valueString": "Hemoglobin A1c" },
        { "name": "newValue", "valueString": "Hemoglobin A1c (NGSP)" }
      ]
    }
  ]
}
```

`Bundle.id` and the `MessageHeader` id are the outbox batch id and stay the same on redelivery, so consumers should use them to deduplicate. Resource ids are bare UUIDs; `fullUrl` and `MessageHeader.focus` references use `urn:uuid:<id>`.

## Consuming Events

### Binding a Queue to the Exchange

The poller publishes to a **topic exchange** (default: `codesystem.changes`) with routing keys in the format:

```
codesystem.<slugified-canonical-url>.changed
```

To consume, declare a queue and bind it:

```python
import pika, json

connection = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@localhost:5672/"))
channel = connection.channel()

# Declare your consumer queue
channel.queue_declare(queue="my-consumer-queue", durable=True)

# Bind to the exchange — use '#' to receive all CodeSystem changes,
# or a specific slug to filter
channel.queue_bind(
    queue="my-consumer-queue",
    exchange="codesystem.changes",
    routing_key="codesystem.#",
)

def on_message(ch, method, properties, body):
    bundle = json.loads(body)
    for entry in bundle["entry"]:
        resource = entry["resource"]
        if resource["resourceType"] == "Parameters":
            params = {p["name"]: p for p in resource["parameter"]}
            change_type = params["changeType"]["valueString"]
            code = params["code"]["valueCode"]
            print(f"{change_type}: {code}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue="my-consumer-queue", on_message_callback=on_message)
channel.start_consuming()
```

### Forwarding to a FHIR `$process-message` Endpoint

If the FHIR endpoint is the only consumer, you don't need RabbitMQ or the forwarder: set `RELAY_TARGET=fhir` and `FHIR_TARGET_URL`, and the relay POSTs Bundles directly, with the guarantees in [Delivery Guarantees](#delivery-guarantees). Use the forwarder when other systems also consume from the exchange.

If a downstream system exposes a FHIR `$process-message` endpoint (e.g., an integration engine, another EHR, or a FHIR façade), the project includes a **FHIR Forwarder** service that acts as a RabbitMQ consumer and POSTs each Bundle to that endpoint.

The flow is:

```
Poller → outbox → RabbitMQ → FHIR Forwarder → POST to downstream $process-message
```

The forwarder is a separate consumer with its own queue bound to the same exchange. This means other consumers (your own internal services, analytics pipelines, etc.) can also bind to the exchange independently — the forwarder doesn't interfere with them.

**Starting the forwarder:**

The forwarder is defined as a Docker Compose profile so it doesn't start by default:

```bash
# Add these to your .env (or export them)
FHIR_TARGET_URL=https://downstream.example/fhir/$process-message
FHIR_AUTH_TOKEN=your-bearer-token  # optional, omit if not needed

# Start everything including the forwarder
docker compose --profile forwarder up -d
```

**Running the forwarder standalone** (if you use external RabbitMQ):

```bash
docker run -d \
  --name fhir-forwarder \
  --restart unless-stopped \
  -e RABBITMQ_URL=amqp://user:pass@your-rabbitmq:5672/ \
  -e RABBITMQ_EXCHANGE=codesystem.changes \
  -e RABBITMQ_QUEUE=fhir-forwarder \
  -e RABBITMQ_ROUTING_KEY="codesystem.#" \
  -e FHIR_TARGET_URL=https://downstream.example/fhir/$process-message \
  -e FHIR_AUTH_TOKEN=your-bearer-token \
  codesystem-poller python -m src.fhir_forwarder
```

**How it handles failures:**

The forwarder classifies responses the same way as `RELAY_TARGET=fhir`:

- **2xx** → ACK.
- **Bundle rejected** (400, 409, 410, 413, 415, 422, or a `fatal-error` response code) → the message is copied to the durable dead-letter queue **`fhir-forwarder.dead`** (`RABBITMQ_DEAD_QUEUE`) and then ACKed. The ACK happens only after the broker confirms the copy; if the copy fails, the message is requeued, so it is never lost. Headers on the dead letter: `x-reject-reason`, `x-original-exchange`, `x-original-routing-key`, `x-rejected-at`; `message_id` is the Bundle id. Messages that are not JSON or not a FHIR message Bundle are dead-lettered the same way.
- **Anything else** (5xx, 401, 403, 404, 429, network errors) → retried `MAX_RETRIES` times with exponential backoff, then NACKed with requeue. These are endpoint or configuration problems that would hit every Bundle, so they are retried and never dead-lettered. With prefetch 1, later Bundles wait, which keeps them in order.

To replay dead letters after fixing the cause, move them back to `fhir-forwarder` (RabbitMQ management UI → Queues → `fhir-forwarder.dead` → *Move messages*, or a shovel). To drop them, purge the queue.

| Forwarder variable | Default | Description |
|---|---|---|
| `FHIR_TARGET_URL` | (required) | Full URL to the downstream `$process-message` endpoint |
| `FHIR_AUTH_TOKEN` | (empty) | Bearer token for authentication (omit if not needed) |
| `RABBITMQ_QUEUE` | `fhir-forwarder` | Durable queue name for this consumer |
| `RABBITMQ_ROUTING_KEY` | `codesystem.#` | Routing key pattern (`#` = all CodeSystems) |
| `RABBITMQ_DEAD_QUEUE` | `<RABBITMQ_QUEUE>.dead` | Durable queue for rejected Bundles |
| `MAX_RETRIES` | `3` | Retry attempts per message on transient failure |
| `RETRY_DELAY` | `5` | Initial retry delay in seconds (doubles each retry) |

**Testing the flow manually:**

```bash
# 1. Start the stack with the forwarder
docker compose --profile forwarder up -d

# 2. Watch the forwarder logs
docker compose logs -f fhir-forwarder

# 3. Force a poll cycle (if the CodeSystem has changed since baseline)
docker compose run --rm poller python -c \
  "from src.scheduler import run_poll_cycle; run_poll_cycle()"

# 4. You should see the forwarder receive the Bundle and POST it
```

**Writing your own consumer instead:**

If your downstream system doesn't speak FHIR, you don't need the forwarder at all. Write any RabbitMQ consumer that binds to the exchange, parses the Bundle JSON, and does whatever you need — update a database, send a Slack notification, trigger a pipeline. The example in the section above shows the basic pattern.

## Operations

### Monitoring

The service logs structured output to stdout. Key log lines to watch for:

| Log message | Meaning |
|---|---|
| `No change detected (hash match), skipping diff` | Normal — API content hasn't changed |
| `Diff result: X added, Y modified, Z removed` | Changes detected and persisted |
| `Run N done: discovered=… unchanged=… changed=… skipped=… failed=…` | One summary line per scheduled run |
| `Skipping …, state left untouched` | Partial CodeSystem (`content`/`count` guard) — investigate |
| `Delivered batch … to rabbitmq/fhir` | Events confirmed by the target |
| `Delivery of batch … failed (attempt N), will retry` | Target unreachable/erroring — changes are kept and retried |
| `… was REJECTED … parked as dead` | Target refused a Bundle — inspect with `src.outbox_admin dead` |
| `FHIR API request failed` | API is unreachable — retried `HTTP_RETRIES` times, then next cycle |

**Staleness alert** (Grafana / cron), e.g. for a daily sync:

```sql
SELECT system_url, last_seen_at
FROM poller.codesystem_sync_state
WHERE active AND (last_seen_at IS NULL OR last_seen_at < now() - interval '26 hours');
```

**Delivery alert** — changes not delivered within an hour, or rejected:

```sql
SELECT id, system_url, status, attempts, last_error, created_at
FROM poller.outbox_batch
WHERE status = 'dead' OR (status = 'pending' AND created_at < now() - interval '1 hour');
```

**Last runs:**

```sql
SELECT id, started_at, finished_at, discovered, unchanged, changed, skipped, failed
FROM poller.sync_run ORDER BY id DESC LIMIT 10;
```

**Reading the local copy:** removed concepts are kept with `active = false` (soft delete), so readers must filter `WHERE active`. CodeSystems removed from `codesystems.yml` are marked `active = false` in `codesystem_sync_state`.

### Useful Commands

```bash
# View service logs
docker compose logs -f poller

# Check outbox state
docker compose exec db psql -U poller -d codesystem_poller -c \
  "SELECT id, change_type, code, published, created_at
   FROM poller.change_outbox ORDER BY id DESC LIMIT 20;"

# Check last sync timestamp
docker compose exec db psql -U poller -d codesystem_poller -c \
  "SELECT system_url, version, synced_at,
          LEFT(resource_hash, 16) AS hash_prefix
   FROM poller.codesystem_sync_state;"

# Count live concepts currently tracked
docker compose exec db psql -U poller -d codesystem_poller -c \
  "SELECT system_url, COUNT(*) AS concepts
   FROM poller.codesystem_concept_state WHERE active GROUP BY system_url;"

# Force a manual poll cycle for all configured CodeSystems
docker compose run --rm poller python -c \
  "from src.scheduler import run_poll_cycle; run_poll_cycle()"

# Reset all state (next poll re-emits every concept as concept_added!)
docker compose exec db psql -U poller -d codesystem_poller -c \
  "TRUNCATE poller.codesystem_sync_state,
            poller.codesystem_concept_state,
            poller.change_outbox,
            poller.outbox_batch,
            poller.sync_run;"

# Access RabbitMQ management UI
# Open http://localhost:15672 (guest / guest)
```

## Failure Modes and Recovery

| Scenario | What happens | Recovery |
|---|---|---|
| **FHIR API unreachable** | The fetch is retried `HTTP_RETRIES` times (2s, 8s, 30s), then the CodeSystem is recorded as `failed` in `sync_run`. State is untouched. | Automatic retry on next cron tick. |
| **Partial CodeSystem** (`content` ≠ `complete`, or `count` mismatch) | Skipped, recorded as `skipped`. State is untouched. | Investigate the source; `ALLOW_COUNT_MISMATCH=true` if `count` is unreliable. |
| **FHIR API returns non-200** | Same as above. | Same. |
| **PostgreSQL down** | Transaction fails, nothing is committed. No partial state, no orphaned outbox rows. | Service retries on next cycle once PG is back. |
| **RabbitMQ / FHIR target down** | Batches stay `pending`; the relay retries with exponential backoff (capped at `RELAY_RETRY_MAX`). | Delivered automatically once the target recovers. No manual intervention needed. |
| **No queue bound to the exchange** | Publishing is unroutable, so it is retried rather than dropped. | Bind a queue (or start the forwarder). |
| **Target rejects a Bundle** | Batch parked as `dead`, logged at ERROR. | `python -m src.outbox_admin dead`, fix the cause, then `requeue` (or `discard`). |
| **Endpoint rejects a Bundle (forwarder)** | Copied to `fhir-forwarder.dead` with the reason in headers, then ACKed. | Fix the cause, then move the messages back to `fhir-forwarder` (management UI or shovel). |
| **Crash after the target accepted, before recording** | The same Bundle (same id) is sent again. | Receivers deduplicate on `Bundle.id`. |
| **Service crash mid-transaction** | PostgreSQL rolls back the uncommitted transaction. | Next cycle diffs against the last committed state. No re-notification. |
| **Service crash after commit, before delivery** | Outbox rows exist but are unpublished. | Relay picks them up on restart. |
| **Service restart (clean or crash)** | Reads last committed snapshot from PG. Only emits events for changes since that snapshot. | Automatic. No full-table re-notification. |
| **Phantom diffs** (non-deterministic JSON from server) | Raw body hash changes every cycle even though content is the same, causing unnecessary diffs. | Set `CANONICAL_HASH=true` in `.env`. |

## Development

### Running Tests

```bash
# Inside Docker
docker compose run --rm poller python -m pytest tests/ -v

# Locally (requires Python 3.12+)
pip install -e ".[dev]"
pytest tests/ -v
```

Tests that need PostgreSQL (`tests/test_sync_db.py`, `tests/test_outbox_relay_db.py`, `tests/test_migrations.py`) run only when `TEST_DATABASE_URL` points at a **throwaway** database (the `poller` schema is dropped and recreated). `TEST_RABBITMQ_URL` enables a test against a real broker.

```bash
docker compose run --rm -e TEST_DATABASE_URL=postgresql://poller:<password>@db:5432/codesystem_poller_test poller python -m pytest tests/ -v
```

### Adding a New Migration

Create a new file in `migrations/` with the next sequence number:

```bash
touch migrations/004_your_change.sql
```

The migration runner in Docker Compose applies **all** `.sql` files in order on **every** startup, so new migrations must be idempotent (`IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, guarded `DO $$` blocks).

### Customising the FHIR Bundle

The event coding, source name, and source endpoint are defined as constants at the top of `src/fhir_bundle.py`. Replace them with your organisation's values:

```python
EVENT_SYSTEM = "https://your-org.example/fhir/events"
EVENT_CODE = "codesystem-change"
EVENT_DISPLAY = "CodeSystem Change Notification"
SOURCE_NAME = "codesystem-polling-service"
SOURCE_ENDPOINT = "https://your-org.example/fhir/polling"
```

## Troubleshooting

**The first poll emitted an event for every concept.** This is expected: with no stored state, every concept is `concept_added`. See the note in [Quick Start](#quick-start) to suppress the initial load.

**I see "hash changed but no concept diffs" in the logs.** The resource-level metadata (e.g., `version`, `date`, `count`) changed but no concepts were modified. The service updates the stored hash to avoid re-parsing on the next cycle but does not emit events. This is correct behaviour.

**Every poll cycle shows changes even though nothing changed.** The FHIR server is returning non-deterministic JSON (different field ordering or whitespace each time). Set `CANONICAL_HASH=true` in your `.env` file.

**Outbox rows are stuck as unpublished.** Run `python -m src.outbox_admin status`: `last_error` says why. Common causes: the target is unreachable (check `RABBITMQ_URL` / `FHIR_TARGET_URL`), no queue is bound to the exchange (unroutable), a rejected Bundle is `dead`, or `RELAY_ENABLED=false`. Pending batches are delivered automatically once the cause is fixed.

**I need to re-baseline after a schema change.** Truncate the tables (see the reset command in [Useful Commands](#useful-commands)) and restart the service. The next poll stores a fresh baseline and emits every concept as added, unless the relay is disabled while it runs.
