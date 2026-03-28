# CodeSystem Change Poller

A service that polls a FHIR R4 CodeSystem API endpoint, detects changes in concepts (additions, modifications, removals), and emits standardised FHIR R4 message Bundles to RabbitMQ.

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
5. **Persist** — In a **single PostgreSQL transaction**: the sync state is updated, concept rows are upserted/deleted, and change events are inserted into an outbox table. This guarantees atomicity — either everything is committed or nothing is.
6. **Relay** — A background thread polls the outbox table, builds a FHIR R4 message Bundle from unpublished rows, publishes it to RabbitMQ, and marks the rows as published.

The **transactional outbox pattern** ensures that if the service crashes between committing state and publishing to RabbitMQ, the relay will catch up on the next cycle. No lost events, no duplicate full-table notifications.

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
                              │  │ UPSERT/DEL concept_state rows  │  │
                              │  │ INSERT change_outbox rows      │  │
                              │  └────────────────────────────────┘  │
                              └───────────────────┬──────────────────┘
                                                  │
                                                  ▼
                              ┌──────────────────────────────────────┐
                              │  Outbox Relay (background thread)    │
                              │  Polls outbox → builds FHIR Bundle   │
                              │  → publishes to RabbitMQ             │
                              └───────────────────┬──────────────────┘
                                                  │
                                                  ▼
                              ┌──────────────────────────────────────┐
                              │  RabbitMQ topic exchange              │
                              │  codesystem.<slug>.changed            │
                              └──────────────────────────────────────┘
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
│   └── 001_init.sql           # PostgreSQL schema
├── src/
│   ├── __init__.py
│   ├── config.py              # Pydantic settings + CodeSystemEntry + load_codesystems()
│   ├── db.py                  # psycopg3 connection + transaction helper
│   ├── poller.py              # HTTP fetch + SHA-256 hash comparison
│   ├── differ.py              # Concept flattening, diffing, state persistence
│   ├── fhir_bundle.py         # FHIR R4 message Bundle builder
│   ├── fhir_forwarder.py      # RabbitMQ consumer → FHIR $process-message POST
│   ├── outbox_relay.py        # Outbox → RabbitMQ relay
│   ├── scheduler.py           # Poll cycle orchestration (iterates all CodeSystems)
│   └── main.py                # Entry point (scheduler + relay)
└── tests/
    ├── __init__.py
    └── test_differ.py          # Unit tests for flatten + diff logic
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

Edit `.env` to set your PostgreSQL and RabbitMQ credentials.

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
- Run the SQL migration (`migrations/001_init.sql`)
- Start RabbitMQ and wait for it to be healthy
- Build and start the poller service

5. **Check the logs**

```bash
docker compose logs -f poller
```

On the first run, the poller stores the full snapshot as the baseline state. No change events are emitted because there is no prior state to diff against. Subsequent poll cycles will only emit events for actual changes.

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
| `RABBITMQ_USER` | Yes | — | RabbitMQ username |
| `RABBITMQ_PASSWORD` | Yes | — | RabbitMQ password |
| `RABBITMQ_URL` | Yes | — | RabbitMQ AMQP connection string |
| `POLL_CRON` | No | `0 */4 * * *` | Cron expression controlling poll frequency |
| `CANONICAL_HASH` | No | `false` | Set to `true` if the FHIR server returns non-deterministic JSON |
| `RABBITMQ_EXCHANGE` | No | `codesystem.changes` | Name of the RabbitMQ topic exchange |
| `OUTBOX_POLL_INTERVAL` | No | `5` | Seconds between outbox relay cycles |
| `LOG_LEVEL` | No | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `HTTP_TIMEOUT` | No | `30` | Timeout in seconds for FHIR API HTTP requests |

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
```

## Deployment

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
psql -h <pg-host> -U <pg-user> -d <pg-database> -f migrations/001_init.sql
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

- **Replicas**: Run exactly **1 replica**. Multiple replicas would cause duplicate poll cycles and race conditions on the outbox. If you need HA, use an active-passive setup with leader election.
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
  "id": "urn:uuid:...",
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

The forwarder retries with exponential backoff on transient errors (5xx, network failures). On permanent errors (4xx), it logs the rejection and ACKs the message to avoid infinite requeue. If all retries are exhausted, the message is NACKed with requeue. In production, you should configure a **dead-letter exchange (DLX)** on the `fhir-forwarder` queue so that repeatedly failing messages move to a dead-letter queue instead of looping forever.

| Forwarder variable | Default | Description |
|---|---|---|
| `FHIR_TARGET_URL` | (required) | Full URL to the downstream `$process-message` endpoint |
| `FHIR_AUTH_TOKEN` | (empty) | Bearer token for authentication (omit if not needed) |
| `RABBITMQ_QUEUE` | `fhir-forwarder` | Durable queue name for this consumer |
| `RABBITMQ_ROUTING_KEY` | `codesystem.#` | Routing key pattern (`#` = all CodeSystems) |
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
| `Published FHIR Bundle to ...` | Events successfully sent to RabbitMQ |
| `FHIR API request failed` | API is unreachable — will retry next cycle |
| `Relay cycle failed` | RabbitMQ is unreachable — outbox rows remain, will retry |

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

# Count concepts currently tracked
docker compose exec db psql -U poller -d codesystem_poller -c \
  "SELECT system_url, COUNT(*) AS concepts
   FROM poller.codesystem_concept_state GROUP BY system_url;"

# Force a manual poll cycle for all configured CodeSystems
docker compose run --rm poller python -c \
  "from src.scheduler import run_poll_cycle; run_poll_cycle()"

# Reset all state (next poll stores a fresh baseline, no events emitted)
docker compose exec db psql -U poller -d codesystem_poller -c \
  "TRUNCATE poller.codesystem_sync_state,
            poller.codesystem_concept_state,
            poller.change_outbox;"

# Access RabbitMQ management UI
# Open http://localhost:15672 (guest / guest)
```

## Failure Modes and Recovery

| Scenario | What happens | Recovery |
|---|---|---|
| **FHIR API unreachable** | Poll cycle logs the error and exits. State is untouched. | Automatic retry on next cron tick. |
| **FHIR API returns non-200** | Same as above. | Same. |
| **PostgreSQL down** | Transaction fails, nothing is committed. No partial state, no orphaned outbox rows. | Service retries on next cycle once PG is back. |
| **RabbitMQ down** | Outbox rows remain `published = false`. The relay retries every `OUTBOX_POLL_INTERVAL` seconds. | Events are delivered automatically once RabbitMQ recovers. No manual intervention needed. |
| **Service crash mid-transaction** | PostgreSQL rolls back the uncommitted transaction. | Next cycle diffs against the last committed state. No re-notification. |
| **Service crash after commit, before RabbitMQ publish** | Outbox rows exist but are unpublished. | Relay picks them up on restart. |
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

### Adding a New Migration

Create a new file in `migrations/` with the next sequence number:

```bash
touch migrations/002_your_change.sql
```

The migration runner in Docker Compose applies all `.sql` files in order on startup.

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

**The first poll emits no events.** This is expected. On the first run, there is no prior state to diff against, so the entire CodeSystem is stored as the baseline. Events will be emitted starting from the second poll if anything has changed.

**I see "hash changed but no concept diffs" in the logs.** The resource-level metadata (e.g., `version`, `date`, `count`) changed but no concepts were modified. The service updates the stored hash to avoid re-parsing on the next cycle but does not emit events. This is correct behaviour.

**Every poll cycle shows changes even though nothing changed.** The FHIR server is returning non-deterministic JSON (different field ordering or whitespace each time). Set `CANONICAL_HASH=true` in your `.env` file.

**Outbox rows are stuck as unpublished.** RabbitMQ is unreachable. Check `docker compose logs rabbitmq` and verify the `RABBITMQ_URL` in your `.env`. Once RabbitMQ is back, the relay will publish automatically.

**I need to re-baseline after a schema change.** Truncate all three tables (see the reset command in [Useful Commands](#useful-commands)) and restart the service. The next poll will store a fresh baseline.
