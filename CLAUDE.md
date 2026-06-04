# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Full stack (Docker)
```bash
docker-compose up -d                         # Start everything (Kafka, Zookeeper, Redis, Postgres, API, 3 workers)
docker-compose up -d zookeeper kafka redis postgres  # Infrastructure only (for local dev)
docker-compose logs -f api                   # Tail API logs
docker-compose logs -f worker                # Tail worker logs
```

### Local dev (no Docker for app code)
```bash
pip install -r requirements.txt

uvicorn api.main:app --reload --port 8000    # API with hot reload
python -m workers.worker                     # Single worker instance
python scripts/seed_alerts.py                # Seed 7 sample alerts
```

### Manual testing
```bash
curl -X POST http://localhost:8000/alerts -H "Content-Type: application/json" \
  -d '{"alert_name": "test", "severity": "critical", "source": "manual"}'

curl http://localhost:8000/alerts            # List all jobs
curl http://localhost:8000/metrics           # Redis counters
curl http://localhost:8000/docs              # Swagger UI
```

## Architecture

The system is a distributed alert processor with three tiers:

**API (`api/main.py`)** — FastAPI app that validates incoming alerts, writes to PostgreSQL, caches to Redis, and publishes to Kafka. Routes: `POST /alerts`, `GET /alerts`, `GET /alerts/{id}`, `POST /alerts/{id}/retry`, `GET /metrics`. The Kafka `Producer` is a module-level singleton initialized at startup.

**Workers (`workers/worker.py`)** — Each worker instance is a Kafka consumer in the `sentinelq-workers` group subscribing to all 4 priority topics simultaneously. Workers use manual offset commit (`enable.auto.commit=False`) — the offset is committed only after `process_message` returns. Each job goes through a distributed lock (`lock:{job_id}` in Redis, NX+EX) to ensure only one worker processes each message even with 3+ replicas consuming from shared partitions.

**Shared layer (`shared/`)**:
- `models.py` — Pydantic `Job` model (used for in-flight data), enums (`Priority`, `JobStatus`, `AlertSeverity`), `TOPICS` dict mapping priority → Kafka topic name, `SEVERITY_PRIORITY_MAP` for auto-deriving priority from severity
- `database.py` — SQLAlchemy `JobRecord` ORM model (`jobs` table), `init_db()` (called at startup of both API and worker), `get_db()` FastAPI dependency
- `state.py` — Redis helper functions: job status hashes (`job:{id}`), distributed locks (`lock:{id}`), full job cache (`jobdata:{id}`), metrics counters (`metrics:*`)

## Data flow

1. Alert submitted → API writes `JobRecord` to PostgreSQL (authoritative store), hashes status in Redis, publishes `Job.model_dump_json()` to the appropriate Kafka topic
2. Worker polls Kafka, deserializes message back to `Job`, acquires Redis lock, calls the priority-specific handler, updates both Redis status and PostgreSQL on every state transition
3. On failure: exponential backoff (`2^n + jitter`, max 60s), requeue to same topic; after `max_retries` exhausted → mark DEAD and publish to `alerts.dead-letter`
4. `POST /alerts/{id}/retry` resets the DB record to `queued` and re-publishes to Kafka (does not touch Redis lock)

## Key design details

- `sys.path.append("/app")` at the top of `api/main.py` and `workers/worker.py` is required for the Docker container path; the `shared/` package is volume-mounted into `/app/shared`
- `WORKER_ID` is a random 8-char UUID prefix generated once per process — used as the Redis lock value and logged with every job event
- Job status has two storage locations that can diverge: Redis hash (`job:{id}`) expires after 24h and is the fast-path lookup; PostgreSQL is the durable audit log. `GET /alerts/{id}` checks Redis first, falls back to DB
- The `Job` Pydantic model is the transport format (Kafka messages are `model_dump_json()`); `JobRecord` is the ORM model — they are kept in sync manually in `update_db()`
- Worker handlers (`handle_critical`, `handle_high`, etc.) simulate failures with random probability to exercise the retry path; replace these with real integrations (PagerDuty, Slack, etc.)

## Environment variables

| Variable | Default | Used by |
|---|---|---|
| `DATABASE_URL` | `postgresql://sentinelq:sentinelq@localhost:5432/sentinelq` | API, Worker |
| `REDIS_URL` | `redis://localhost:6379` | API, Worker |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | API, Worker |

Docker Compose overrides these to use internal service names (`kafka:29092`, `redis:6379`, `postgres:5432`).
