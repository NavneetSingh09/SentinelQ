# SentinelQ — Distributed Incident Alert Processor

A production-grade distributed job scheduler built for real-time alert triage.
When monitoring systems fire hundreds of alerts simultaneously, SentinelQ ensures
**P1 outages are never buried behind low-severity noise**.

## Architecture

```
Clients (REST / Cron / SDK)
        │
        ▼
  Job Queue Manager        ← validates, prioritizes, deduplicates
        │
        ▼
  Apache Kafka             ← 4 priority topics + dead-letter
  ┌─────────────────────────────────────────────┐
  │  alerts.critical  alerts.high               │
  │  alerts.normal    alerts.low                │
  │  alerts.dead-letter                         │
  └─────────────────────────────────────────────┘
        │
        ▼
  Worker Pool (3+ instances)
  ├── Distributed lock via Redis (no double-processing)
  ├── Exponential backoff retry (2^n + jitter, max 60s)
  └── Dead-letter after max_retries exhausted
        │
        ▼
  Redis          ← live job state, metrics, locks
  PostgreSQL     ← full audit log, history, retry tracking
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| API | FastAPI + Uvicorn |
| Message Broker | Apache Kafka (confluent-kafka) |
| State Store | Redis |
| Database | PostgreSQL + SQLAlchemy |
| Containerization | Docker Compose |

## Quick Start

### 1. Start infrastructure

```bash
docker-compose up -d
```

This starts: Kafka, Zookeeper, Redis, PostgreSQL, API (port 8000), 3 Workers.

### 2. Submit alerts

```bash
# Single alert via curl
curl -X POST http://localhost:8000/alerts \
  -H "Content-Type: application/json" \
  -d '{
    "alert_name": "CPU usage > 95% on prod-api-1",
    "severity": "critical",
    "source": "prometheus",
    "payload": {"host": "prod-api-1", "value": 97.3}
  }'

# Or run the seed script to fire 7 sample alerts
python scripts/seed_alerts.py
```

### 3. Check status

```bash
# Get job status
curl http://localhost:8000/alerts/{job_id}

# List all jobs
curl http://localhost:8000/alerts

# Filter by status
curl "http://localhost:8000/alerts?status=failed&priority=critical"

# Live metrics
curl http://localhost:8000/metrics

# API docs (Swagger UI)
open http://localhost:8000/docs
```

### 4. Retry a failed job

```bash
curl -X POST http://localhost:8000/alerts/{job_id}/retry
```

## Priority System

| Severity | Auto-mapped Priority | Kafka Topic |
|----------|---------------------|-------------|
| critical | CRITICAL | alerts.critical |
| warning  | HIGH | alerts.high |
| info     | LOW | alerts.low |

You can override priority explicitly in the request payload.

## Key Features

- **Priority lanes** — 4 Kafka topics ensure critical alerts are never blocked by low-priority queue depth
- **Distributed locking** — Redis-based locks prevent two workers from processing the same job
- **Exponential backoff** — retries use `2^n + jitter` delay (capped at 60s) to avoid thundering herd
- **Dead-letter queue** — permanently failed jobs land in `alerts.dead-letter` for inspection/replay
- **Manual retry** — `POST /alerts/{id}/retry` requeues any failed/dead job
- **Full audit log** — every state transition persisted to PostgreSQL
- **Live metrics** — Redis counters exposed via `/metrics`

## Local Development (without Docker)

```bash
pip install -r requirements.txt

# Start infra only
docker-compose up -d zookeeper kafka redis postgres

# Run API
uvicorn api.main:app --reload --port 8000

# Run worker (in separate terminal)
python -m workers.worker
```

## Project Structure

```
sentinelq/
├── api/
│   ├── main.py          # FastAPI app, all routes
│   └── Dockerfile
├── workers/
│   ├── worker.py        # Kafka consumer, retry logic, handlers
│   └── Dockerfile
├── shared/
│   ├── models.py        # Pydantic models, enums, topic map
│   ├── database.py      # SQLAlchemy models + session
│   └── state.py         # Redis state manager
├── scripts/
│   └── seed_alerts.py   # Test data seeder
├── docker-compose.yml
└── requirements.txt
```

## Phase 2 (coming next)

- [ ] Cron-style recurring alert schedules
- [ ] React dashboard with live job feed
- [ ] Prometheus `/metrics` endpoint + Grafana dashboard
- [ ] Alert deduplication (same alert firing repeatedly → one job)
- [ ] WebSocket live updates on job status
