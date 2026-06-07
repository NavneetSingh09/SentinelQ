import sys
sys.path.append("/app")

import os
import uuid
import json
import logging
import asyncio
import threading
import time
from datetime import datetime
from typing import Optional, Any, List

from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
from confluent_kafka import Producer
from sqlalchemy.orm import Session
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from apscheduler.schedulers.background import BackgroundScheduler
from croniter import croniter as CronIter
import redis as redis_sync

from shared.models import Job, Priority, AlertSeverity, TOPICS, SEVERITY_PRIORITY_MAP
from shared.database import init_db, get_db, JobRecord, ScheduledAlert, SessionLocal
from shared.state import (
    set_job_status, get_job_status, cache_job,
    check_dedup, set_dedup, get_redis, publish_job_update,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sentinelq.api")

app = FastAPI(
    title="SentinelQ",
    description="Distributed Incident Alert Processor",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

producer = Producer({"bootstrap.servers": KAFKA_SERVERS})

# ── Prometheus metrics ────────────────────────────────────────────────────────

JOBS_SUBMITTED = Counter(
    "sentinelq_jobs_submitted_total",
    "Total jobs submitted by priority",
    ["priority"],
)
JOBS_PROCESSED = Gauge(
    "sentinelq_jobs_processed_total",
    "Total jobs processed per topic (sourced from Redis)",
    ["topic"],
)
JOBS_FAILED = Counter(
    "sentinelq_jobs_failed_total",
    "Total jobs that failed (all retries exhausted)",
)
QUEUE_DEPTH = Gauge(
    "sentinelq_queue_depth",
    "Estimated pending jobs per topic",
    ["topic"],
)
PROCESSING_DURATION = Histogram(
    "sentinelq_worker_processing_duration_seconds",
    "Worker processing duration in seconds",
    ["priority"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
)

# ── WebSocket connection manager ──────────────────────────────────────────────

class ConnectionManager:
    def __init__(self) -> None:
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket) -> None:
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: str) -> None:
        for ws in list(self.active):
            try:
                await ws.send_text(message)
            except Exception:
                self.disconnect(ws)


manager = ConnectionManager()
_ws_loop: Optional[asyncio.AbstractEventLoop] = None


def _redis_pubsub_listener() -> None:
    """Background thread: relay Redis pub/sub messages to all WebSocket clients."""
    while True:
        try:
            r = redis_sync.from_url(REDIS_URL, decode_responses=True)
            pubsub = r.pubsub()
            pubsub.subscribe("job:updates")
            for message in pubsub.listen():
                if message["type"] == "message" and _ws_loop and not _ws_loop.is_closed():
                    asyncio.run_coroutine_threadsafe(
                        manager.broadcast(message["data"]), _ws_loop
                    )
        except Exception as exc:
            log.warning(f"Redis pub/sub disconnected, reconnecting in 2s: {exc}")
            time.sleep(2)


# ── APScheduler ───────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()


def fire_scheduled_alerts() -> None:
    """Check for due scheduled alerts and submit them through the Kafka pipeline."""
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        due = (
            db.query(ScheduledAlert)
            .filter(ScheduledAlert.is_active == True)
            .filter(ScheduledAlert.next_run_at <= now)
            .all()
        )
        for schedule in due:
            try:
                severity = AlertSeverity(schedule.severity)
                priority = SEVERITY_PRIORITY_MAP[severity]
                topic = TOPICS[priority]

                job = Job(
                    alert_name=schedule.alert_name,
                    severity=severity,
                    priority=priority,
                    source=schedule.source,
                    payload=schedule.payload or {},
                )
                record = JobRecord(
                    id=job.id,
                    alert_name=job.alert_name,
                    severity=job.severity.value,
                    priority=job.priority.value,
                    source=job.source,
                    payload=job.payload,
                    status=job.status.value,
                    max_retries=job.max_retries,
                    deduplicated=False,
                )
                db.add(record)

                cron = CronIter(schedule.cron_expr, now)
                schedule.last_run_at = now
                schedule.next_run_at = cron.get_next(datetime)

                producer.produce(topic, key=job.id, value=job.to_kafka_message())
                producer.flush()
                log.info(f"Fired schedule {schedule.id}: {schedule.alert_name} → {topic}")

            except Exception as exc:
                log.error(f"Failed to fire schedule {schedule.id}: {exc}")

        db.commit()
    except Exception as exc:
        log.error(f"Scheduler check failed: {exc}")
    finally:
        db.close()


# ── Startup / Shutdown ────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup() -> None:
    global _ws_loop
    _ws_loop = asyncio.get_event_loop()

    init_db()

    t = threading.Thread(target=_redis_pubsub_listener, daemon=True)
    t.start()

    scheduler.add_job(fire_scheduled_alerts, "interval", seconds=30, id="schedule_checker")
    scheduler.start()
    log.info("SentinelQ API started — scheduler and WebSocket relay active")


@app.on_event("shutdown")
def shutdown() -> None:
    scheduler.shutdown(wait=False)


# ── Request / Response schemas ────────────────────────────────────────────────

class AlertSubmit(BaseModel):
    alert_name: str
    severity: AlertSeverity
    source: str = "manual"
    priority: Optional[Priority] = None
    payload: dict[str, Any] = {}
    max_retries: int = 3


class ScheduleCreate(BaseModel):
    alert_name: str
    severity: AlertSeverity
    source: str = "scheduler"
    cron_expr: str
    payload: dict[str, Any] = {}


# ── Alert routes ──────────────────────────────────────────────────────────────

@app.post("/alerts", response_model=dict, status_code=202)
def submit_alert(alert: AlertSubmit, db: Session = Depends(get_db)):
    """Submit a new alert for processing. Returns 409 if a duplicate arrives within 60 s."""
    existing = check_dedup(alert.alert_name, alert.source)
    if existing:
        raise HTTPException(
            status_code=409,
            detail={"message": "Duplicate alert rejected", "existing_job_id": existing},
        )

    priority = alert.priority or SEVERITY_PRIORITY_MAP[alert.severity]
    topic = TOPICS[priority]

    job = Job(
        alert_name=alert.alert_name,
        severity=alert.severity,
        priority=priority,
        source=alert.source,
        payload=alert.payload,
        max_retries=alert.max_retries,
    )

    record = JobRecord(
        id=job.id,
        alert_name=job.alert_name,
        severity=job.severity.value,
        priority=job.priority.value,
        source=job.source,
        payload=job.payload,
        status=job.status.value,
        max_retries=job.max_retries,
        deduplicated=False,
    )
    db.add(record)
    db.commit()

    cache_job(job)
    set_job_status(job.id, job.status)
    set_dedup(alert.alert_name, alert.source, job.id)

    JOBS_SUBMITTED.labels(priority=priority.value).inc()
    r = get_redis()
    r.incr(f"metrics:submitted:{topic}")

    producer.produce(topic, key=job.id, value=job.to_kafka_message())
    producer.flush()

    return {"job_id": job.id, "topic": topic, "priority": priority.value, "message": f"Alert queued on {topic}"}


@app.get("/alerts", response_model=list)
def list_alerts(
    status: Optional[str] = None,
    priority: Optional[str] = None,
    severity: Optional[str] = None,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """List alert jobs with optional filters by status, priority, or severity."""
    query = db.query(JobRecord)
    if status:
        query = query.filter(JobRecord.status == status)
    if priority:
        query = query.filter(JobRecord.priority == priority)
    if severity:
        query = query.filter(JobRecord.severity == severity)
    records = query.order_by(JobRecord.created_at.desc()).limit(limit).all()
    return [
        {
            "id": r.id,
            "alert_name": r.alert_name,
            "severity": r.severity,
            "priority": r.priority,
            "status": r.status,
            "retry_count": r.retry_count,
            "worker_id": r.worker_id,
            "created_at": str(r.created_at),
            "error": r.error,
            "deduplicated": r.deduplicated,
        }
        for r in records
    ]


@app.get("/alerts/{job_id}", response_model=dict)
def get_alert(job_id: str, db: Session = Depends(get_db)):
    """Get current status of an alert job. Checks Redis cache first, falls back to DB."""
    redis_status = get_job_status(job_id)
    if redis_status:
        return {"job_id": job_id, "source": "cache", **redis_status}

    record = db.query(JobRecord).filter(JobRecord.id == job_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": record.id,
        "alert_name": record.alert_name,
        "severity": record.severity,
        "priority": record.priority,
        "status": record.status,
        "retry_count": record.retry_count,
        "worker_id": record.worker_id,
        "error": record.error,
        "payload": record.payload,
        "deduplicated": record.deduplicated,
        "created_at": str(record.created_at),
        "updated_at": str(record.updated_at),
        "source": "db",
    }


@app.post("/alerts/{job_id}/retry", response_model=dict)
def retry_alert(job_id: str, db: Session = Depends(get_db)):
    """Manually requeue a failed or dead job."""
    record = db.query(JobRecord).filter(JobRecord.id == job_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")
    if record.status not in ("failed", "dead"):
        raise HTTPException(status_code=400, detail="Only failed or dead jobs can be retried")

    record.status = "queued"
    record.retry_count = 0
    record.error = None
    db.commit()

    job = Job(
        id=record.id,
        alert_name=record.alert_name,
        severity=AlertSeverity(record.severity),
        priority=Priority(record.priority),
        source=record.source,
        payload=record.payload or {},
        max_retries=record.max_retries,
    )
    topic = TOPICS[job.priority]
    producer.produce(topic, key=job.id, value=job.to_kafka_message())
    producer.flush()

    return {"message": f"Job {job_id} requeued on {topic}"}


# ── Metrics & stats routes ────────────────────────────────────────────────────

@app.get("/metrics", response_class=Response)
def prometheus_metrics():
    """Prometheus text-format metrics endpoint for Grafana/Prometheus scraping."""
    r = get_redis()
    for priority, topic in TOPICS.items():
        if priority == "dead_letter":
            continue
        processed = int(r.get(f"metrics:topic:{topic}") or 0)
        submitted = int(r.get(f"metrics:submitted:{topic}") or 0)
        JOBS_PROCESSED.labels(topic=topic).set(processed)
        QUEUE_DEPTH.labels(topic=topic).set(max(0, submitted - processed))

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/stats", response_model=dict)
def get_stats(db: Session = Depends(get_db)):
    """JSON stats summary for the React dashboard: totals, success rate, queue depths."""
    r = get_redis()

    done_count = db.query(JobRecord).filter(JobRecord.status == "done").count()
    failed_count = db.query(JobRecord).filter(JobRecord.status == "failed").count()
    dead_count = db.query(JobRecord).filter(JobRecord.status == "dead").count()
    total_count = db.query(JobRecord).count()
    success_rate = round((done_count / total_count * 100) if total_count > 0 else 0.0, 1)

    queue_depths: dict = {}
    for priority, topic in TOPICS.items():
        if priority == "dead_letter":
            continue
        submitted = int(r.get(f"metrics:submitted:{topic}") or 0)
        processed = int(r.get(f"metrics:topic:{topic}") or 0)
        queue_depths[topic] = max(0, submitted - processed)

    by_priority: dict = {}
    for p in [Priority.CRITICAL, Priority.HIGH, Priority.NORMAL, Priority.LOW]:
        by_priority[p.value] = db.query(JobRecord).filter(JobRecord.priority == p.value).count()

    return {
        "total_processed": done_count,
        "total_submitted": total_count,
        "success_rate": success_rate,
        "failed_jobs": failed_count,
        "dead_letter_count": dead_count,
        "queue_depths": queue_depths,
        "by_priority": by_priority,
    }


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint that streams live job status updates from Redis pub/sub."""
    await manager.connect(websocket)
    try:
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        manager.disconnect(websocket)


# ── Schedule routes ───────────────────────────────────────────────────────────

@app.post("/schedules", response_model=dict, status_code=201)
def create_schedule(schedule: ScheduleCreate, db: Session = Depends(get_db)):
    """Create a new cron-style recurring alert schedule."""
    try:
        cron = CronIter(schedule.cron_expr, datetime.utcnow())
        next_run = cron.get_next(datetime)
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=422, detail=f"Invalid cron expression: {exc}")

    record = ScheduledAlert(
        id=str(uuid.uuid4()),
        alert_name=schedule.alert_name,
        severity=schedule.severity.value,
        source=schedule.source,
        payload=schedule.payload,
        cron_expr=schedule.cron_expr,
        is_active=True,
        next_run_at=next_run,
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    return {
        "id": record.id,
        "alert_name": record.alert_name,
        "severity": record.severity,
        "source": record.source,
        "cron_expr": record.cron_expr,
        "is_active": record.is_active,
        "next_run_at": str(record.next_run_at),
        "created_at": str(record.created_at),
    }


@app.get("/schedules", response_model=list)
def list_schedules(db: Session = Depends(get_db)):
    """List all scheduled alerts."""
    records = db.query(ScheduledAlert).order_by(ScheduledAlert.created_at.desc()).all()
    return [
        {
            "id": r.id,
            "alert_name": r.alert_name,
            "severity": r.severity,
            "source": r.source,
            "cron_expr": r.cron_expr,
            "is_active": r.is_active,
            "last_run_at": str(r.last_run_at) if r.last_run_at else None,
            "next_run_at": str(r.next_run_at) if r.next_run_at else None,
            "created_at": str(r.created_at),
        }
        for r in records
    ]


@app.delete("/schedules/{schedule_id}", status_code=204)
def delete_schedule(schedule_id: str, db: Session = Depends(get_db)):
    """Delete a scheduled alert permanently."""
    record = db.query(ScheduledAlert).filter(ScheduledAlert.id == schedule_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Schedule not found")
    db.delete(record)
    db.commit()


@app.patch("/schedules/{schedule_id}/toggle", response_model=dict)
def toggle_schedule(schedule_id: str, db: Session = Depends(get_db)):
    """Toggle a schedule between active and inactive."""
    record = db.query(ScheduledAlert).filter(ScheduledAlert.id == schedule_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Schedule not found")
    record.is_active = not record.is_active
    db.commit()
    return {"id": record.id, "is_active": record.is_active}


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "SentinelQ API"}
