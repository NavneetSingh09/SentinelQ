import sys
sys.path.append("/app")

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Any
from confluent_kafka import Producer
from sqlalchemy.orm import Session
import json
import os

from shared.models import Job, Priority, AlertSeverity, TOPICS, SEVERITY_PRIORITY_MAP
from shared.database import init_db, get_db, JobRecord
from shared.state import set_job_status, get_job_status, cache_job, get_metrics

app = FastAPI(
    title="SentinelQ",
    description="Distributed Incident Alert Processor",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer_conf = {"bootstrap.servers": KAFKA_SERVERS}
producer = Producer(producer_conf)


@app.on_event("startup")
def startup():
    init_db()


# ── Request / Response schemas ──────────────────────────────────────────────

class AlertSubmit(BaseModel):
    alert_name: str
    severity: AlertSeverity
    source: str = "manual"
    priority: Optional[Priority] = None   # auto-derived from severity if omitted
    payload: dict[str, Any] = {}
    max_retries: int = 3


class JobResponse(BaseModel):
    id: str
    alert_name: str
    severity: str
    priority: str
    source: str
    status: str
    retry_count: int
    created_at: str
    worker_id: Optional[str]
    error: Optional[str]


# ── Routes ───────────────────────────────────────────────────────────────────

@app.post("/alerts", response_model=dict, status_code=202)
def submit_alert(alert: AlertSubmit, db: Session = Depends(get_db)):
    """Submit a new alert for processing."""
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

    # Persist to PostgreSQL
    record = JobRecord(
        id=job.id,
        alert_name=job.alert_name,
        severity=job.severity.value,
        priority=job.priority.value,
        source=job.source,
        payload=job.payload,
        status=job.status.value,
        max_retries=job.max_retries,
    )
    db.add(record)
    db.commit()

    # Cache in Redis for fast lookups
    cache_job(job)
    set_job_status(job.id, job.status)

    # Publish to Kafka
    producer.produce(topic, key=job.id, value=job.to_kafka_message())
    producer.flush()

    return {
        "job_id": job.id,
        "topic": topic,
        "priority": priority.value,
        "message": f"Alert queued on {topic}"
    }


@app.get("/alerts/{job_id}", response_model=dict)
def get_alert(job_id: str, db: Session = Depends(get_db)):
    """Get current status of an alert job."""
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
        "created_at": str(record.created_at),
        "source": "db"
    }


@app.get("/alerts", response_model=list)
def list_alerts(
    status: Optional[str] = None,
    priority: Optional[str] = None,
    limit: int = 50,
    db: Session = Depends(get_db)
):
    """List all alert jobs with optional filters."""
    query = db.query(JobRecord)
    if status:
        query = query.filter(JobRecord.status == status)
    if priority:
        query = query.filter(JobRecord.priority == priority)
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
        }
        for r in records
    ]


@app.post("/alerts/{job_id}/retry")
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


@app.get("/metrics")
def metrics():
    """Live queue metrics from Redis."""
    return get_metrics()


@app.get("/health")
def health():
    return {"status": "ok", "service": "SentinelQ API"}
