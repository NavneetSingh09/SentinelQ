from enum import Enum
from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field
import uuid


class Priority(str, Enum):
    CRITICAL = "critical"   # P1 — fires immediately, skips queue depth
    HIGH = "high"           # P2
    NORMAL = "normal"       # P3
    LOW = "low"             # P4 — batched, low urgency


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD = "dead"           # exhausted all retries → dead-letter


class AlertSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class Job(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    alert_name: str
    severity: AlertSeverity
    priority: Priority
    source: str                        # e.g. "prometheus", "datadog", "manual"
    payload: dict[str, Any] = {}
    status: JobStatus = JobStatus.QUEUED
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    worker_id: Optional[str] = None
    error: Optional[str] = None

    def to_kafka_message(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_kafka_message(cls, data: str) -> "Job":
        return cls.model_validate_json(data)


# Kafka topic names
TOPICS = {
    Priority.CRITICAL: "alerts.critical",
    Priority.HIGH:     "alerts.high",
    Priority.NORMAL:   "alerts.normal",
    Priority.LOW:      "alerts.low",
    "dead_letter":     "alerts.dead-letter",
}

# Map severity → default priority
SEVERITY_PRIORITY_MAP = {
    AlertSeverity.CRITICAL: Priority.CRITICAL,
    AlertSeverity.WARNING:  Priority.HIGH,
    AlertSeverity.INFO:     Priority.LOW,
}
