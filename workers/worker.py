import sys
sys.path.append("/app")

import os
import time
import uuid
import random
import logging
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError
from sqlalchemy.orm import Session

from shared.models import Job, JobStatus, Priority, TOPICS
from shared.database import SessionLocal, JobRecord, init_db
from shared.state import (
    set_job_status, acquire_lock, release_lock,
    increment_metrics, cache_job, publish_job_update,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("sentinelq.worker")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
WORKER_ID = f"worker-{str(uuid.uuid4())[:8]}"

SUBSCRIBE_TOPICS = [
    TOPICS[Priority.CRITICAL],
    TOPICS[Priority.HIGH],
    TOPICS[Priority.NORMAL],
    TOPICS[Priority.LOW],
]

consumer = Consumer({
    "bootstrap.servers": KAFKA_SERVERS,
    "group.id": "sentinelq-workers",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

producer = Producer({"bootstrap.servers": KAFKA_SERVERS})


# ── Job handlers (simulate real integrations) ─────────────────────────────────

def handle_critical(job: Job) -> bool:
    log.info(f"[CRITICAL] Processing {job.alert_name} — paging on-call engineer")
    time.sleep(0.5)
    if random.random() < 0.1:
        raise Exception("PagerDuty webhook timeout")
    return True


def handle_high(job: Job) -> bool:
    log.info(f"[HIGH] Processing {job.alert_name} — sending Slack alert")
    time.sleep(0.8)
    if random.random() < 0.15:
        raise Exception("Slack API rate limit")
    return True


def handle_normal(job: Job) -> bool:
    log.info(f"[NORMAL] Processing {job.alert_name} — logging to incident tracker")
    time.sleep(1.0)
    return True


def handle_low(job: Job) -> bool:
    log.info(f"[LOW] Processing {job.alert_name} — batching for daily digest")
    time.sleep(0.3)
    return True


HANDLERS = {
    Priority.CRITICAL: handle_critical,
    Priority.HIGH:     handle_high,
    Priority.NORMAL:   handle_normal,
    Priority.LOW:      handle_low,
}


# ── Retry logic ───────────────────────────────────────────────────────────────

def exponential_backoff(retry_count: int) -> float:
    """2^n seconds with jitter, capped at 60 s."""
    return min(60.0, (2 ** retry_count) + random.uniform(0, 1))


def send_to_dead_letter(job: Job, error: str) -> None:
    job.status = JobStatus.DEAD
    job.error = error
    producer.produce(TOPICS["dead_letter"], key=job.id, value=job.to_kafka_message())
    producer.flush()
    log.warning(f"Job {job.id} sent to dead-letter after {job.retry_count} retries")


def update_db(job: Job, db: Session) -> None:
    record = db.query(JobRecord).filter(JobRecord.id == job.id).first()
    if record:
        record.status = job.status.value
        record.retry_count = job.retry_count
        record.worker_id = job.worker_id
        record.error = job.error
        record.updated_at = datetime.utcnow()
        db.commit()


# ── Main consume loop ─────────────────────────────────────────────────────────

def _transition(job: Job, status: JobStatus, db: Session, error: str = None) -> None:
    """Update job status in Redis, PostgreSQL, and broadcast to WebSocket clients."""
    job.status = status
    if error:
        job.error = error
    set_job_status(job.id, status, worker_id=job.worker_id, error=error)
    publish_job_update(job.id, status.value, job.worker_id)
    update_db(job, db)


def process_message(msg) -> None:
    db = SessionLocal()
    try:
        job = Job.from_kafka_message(msg.value().decode("utf-8"))
        job.worker_id = WORKER_ID
        topic = msg.topic()

        log.info(f"Worker {WORKER_ID} picked up job {job.id} [{job.priority.value}] from {topic}")

        if not acquire_lock(job.id, WORKER_ID):
            log.info(f"Job {job.id} already locked by another worker, skipping")
            return

        try:
            _transition(job, JobStatus.RUNNING, db)

            handler = HANDLERS.get(job.priority, handle_normal)
            handler(job)

            _transition(job, JobStatus.DONE, db)
            increment_metrics(topic)
            log.info(f"Job {job.id} completed successfully")

        except Exception as exc:
            error_msg = str(exc)
            job.retry_count += 1
            log.error(f"Job {job.id} failed (attempt {job.retry_count}/{job.max_retries}): {error_msg}")

            if job.retry_count >= job.max_retries:
                _transition(job, JobStatus.DEAD, db, error=error_msg)
                send_to_dead_letter(job, error_msg)
            else:
                delay = exponential_backoff(job.retry_count)
                log.info(f"Retrying job {job.id} in {delay:.1f}s")
                _transition(job, JobStatus.RETRYING, db, error=error_msg)
                time.sleep(delay)
                producer.produce(topic, key=job.id, value=job.to_kafka_message())
                producer.flush()

        finally:
            release_lock(job.id, WORKER_ID)

    except Exception as exc:
        log.error(f"Fatal error processing message: {exc}")
    finally:
        db.close()


def run() -> None:
    init_db()
    consumer.subscribe(SUBSCRIBE_TOPICS)
    log.info(f"Worker {WORKER_ID} started — subscribed to: {SUBSCRIBE_TOPICS}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Kafka error: {msg.error()}")
                continue

            process_message(msg)
            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        log.info(f"Worker {WORKER_ID} shutting down")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
