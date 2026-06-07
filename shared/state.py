import redis
import json
import os
from datetime import datetime, timedelta
from shared.models import Job, JobStatus

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

_client = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(REDIS_URL, decode_responses=True)
    return _client


def set_job_status(job_id: str, status: JobStatus, worker_id: str = None, error: str = None) -> None:
    r = get_redis()
    data: dict = {"status": status.value}
    if worker_id:
        data["worker_id"] = worker_id
    if error:
        data["error"] = error
    r.hset(f"job:{job_id}", mapping=data)
    r.expire(f"job:{job_id}", int(timedelta(hours=24).total_seconds()))


def get_job_status(job_id: str) -> dict:
    r = get_redis()
    return r.hgetall(f"job:{job_id}")


def acquire_lock(job_id: str, worker_id: str, ttl_seconds: int = 60) -> bool:
    """Distributed lock — only one worker can claim a job."""
    r = get_redis()
    return r.set(f"lock:{job_id}", worker_id, nx=True, ex=ttl_seconds)


def release_lock(job_id: str, worker_id: str) -> None:
    r = get_redis()
    current = r.get(f"lock:{job_id}")
    if current == worker_id:
        r.delete(f"lock:{job_id}")


def cache_job(job: Job) -> None:
    """Cache full job object for fast status lookups."""
    r = get_redis()
    r.setex(f"jobdata:{job.id}", int(timedelta(hours=1).total_seconds()), job.model_dump_json())


def get_cached_job(job_id: str) -> Job | None:
    r = get_redis()
    data = r.get(f"jobdata:{job_id}")
    if data:
        return Job.model_validate_json(data)
    return None


def check_dedup(alert_name: str, source: str) -> str | None:
    """Return existing job_id if a duplicate exists within 60s window, else None."""
    r = get_redis()
    return r.get(f"dedup:{alert_name}:{source}")


def set_dedup(alert_name: str, source: str, job_id: str) -> None:
    """Set dedup key with 60s TTL to prevent duplicate alert submissions."""
    r = get_redis()
    r.setex(f"dedup:{alert_name}:{source}", 60, job_id)


def publish_job_update(job_id: str, status: str, worker_id: str | None = None) -> None:
    """Publish a job status change to Redis pub/sub channel for WebSocket clients."""
    r = get_redis()
    message = json.dumps({
        "job_id": job_id,
        "status": status,
        "worker_id": worker_id,
        "timestamp": datetime.utcnow().isoformat(),
    })
    r.publish("job:updates", message)


def increment_metrics(topic: str) -> None:
    r = get_redis()
    r.incr("metrics:total_processed")
    r.incr(f"metrics:topic:{topic}")


def get_metrics() -> dict:
    r = get_redis()
    keys = r.keys("metrics:*")
    return {k: r.get(k) for k in keys}
