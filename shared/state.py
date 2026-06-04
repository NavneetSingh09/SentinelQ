import redis
import json
import os
from datetime import timedelta
from shared.models import Job, JobStatus

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

_client = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(REDIS_URL, decode_responses=True)
    return _client


def set_job_status(job_id: str, status: JobStatus, worker_id: str = None, error: str = None):
    r = get_redis()
    data = {"status": status.value}
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


def release_lock(job_id: str, worker_id: str):
    r = get_redis()
    current = r.get(f"lock:{job_id}")
    if current == worker_id:
        r.delete(f"lock:{job_id}")


def cache_job(job: Job):
    """Cache full job object for fast status lookups."""
    r = get_redis()
    r.setex(f"jobdata:{job.id}", int(timedelta(hours=1).total_seconds()), job.model_dump_json())


def get_cached_job(job_id: str) -> Job | None:
    r = get_redis()
    data = r.get(f"jobdata:{job_id}")
    if data:
        return Job.model_validate_json(data)
    return None


def increment_metrics(topic: str):
    r = get_redis()
    r.incr(f"metrics:total_processed")
    r.incr(f"metrics:topic:{topic}")


def get_metrics() -> dict:
    r = get_redis()
    keys = r.keys("metrics:*")
    return {k: r.get(k) for k in keys}
