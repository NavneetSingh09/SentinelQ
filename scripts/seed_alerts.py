#!/usr/bin/env python3
"""
Quick local test — submit a batch of alerts and watch them process.
Run this after starting docker-compose (or local infra).

Usage:
    python scripts/seed_alerts.py
"""
import httpx
import time
import random

BASE_URL = "http://localhost:8000"

SAMPLE_ALERTS = [
    {"alert_name": "CPU usage > 95% on prod-api-1", "severity": "critical", "source": "prometheus",
     "payload": {"host": "prod-api-1", "value": 97.3}},

    {"alert_name": "Memory leak detected in order-service", "severity": "critical", "source": "datadog",
     "payload": {"service": "order-service", "memory_mb": 4096}},

    {"alert_name": "Database connection pool exhausted", "severity": "warning", "source": "prometheus",
     "payload": {"db": "postgres-primary", "pool_size": 100, "active": 100}},

    {"alert_name": "Latency P99 > 2s on /api/checkout", "severity": "warning", "source": "grafana",
     "payload": {"endpoint": "/api/checkout", "p99_ms": 2340}},

    {"alert_name": "SSL certificate expires in 7 days", "severity": "warning", "source": "certbot",
     "payload": {"domain": "api.sentinelq.io", "expires_in_days": 7}},

    {"alert_name": "Disk usage > 80% on log server", "severity": "info", "source": "node-exporter",
     "payload": {"host": "log-server-1", "usage_pct": 82}},

    {"alert_name": "Scheduled backup completed", "severity": "info", "source": "backup-service",
     "payload": {"database": "sentinelq", "size_gb": 12.4}},
]


def main():
    print(f"Submitting {len(SAMPLE_ALERTS)} alerts to SentinelQ...\n")
    job_ids = []

    for alert in SAMPLE_ALERTS:
        resp = httpx.post(f"{BASE_URL}/alerts", json=alert)
        data = resp.json()
        print(f"[{alert['severity'].upper():8}] {alert['alert_name'][:50]}")
        print(f"           -> job_id={data['job_id']} topic={data['topic']}\n")
        job_ids.append(data["job_id"])
        time.sleep(0.2)

    print("\nWaiting 5s for workers to process...\n")
    time.sleep(5)

    print("=== Job Status ===")
    for job_id in job_ids:
        resp = httpx.get(f"{BASE_URL}/alerts/{job_id}")
        data = resp.json()
        status = data.get("status", "unknown")
        worker = data.get("worker_id", "-")
        print(f"{job_id[:8]}... status={status:10} worker={worker}")

    print("\n=== Stats ===")
    stats = httpx.get(f"{BASE_URL}/stats").json()
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
