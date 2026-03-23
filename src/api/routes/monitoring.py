"""Monitoring API routes — new in v2.

Endpoints:
- GET /monitoring/dashboard    — KPI snapshot for the dashboard
- GET /monitoring/workers      — All worker node metrics
- GET /monitoring/queue        — Queue depths, metrics, and DLQ contents
- GET /monitoring/executions/stats — Aggregate execution stats (throughput, SLO)
- GET /monitoring/executions/heatmap — Hourly execution counts for the heatmap
- GET /monitoring/executions/failures — Recent failures with details
- GET /monitoring/slo          — SLO status report
- POST /monitoring/queue/dlq/purge — Purge the Dead Letter Queue
- GET /monitoring/tasks        — Task registry listing

Pain point fix: v1 had ZERO monitoring endpoints. Operators were blind.
"""

import calendar
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from src.db.repositories.execution_repo import ExecutionRepository
from src.db.repositories.job_repo import JobRepository
from src.models.enums import ExecutionStatus, JobPriority, JobStatus
from src.queue.redis_queue import RedisQueue
from src.tasks.registry import TASK_REGISTRY

logger = logging.getLogger(__name__)

router = APIRouter()


def get_job_repo(request: Request) -> JobRepository:
    return request.app.state.job_repo

def get_exec_repo(request: Request) -> ExecutionRepository:
    return request.app.state.exec_repo

def get_queue(request: Request) -> RedisQueue:
    return request.app.state.queue


# ================================================================
# DASHBOARD KPI SNAPSHOT
# ================================================================

@router.get("/dashboard", summary="KPI snapshot for the monitoring dashboard")
async def get_dashboard(
    request: Request,
    job_repo: JobRepository = Depends(get_job_repo),
    exec_repo: ExecutionRepository = Depends(get_exec_repo),
    queue: RedisQueue = Depends(get_queue),
) -> dict:
    """Return all KPI data needed to render the monitoring dashboard in one call.

    Includes: job counts, execution stats (24h), queue depths, worker summary,
    SLO values, and recent alerts.
    """
    now = datetime.utcnow()
    now_ts = int(calendar.timegm(now.timetuple()))
    start_24h = now_ts - 86400
    start_bucket_24h = (start_24h // 3600) * 3600
    end_bucket = (now_ts // 3600) * 3600

    # Execution stats (last 24h)
    try:
        stats = exec_repo.get_execution_stats(start_bucket_24h, end_bucket)
    except Exception as exc:
        logger.error("Failed to get execution stats: %s", exc)
        stats = {"total": 0, "completed": 0, "failed": 0, "success_rate": 0,
                 "avg_duration_ms": 0, "p95_duration_ms": 0}

    # Queue depth
    try:
        queue_depth = queue.get_queue_depth()
        queue_metrics = queue.get_queue_metrics()
    except Exception:
        queue_depth = {"pending": -1, "processing": -1, "dlq": 0}
        queue_metrics = {}

    # Job counts by status
    job_counts = {"ACTIVE": 0, "PAUSED": 0, "DELETED": 0, "COMPLETED": 0}
    try:
        for status_val in JobStatus:
            jobs = job_repo.get_jobs_by_status(status_val)
            job_counts[str(status_val)] = len(jobs)
    except Exception:
        pass

    # Worker summary from Redis (workers register themselves)
    worker_count = _get_worker_count(request)

    return {
        "timestamp": now.isoformat(),
        "jobs": job_counts,
        "executions_24h": stats,
        "queue": queue_depth,
        "queue_metrics": queue_metrics,
        "workers": worker_count,
        "slos": _compute_slos(stats),
    }


def _compute_slos(stats: dict) -> List[dict]:
    """Compute SLO status from execution stats."""
    return [
        {
            "name": "Job Success Rate",
            "target": 99.0,
            "actual": stats.get("success_rate", 0),
            "met": stats.get("success_rate", 0) >= 99.0,
        },
        {
            "name": "P95 Latency < 5s",
            "target": 5000,
            "actual": stats.get("p95_duration_ms", 0),
            "met": stats.get("p95_duration_ms", 0) <= 5000,
        },
        {
            "name": "Avg Latency < 2s",
            "target": 2000,
            "actual": stats.get("avg_duration_ms", 0),
            "met": stats.get("avg_duration_ms", 0) <= 2000,
        },
    ]


def _get_worker_count(request: Request) -> dict:
    """Attempt to get worker count from app state."""
    try:
        workers = getattr(request.app.state, "workers", [])
        active = sum(1 for w in workers if w.get("status") == "active")
        return {"total": len(workers), "active": active}
    except Exception:
        return {"total": 0, "active": 0}


# ================================================================
# WORKERS
# ================================================================

@router.get("/workers", summary="Worker node metrics")
async def get_workers(request: Request) -> dict:
    """Return per-worker metrics for the worker grid view.

    Enhancement: v1 had no worker visibility at all.
    Workers now register their metrics in Redis.
    """
    queue = request.app.state.queue

    # Workers publish their metrics to Redis hash: worker_metrics:{worker_id}
    try:
        import json
        redis = queue._redis
        worker_keys = redis.keys("worker_metrics:*")
        workers = []
        for key in worker_keys:
            raw = redis.hgetall(key)
            if raw:
                workers.append({k: v for k, v in raw.items()})
        return {
            "workers": workers,
            "count": len(workers),
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as exc:
        logger.error("Failed to get worker metrics: %s", exc)
        return {"workers": [], "count": 0, "error": str(exc)}


# ================================================================
# QUEUE INSPECTOR
# ================================================================

@router.get("/queue", summary="Queue depth and metrics")
async def get_queue_stats(queue: RedisQueue = Depends(get_queue)) -> dict:
    """Return queue depths and cumulative metrics for queue monitoring."""
    try:
        depth = queue.get_queue_depth()
        metrics = queue.get_queue_metrics()
        return {
            "depth": depth,
            "metrics": metrics,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/queue/dlq", summary="Dead Letter Queue contents")
async def get_dlq(
    limit: int = Query(50, ge=1, le=200),
    queue: RedisQueue = Depends(get_queue),
) -> dict:
    """Return jobs in the Dead Letter Queue.

    Enhancement: v1 had no DLQ — permanently failed jobs disappeared with no trace.
    """
    try:
        entries = queue.get_dlq_jobs(limit=limit)
        return {
            "entries": entries,
            "count": len(entries),
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/queue/dlq/purge", summary="Purge the Dead Letter Queue")
async def purge_dlq(queue: RedisQueue = Depends(get_queue)) -> dict:
    """Clear all entries from the Dead Letter Queue."""
    count = queue.purge_dlq()
    return {"purged": count, "timestamp": datetime.utcnow().isoformat()}


# ================================================================
# EXECUTION STATS
# ================================================================

@router.get("/executions/stats", summary="Aggregate execution statistics")
async def get_execution_stats(
    hours: int = Query(24, ge=1, le=168, description="Look-back window in hours"),
    exec_repo: ExecutionRepository = Depends(get_exec_repo),
) -> dict:
    """Return aggregate execution stats (total, success rate, durations) for a time window."""
    now = datetime.utcnow()
    now_ts = int(calendar.timegm(now.timetuple()))
    start_ts = now_ts - (hours * 3600)
    start_bucket = (start_ts // 3600) * 3600
    end_bucket = (now_ts // 3600) * 3600

    try:
        stats = exec_repo.get_execution_stats(start_bucket, end_bucket)
        stats["window_hours"] = hours
        stats["timestamp"] = now.isoformat()
        return stats
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/executions/heatmap", summary="Hourly execution count heatmap")
async def get_execution_heatmap(
    hours: int = Query(24, ge=1, le=168),
    exec_repo: ExecutionRepository = Depends(get_exec_repo),
) -> dict:
    """Return hourly execution counts for the last N hours.

    Used to render the activity heatmap in the dashboard.
    Enhancement: v1 had no heatmap or time-series analytics.
    """
    now = datetime.utcnow()
    now_ts = int(calendar.timegm(now.timetuple()))
    buckets = []

    for i in range(hours):
        bucket_start = ((now_ts - (hours - i) * 3600) // 3600) * 3600
        try:
            execs = exec_repo.get_executions_by_time_bucket(bucket_start)
            completed = sum(1 for e in execs if str(e.status) == "COMPLETED")
            failed = sum(1 for e in execs if str(e.status) in ("FAILED", "TIMED_OUT"))
            buckets.append({
                "hour": bucket_start,
                "hour_label": datetime.utcfromtimestamp(bucket_start).strftime("%H:00"),
                "total": len(execs),
                "completed": completed,
                "failed": failed,
            })
        except Exception:
            buckets.append({
                "hour": bucket_start,
                "hour_label": datetime.utcfromtimestamp(bucket_start).strftime("%H:00"),
                "total": 0, "completed": 0, "failed": 0,
            })

    return {"buckets": buckets, "hours": hours, "timestamp": now.isoformat()}


@router.get("/executions/failures", summary="Recent job failures")
async def get_recent_failures(
    limit: int = Query(20, ge=1, le=100),
    exec_repo: ExecutionRepository = Depends(get_exec_repo),
) -> dict:
    """Return recently failed executions for the failure analysis panel.

    Enhancement: v1 had no failure analysis endpoint.
    """
    now = datetime.utcnow()
    now_ts = int(calendar.timegm(now.timetuple()))
    start_bucket = ((now_ts - 86400) // 3600) * 3600
    end_bucket = (now_ts // 3600) * 3600

    try:
        failures = exec_repo.get_executions_in_range(
            start_bucket, end_bucket,
            [ExecutionStatus.FAILED, ExecutionStatus.TIMED_OUT],
        )
        failures.sort(key=lambda e: e.execution_time, reverse=True)
        failures = failures[:limit]

        return {
            "failures": [
                {
                    "execution_key": e.execution_key,
                    "job_id": str(e.job_id),
                    "status": str(e.status),
                    "attempt": e.attempt,
                    "error": e.error,
                    "worker_id": e.worker_id,
                    "duration_ms": e.duration_ms,
                    "execution_time": e.execution_time.isoformat() if e.execution_time else None,
                }
                for e in failures
            ],
            "count": len(failures),
            "timestamp": now.isoformat(),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ================================================================
# SLO TRACKER
# ================================================================

@router.get("/slo", summary="SLO status report")
async def get_slo_status(
    exec_repo: ExecutionRepository = Depends(get_exec_repo),
    queue: RedisQueue = Depends(get_queue),
) -> dict:
    """Return SLO compliance status for key system metrics.

    Enhancement: v1 had no SLO tracking or reporting.
    """
    now = datetime.utcnow()
    now_ts = int(calendar.timegm(now.timetuple()))
    start_1h = ((now_ts - 3600) // 3600) * 3600
    start_24h = ((now_ts - 86400) // 3600) * 3600
    end_bucket = (now_ts // 3600) * 3600

    try:
        stats_1h = exec_repo.get_execution_stats(start_1h, end_bucket)
        stats_24h = exec_repo.get_execution_stats(start_24h, end_bucket)
    except Exception:
        stats_1h = stats_24h = {}

    queue_depth = queue.get_queue_depth()

    slos = [
        {
            "name": "Job Success Rate (24h)",
            "target_pct": 99.0,
            "actual_pct": stats_24h.get("success_rate", 0),
            "met": stats_24h.get("success_rate", 0) >= 99.0,
            "window": "24h",
        },
        {
            "name": "Job Success Rate (1h)",
            "target_pct": 99.0,
            "actual_pct": stats_1h.get("success_rate", 0),
            "met": stats_1h.get("success_rate", 0) >= 99.0,
            "window": "1h",
        },
        {
            "name": "P95 Execution Latency < 5s",
            "target_ms": 5000,
            "actual_ms": stats_24h.get("p95_duration_ms", 0),
            "met": stats_24h.get("p95_duration_ms", 0) <= 5000,
            "window": "24h",
        },
        {
            "name": "Queue Pending Depth < 100",
            "target": 100,
            "actual": queue_depth.get("pending", 0),
            "met": queue_depth.get("pending", 0) < 100,
            "window": "live",
        },
        {
            "name": "DLQ Empty",
            "target": 0,
            "actual": queue_depth.get("dlq", 0),
            "met": queue_depth.get("dlq", 0) == 0,
            "window": "live",
        },
    ]

    met_count = sum(1 for s in slos if s["met"])
    return {
        "slos": slos,
        "met_count": met_count,
        "total_count": len(slos),
        "overall_healthy": met_count == len(slos),
        "timestamp": now.isoformat(),
    }


# ================================================================
# TASK REGISTRY
# ================================================================

@router.get("/tasks", summary="List registered tasks")
async def list_tasks() -> dict:
    """Return all registered task IDs.

    Enhancement: v1 only surfaced task list in error messages.
    Now it's a proper API endpoint for discoverability.
    """
    return {
        "tasks": TASK_REGISTRY.list_tasks(),
        "count": len(TASK_REGISTRY.list_tasks()),
    }
