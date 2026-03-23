# Job Scheduler v2 — Changes & Enhancement Summary

## Overview

This document details every change made to upgrade the job-scheduler from v1 to v2,
organized by component, with rationale tied directly to identified pain points and
feature gaps compared to popular schedulers (Celery Beat, Airflow, Temporal, Quartz).

---

## Pain Points in v1 (Identified from Code Analysis)

| # | Pain Point | Location | Fix in v2 |
|---|---|---|---|
| 1 | No priority queue — all jobs treated equally | `redis_queue.py` | `JobPriority` enum + priority-offset scoring |
| 2 | No INTERVAL schedule type | `enums.py`, `scheduler.py` | `ScheduleType.INTERVAL` + `schedule_interval_job()` |
| 3 | No job update (PATCH) — delete and recreate required | `routes/jobs.py` | `PATCH /jobs/{job_id}` endpoint |
| 4 | No pause/resume — status enum existed but wasn't enforced | `worker.py`, `scheduler.py` | `pause_job()`, `resume_job()`, worker enforcement |
| 5 | No bulk operations — operators called API N times | `routes/jobs.py` | `POST /jobs/bulk` endpoint |
| 6 | No failure notifications — silent failures | `worker.py` | Webhook notification on job failure |
| 7 | No Dead Letter Queue — failed jobs disappeared | `redis_queue.py` | `queue:dlq` sorted set + DLQ API endpoints |
| 8 | No per-job timeout — jobs could run forever | `worker.py` | `execution_timeout_seconds` per job, thread timeout |
| 9 | No worker-level observability | `worker.py` | `WorkerMetrics` class, Redis metric publishing |
| 10 | No monitoring API — operators were blind | `routes/` | `/api/v1/monitoring/*` router (9 endpoints) |
| 11 | No job tagging — no organizational metadata | `models/job.py` | `tags: List[str]` field + tag-based filtering |
| 12 | No job priority filtering in API | `routes/jobs.py` | `?priority=HIGH` query parameter |
| 13 | No job naming/uniqueness | `models/job.py` | `job_name` field + 409 Conflict on duplicate |
| 14 | No execution duration tracking | `models/execution.py` | `started_at`, `completed_at`, `duration_ms` |
| 15 | No worker identity in executions | `models/execution.py` | `worker_id` stored in execution records |
| 16 | No max_executions cap for CRON jobs | `models/job.py` | `max_executions` field + scheduler enforcement |
| 17 | No job dependency support | `models/job.py` | `depends_on: List[str]` + dependency checking |
| 18 | No timezone support — UTC only | `scheduler.py` | `timezone` field, croniter timezone support |
| 19 | No aggregate stats API | `routes/monitoring.py` | `/monitoring/executions/stats` endpoint |
| 20 | No heatmap data API | `routes/monitoring.py` | `/monitoring/executions/heatmap` endpoint |
| 21 | No SLO tracking | `routes/monitoring.py` | `/monitoring/slo` endpoint |
| 22 | No task registry API | `routes/monitoring.py` | `/monitoring/tasks` endpoint |
| 23 | No request tracing | `app.py` | `RequestIDMiddleware` injects `X-Request-ID` |
| 24 | No v1→v2 migration path | `scripts/init_db.py` | `_run_v1_migration()` — safe ALTER TABLE ADD |

---

## Files Modified

### `src/models/enums.py`
- **Added** `JobPriority` enum: `LOW | NORMAL | HIGH | CRITICAL`
- **Added** `NotificationChannel` enum: `WEBHOOK | EMAIL | SLACK`
- **Added** `ScheduleType.INTERVAL` (run every N seconds)
- **Added** `JobStatus.COMPLETED` (one-shot jobs that finished all runs)
- **Added** `ExecutionStatus.CANCELLED`, `SKIPPED`, `TIMED_OUT`

### `src/models/job.py`
- **Added** `priority: JobPriority = NORMAL`
- **Added** `description: Optional[str]`
- **Added** `tags: List[str]`
- **Added** `execution_timeout_seconds: int = 0`
- **Added** `notification_url: Optional[str]`
- **Added** `notification_channel: Optional[NotificationChannel]`
- **Added** `max_executions: Optional[int]`
- **Added** `execution_count: int = 0`
- **Added** `last_executed_at: Optional[datetime]`
- **Added** `depends_on: List[str]`
- **Added** `timezone: str = "UTC"`
- **Added** `job_name: Optional[str]`
- **Added** `has_reached_max_executions()` method
- **Updated** `to_db_dict()` to include all new fields

### `src/models/execution.py`
- **Added** `started_at: Optional[datetime]`
- **Added** `completed_at: Optional[datetime]`
- **Added** `duration_ms: Optional[int]`
- **Added** `worker_id: Optional[str]`
- **Added** `timeout_seconds: int = 30`
- **Added** `output_size_bytes: int = 0`
- **Added** `priority: str = "NORMAL"`
- **Added** `mark_started()`, `mark_completed()`, `mark_failed()` factory methods
- **Updated** `Execution.create()` to accept `priority` and `timeout_seconds`

### `src/queue/messages.py`
- **Added** `priority: str = "NORMAL"`
- **Added** `timeout_seconds: int = 30`
- **Added** `notification_url: str = ""`
- **Added** `PRIORITY_OFFSET` dict mapping priority → score offset
- **Added** `effective_score()` method for priority-adjusted Redis sorting

### `src/queue/redis_queue.py`
- **Updated** `enqueue()` to use `effective_score()` for priority ordering
- **Added** `enqueue_batch()` for efficient bulk enqueuing
- **Added** `send_to_dlq()` — route permanently failed jobs to `queue:dlq`
- **Added** `get_dlq_jobs()` — inspect DLQ entries
- **Added** `purge_dlq()` — clear DLQ
- **Added** `get_queue_metrics()` — cumulative counters per priority from Redis hash
- **Updated** `get_queue_depth()` to include `dlq` count

### `src/worker/worker.py`
- **Added** `WorkerMetrics` class with thread-safe counters
- **Added** `worker_id` generation from hostname + PID
- **Added** per-job timeout enforcement via thread + `join(timeout=...)`
- **Added** `TIMED_OUT` status for exceeded-timeout executions
- **Added** `_send_failure_notification()` — POST webhook on failure/timeout
- **Added** DLQ routing for exhausted jobs (`send_to_dlq()`)
- **Updated** `execute_job()` to pass `worker_id`, `started_at`, `duration_ms` to exec repo
- **Added** PAUSED job detection (skip execution, mark SKIPPED)
- **Added** `metrics` property for external access

### `src/scheduler/scheduler.py`
- **Added** `schedule_interval_job()` — INTERVAL type support
- **Updated** `poll_and_enqueue()` to check job priority, pause status, max_executions, deps
- **Added** `_dependencies_met()` — DAG dependency resolution
- **Updated** all `schedule_*` methods to pass `priority` and `timeout_seconds` to messages
- **Added** `get_scheduler_stats()` — poll count, total enqueued, uptime

### `src/db/cassandra.py`
- **Updated** `CREATE_JOBS_TABLE_CQL` — all 12 new columns
- **Updated** `CREATE_EXECUTIONS_TABLE_CQL` — 7 new columns
- **Updated** `CREATE_USER_EXECUTIONS_TABLE_CQL` — 3 new columns
- **Added** `CREATE_WORKER_STATS_TABLE_CQL` — new table
- **Added** `CREATE_SLO_METRICS_TABLE_CQL` — new table
- **Added** indexes for `status` and `priority` columns

### `src/db/repositories/job_repo.py`
- **Updated** `create_job()` — persists all 12 new fields
- **Added** `get_jobs_by_status()`, `get_jobs_by_priority()`, `get_jobs_by_tag()`
- **Added** `get_job_by_name()` — unique name lookup per user
- **Added** `pause_job()`, `resume_job()`
- **Added** `increment_execution_count()`
- **Updated** `_row_to_job()` to hydrate all new fields

### `src/db/repositories/execution_repo.py`
- **Updated** `_insert_execution()` — persists all new fields
- **Updated** `update_execution_status()` — accepts `duration_ms`, `worker_id`, `started_at`, `completed_at`
- **Added** `get_execution_stats()` — aggregate analytics (total, success_rate, avg/p95 duration)

### `src/api/schemas.py`
- **Updated** `CreateJobRequest` — all 10 new fields + INTERVAL validation
- **Added** `UpdateJobRequest` — partial update support
- **Added** `BulkJobActionRequest` / `BulkJobActionResponse`
- **Updated** `JobResponse` — includes all new fields
- **Updated** `ExecutionResponse` — includes `started_at`, `completed_at`, `duration_ms`, `worker_id`

### `src/api/routes/jobs.py`
- **Updated** `POST /jobs` — handles INTERVAL, validates job_name uniqueness
- **Updated** `GET /jobs` — adds `?priority`, `?tag`, `?task_id` filters
- **Added** `PATCH /jobs/{job_id}` — partial update
- **Added** `POST /jobs/{job_id}/pause`
- **Added** `POST /jobs/{job_id}/resume`
- **Added** `DELETE /jobs/{job_id}`
- **Added** `POST /jobs/bulk` — bulk pause/resume/delete
- **Updated** `/health` — includes queue metrics and scheduler stats

### `src/api/app.py`
- **Added** `monitoring_router` at `/api/v1/monitoring`
- **Added** `RequestIDMiddleware` for distributed tracing

### `src/core/config.py`
- **Added** `SCHEDULER_TIMEZONE`, `WORKER_REGISTER_INTERVAL`
- **Added** `DEFAULT_EXECUTION_TIMEOUT_SECONDS`, `NOTIFICATION_TIMEOUT_SECONDS`
- **Added** `MONITORING_ENABLED`, `DLQ_RETENTION_DAYS`

---

## Files Added

| File | Purpose |
|---|---|
| `src/api/routes/monitoring.py` | 9 new monitoring endpoints |
| `src/tasks/builtin/db_cleanup_task.py` | Built-in DB cleanup task |
| `src/tasks/builtin/data_export_task.py` | Built-in data export task |
| `tests/unit/test_v2_enhancements.py` | Tests for all v2 features |
| `tests/unit/test_schemas_v2.py` | Tests for v2 schema validation |

---

## New API Endpoints (v2)

| Method | Path | Description |
|---|---|---|
| PATCH | `/api/v1/jobs/{job_id}` | Update job metadata |
| POST | `/api/v1/jobs/{job_id}/pause` | Pause a job |
| POST | `/api/v1/jobs/{job_id}/resume` | Resume a paused job |
| DELETE | `/api/v1/jobs/{job_id}` | Soft-delete a job |
| POST | `/api/v1/jobs/bulk` | Bulk pause/resume/delete |
| GET | `/api/v1/monitoring/dashboard` | KPI snapshot |
| GET | `/api/v1/monitoring/workers` | Worker metrics |
| GET | `/api/v1/monitoring/queue` | Queue stats |
| GET | `/api/v1/monitoring/queue/dlq` | DLQ inspection |
| POST | `/api/v1/monitoring/queue/dlq/purge` | Purge DLQ |
| GET | `/api/v1/monitoring/executions/stats` | Aggregate stats |
| GET | `/api/v1/monitoring/executions/heatmap` | Hourly heatmap data |
| GET | `/api/v1/monitoring/executions/failures` | Recent failures |
| GET | `/api/v1/monitoring/slo` | SLO compliance report |
| GET | `/api/v1/monitoring/tasks` | Task registry list |

---

## Backward Compatibility

All v2 changes are **backward compatible** with existing v1 deployments:

1. **Database**: `scripts/init_db.py` runs `ALTER TABLE ADD IF NOT EXISTS` for each new column —
   existing rows get `NULL` for new columns, which the v2 code handles gracefully with defaults.

2. **API**: All v1 request/response shapes still work. New fields are optional with sensible defaults.

3. **Queue**: The `JobMessage` format adds optional fields — v1 messages missing these fields
   fall back to defaults (`priority="NORMAL"`, `timeout_seconds=30`).

4. **Worker**: v2 worker is fully compatible with v1 queue messages.

---

## How to Deploy v2

```bash
# 1. Pull the v2 changes
git pull

# 2. Install new dependency
pip install -r requirements.txt  # adds pytz

# 3. Run the migration (safe, idempotent — adds columns only)
docker-compose exec api python scripts/init_db.py

# 4. Rebuild and restart
docker-compose build
docker-compose up -d

# 5. Verify
curl http://localhost:8000/api/v1/health
curl http://localhost:8000/api/v1/monitoring/dashboard
```
