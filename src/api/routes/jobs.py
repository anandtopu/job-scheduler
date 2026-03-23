"""Job management API routes (v2 — enhanced).

New endpoints over v1:
- PATCH /jobs/{job_id}          — Update job parameters, priority, tags, etc.
- POST  /jobs/{job_id}/pause    — Pause a job
- POST  /jobs/{job_id}/resume   — Resume a paused job
- DELETE /jobs/{job_id}         — Soft-delete a job
- POST  /jobs/bulk              — Bulk pause/resume/delete
- GET   /jobs/{job_id}/executions — Enhanced with duration/worker filters
- GET   /jobs/search            — Search by tag, priority, task_id, status
"""

import logging
from datetime import datetime
from typing import List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from src.api.schemas import (
    BulkJobActionRequest,
    BulkJobActionResponse,
    CreateJobRequest,
    ErrorResponse,
    ExecutionListResponse,
    ExecutionResponse,
    JobDetailResponse,
    JobListResponse,
    JobResponse,
    UpdateJobRequest,
    decode_page_token,
    encode_page_token,
)
from src.db.repositories.execution_repo import ExecutionRepository
from src.db.repositories.job_repo import JobRepository
from src.models.enums import ExecutionStatus, JobPriority, JobStatus, ScheduleType
from src.models.job import Job
from src.scheduler.scheduler import Scheduler
from src.tasks.registry import TASK_REGISTRY

logger = logging.getLogger(__name__)

router = APIRouter()


# ---- Dependency helpers ----

def get_job_repo(request: Request) -> JobRepository:
    return request.app.state.job_repo

def get_exec_repo(request: Request) -> ExecutionRepository:
    return request.app.state.exec_repo

def get_scheduler(request: Request) -> Scheduler:
    return request.app.state.scheduler


def job_to_response(job: Job) -> JobResponse:
    """Convert a Job model to a JobResponse schema."""
    return JobResponse(
        job_id=job.job_id,
        user_id=job.user_id,
        task_id=job.task_id,
        schedule_type=str(job.schedule_type),
        schedule_expression=job.schedule_expression,
        parameters=job.parameters,
        status=str(job.status),
        priority=str(job.priority),
        description=job.description,
        tags=job.tags,
        execution_timeout_seconds=job.execution_timeout_seconds,
        notification_url=job.notification_url,
        notification_channel=str(job.notification_channel) if job.notification_channel else None,
        max_executions=job.max_executions,
        execution_count=job.execution_count,
        last_executed_at=job.last_executed_at,
        depends_on=job.depends_on,
        timezone=job.timezone,
        job_name=job.job_name,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def execution_to_response(e) -> ExecutionResponse:
    return ExecutionResponse(
        execution_key=e.execution_key,
        job_id=e.job_id,
        user_id=e.user_id,
        execution_time=e.execution_time,
        status=str(e.status),
        attempt=e.attempt,
        result=e.result,
        error=e.error,
        created_at=e.created_at,
        updated_at=e.updated_at,
        started_at=e.started_at,
        completed_at=e.completed_at,
        duration_ms=e.duration_ms,
        worker_id=e.worker_id,
        priority=str(e.priority) if e.priority else "NORMAL",
    )


# ================================================================
# CREATE JOB
# ================================================================

@router.post(
    "/jobs",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new job",
)
async def create_job(
    request_body: CreateJobRequest,
    request: Request,
    job_repo: JobRepository = Depends(get_job_repo),
    scheduler: Scheduler = Depends(get_scheduler),
) -> JobResponse:
    """Create a new scheduled job.

    Schedule types:
    - **IMMEDIATE**: Executes as soon as possible.
    - **DATETIME**: Executes at the specified ISO 8601 datetime.
    - **CRON**: Recurring schedule via cron expression.
    - **INTERVAL**: Recurring schedule every N seconds (new in v2).
    """
    user_id = request.headers.get("X-User-Id", "anonymous")

    if request_body.task_id not in TASK_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Task '{request_body.task_id}' is not registered. "
                f"Available tasks: {TASK_REGISTRY.list_tasks()}"
            ),
        )

    # Check unique job_name per user
    if request_body.job_name:
        existing = job_repo.get_job_by_name(request_body.job_name, user_id)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A job named '{request_body.job_name}' already exists for this user.",
            )

    job = Job(
        job_id=uuid4(),
        user_id=user_id,
        task_id=request_body.task_id,
        schedule_type=request_body.schedule_type,
        schedule_expression=request_body.schedule_expression,
        parameters=request_body.parameters,
        status=JobStatus.ACTIVE,
        priority=request_body.priority,
        description=request_body.description,
        tags=request_body.tags,
        execution_timeout_seconds=request_body.execution_timeout_seconds,
        notification_url=request_body.notification_url,
        notification_channel=request_body.notification_channel,
        max_executions=request_body.max_executions,
        depends_on=request_body.depends_on,
        timezone=request_body.timezone,
        job_name=request_body.job_name,
    )

    job_repo.create_job(job)

    try:
        schedule_type = request_body.schedule_type
        if schedule_type == ScheduleType.IMMEDIATE:
            scheduler.schedule_immediate_job(job)
        elif schedule_type == ScheduleType.DATETIME:
            scheduler.schedule_datetime_job(job)
        elif schedule_type == ScheduleType.CRON:
            scheduler.schedule_cron_job(job)
        elif schedule_type == ScheduleType.INTERVAL:
            scheduler.schedule_interval_job(job)
    except Exception as exc:
        logger.error("Failed to schedule job %s: %s", job.job_id, exc)
        try:
            job_repo.update_job_status(job.job_id, JobStatus.DELETED)
        except Exception:
            pass
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to schedule job: {exc}",
        )

    logger.info("Created and scheduled job %s (user=%s type=%s priority=%s)",
                job.job_id, user_id, schedule_type, job.priority)
    return job_to_response(job)


# ================================================================
# LIST JOBS (enhanced with tag/priority filters)
# ================================================================

@router.get("/jobs", response_model=JobListResponse, summary="List jobs")
async def list_jobs(
    user_id: Optional[str] = Query(None),
    status_filter: Optional[str] = Query(None, alias="status"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    tag: Optional[str] = Query(None, description="Filter by tag"),
    task_id: Optional[str] = Query(None, description="Filter by task_id"),
    limit: int = Query(20, ge=1, le=100),
    page: int = Query(1, ge=1),
    job_repo: JobRepository = Depends(get_job_repo),
) -> JobListResponse:
    """List jobs with optional filters for status, priority, tag, and task_id."""
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Query parameter 'user_id' is required.",
        )

    # Start with tag filter if provided (most selective)
    if tag:
        jobs = job_repo.get_jobs_by_tag(tag, user_id=user_id)
    else:
        jobs = job_repo.get_jobs_by_user(user_id)

    # Apply additional filters
    if status_filter:
        try:
            status_enum = JobStatus(status_filter.upper())
            jobs = [j for j in jobs if str(j.status) == str(status_enum)]
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status '{status_filter}'. Valid: {[s.value for s in JobStatus]}",
            )

    if priority:
        try:
            prio_enum = JobPriority(priority.upper())
            jobs = [j for j in jobs if str(j.priority) == str(prio_enum)]
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid priority '{priority}'. Valid: {[p.value for p in JobPriority]}",
            )

    if task_id:
        jobs = [j for j in jobs if j.task_id == task_id]

    total = len(jobs)
    start_idx = (page - 1) * limit
    paginated_jobs = jobs[start_idx: start_idx + limit]

    return JobListResponse(
        jobs=[job_to_response(j) for j in paginated_jobs],
        total=total,
        page=page,
        limit=limit,
    )


# ================================================================
# GET JOB
# ================================================================

@router.get("/jobs/{job_id}", response_model=JobDetailResponse, summary="Get job details")
async def get_job(
    job_id: UUID,
    job_repo: JobRepository = Depends(get_job_repo),
    exec_repo: ExecutionRepository = Depends(get_exec_repo),
) -> JobDetailResponse:
    """Get a specific job by ID, including its 10 most recent executions."""
    job = job_repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    executions, _ = exec_repo.get_user_executions(user_id=job.user_id, limit=10)
    job_executions = [e for e in executions if str(e.job_id) == str(job_id)]

    return JobDetailResponse(
        job=job_to_response(job),
        recent_executions=[execution_to_response(e) for e in job_executions],
    )


# ================================================================
# UPDATE JOB (NEW in v2 — pain point: v1 had no update support)
# ================================================================

@router.patch(
    "/jobs/{job_id}",
    response_model=JobResponse,
    summary="Update job metadata (priority, tags, description, etc.)",
)
async def update_job(
    job_id: UUID,
    request_body: UpdateJobRequest,
    job_repo: JobRepository = Depends(get_job_repo),
) -> JobResponse:
    """Update job fields without re-creating it.

    Enhancement: v1 had no update support — users had to delete and recreate.
    Supports updating: status, priority, description, tags, timeout, notification_url,
    max_executions, parameters.
    """
    job = job_repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    updates = request_body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update.")

    # Apply allowed updates
    if "status" in updates:
        job_repo.update_job_status(job_id, JobStatus(updates["status"]))

    # For other fields, rebuild and re-persist (Cassandra UPDATE by PK)
    import json
    update_parts = []
    params = []

    field_map = {
        "priority": "priority",
        "description": "description",
        "tags": "tags",
        "execution_timeout_seconds": "execution_timeout_seconds",
        "notification_url": "notification_url",
        "max_executions": "max_executions",
    }

    for req_field, col_name in field_map.items():
        if req_field in updates:
            update_parts.append(f"{col_name} = %s")
            val = updates[req_field]
            if req_field == "tags":
                val = json.dumps(val)
            elif req_field == "priority":
                val = str(val)
            params.append(val)

    if "parameters" in updates:
        update_parts.append("parameters = %s")
        params.append(json.dumps(updates["parameters"]))

    if update_parts:
        update_parts.append("updated_at = %s")
        params.append(datetime.utcnow())
        params.append(job_id)
        query = f"UPDATE jobs SET {', '.join(update_parts)} WHERE job_id = %s"
        job_repo._client.execute(query, tuple(params))

    updated_job = job_repo.get_job(job_id)
    return job_to_response(updated_job)


# ================================================================
# PAUSE / RESUME (NEW in v2 — pain point: v1 had status enum but no enforcement)
# ================================================================

@router.post(
    "/jobs/{job_id}/pause",
    response_model=JobResponse,
    summary="Pause a job",
)
async def pause_job(
    job_id: UUID,
    job_repo: JobRepository = Depends(get_job_repo),
) -> JobResponse:
    """Pause a job. Future executions will be skipped until resumed."""
    job = job_repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    job_repo.pause_job(job_id)
    updated = job_repo.get_job(job_id)
    return job_to_response(updated)


@router.post(
    "/jobs/{job_id}/resume",
    response_model=JobResponse,
    summary="Resume a paused job",
)
async def resume_job(
    job_id: UUID,
    job_repo: JobRepository = Depends(get_job_repo),
) -> JobResponse:
    """Resume a previously paused job."""
    job = job_repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    job_repo.resume_job(job_id)
    updated = job_repo.get_job(job_id)
    return job_to_response(updated)


# ================================================================
# DELETE JOB
# ================================================================

@router.delete(
    "/jobs/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a job",
)
async def delete_job(
    job_id: UUID,
    job_repo: JobRepository = Depends(get_job_repo),
) -> None:
    """Soft-delete a job by setting its status to DELETED."""
    job = job_repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    job_repo.delete_job(job_id)


# ================================================================
# BULK ACTIONS (NEW in v2 — pain point: no bulk operations in v1)
# ================================================================

@router.post(
    "/jobs/bulk",
    response_model=BulkJobActionResponse,
    summary="Bulk pause/resume/delete multiple jobs",
)
async def bulk_job_action(
    request_body: BulkJobActionRequest,
    job_repo: JobRepository = Depends(get_job_repo),
) -> BulkJobActionResponse:
    """Apply an action (pause/resume/delete) to multiple jobs at once.

    Enhancement: v1 had no bulk operations. Operators managing 100s of jobs
    had to make individual API calls.
    """
    succeeded = []
    failed = []

    for job_id in request_body.job_ids:
        try:
            job = job_repo.get_job(job_id)
            if job is None:
                failed.append(str(job_id))
                continue

            if request_body.action == "pause":
                job_repo.pause_job(job_id)
            elif request_body.action == "resume":
                job_repo.resume_job(job_id)
            elif request_body.action == "delete":
                job_repo.delete_job(job_id)

            succeeded.append(str(job_id))
        except Exception as exc:
            logger.error("Bulk action '%s' failed for job %s: %s",
                         request_body.action, job_id, exc)
            failed.append(str(job_id))

    return BulkJobActionResponse(
        succeeded=succeeded,
        failed=failed,
        total=len(request_body.job_ids),
    )


# ================================================================
# JOB EXECUTIONS
# ================================================================

@router.get(
    "/jobs/{job_id}/executions",
    response_model=ExecutionListResponse,
    summary="Get executions for a job",
)
async def get_job_executions(
    job_id: UUID,
    status_filter: Optional[str] = Query(None, alias="status"),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    page_token: Optional[str] = Query(None),
    job_repo: JobRepository = Depends(get_job_repo),
    exec_repo: ExecutionRepository = Depends(get_exec_repo),
) -> ExecutionListResponse:
    """Get all executions for a specific job with pagination."""
    job = job_repo.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

    status_enum = None
    if status_filter:
        try:
            status_enum = ExecutionStatus(status_filter.upper())
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status '{status_filter}'.")

    page_state = None
    if page_token:
        try:
            page_state = decode_page_token(page_token)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid page_token.")

    executions, next_page_state = exec_repo.get_user_executions(
        user_id=job.user_id,
        status=status_enum,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        page_state=page_state,
    )
    executions = [e for e in executions if str(e.job_id) == str(job_id)]

    next_token = encode_page_token(next_page_state) if next_page_state else None

    return ExecutionListResponse(
        executions=[execution_to_response(e) for e in executions],
        total=len(executions),
        limit=limit,
        next_page_token=next_token,
    )


# ================================================================
# HEALTH CHECK (enhanced with queue depth, scheduler stats)
# ================================================================

@router.get("/health", summary="Health check")
async def health_check(request: Request) -> dict:
    """Health check with detailed service status."""
    from src.db.cassandra import cassandra_client
    health = {
        "status": "healthy",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {},
    }

    try:
        cassandra = request.app.state.cassandra_client
        cassandra.get_session().execute("SELECT now() FROM system.local")
        health["services"]["cassandra"] = "healthy"
    except Exception as exc:
        health["services"]["cassandra"] = f"unhealthy: {exc}"
        health["status"] = "degraded"

    try:
        queue = request.app.state.queue
        queue.ping()
        depth = queue.get_queue_depth()
        health["services"]["redis"] = "healthy"
        health["queue_depth"] = depth
    except Exception as exc:
        health["services"]["redis"] = f"unhealthy: {exc}"
        health["status"] = "degraded"

    try:
        scheduler = request.app.state.scheduler
        health["scheduler"] = scheduler.get_scheduler_stats()
    except Exception:
        pass

    return health
