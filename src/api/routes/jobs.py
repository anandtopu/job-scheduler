"""Job management API routes."""

import logging
from datetime import datetime
from typing import List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from src.api.schemas import (
    CreateJobRequest,
    ErrorResponse,
    ExecutionListResponse,
    ExecutionResponse,
    JobDetailResponse,
    JobListResponse,
    JobResponse,
    decode_page_token,
    encode_page_token,
)
from src.db.repositories.execution_repo import ExecutionRepository
from src.db.repositories.job_repo import JobRepository
from src.models.enums import ExecutionStatus, JobStatus, ScheduleType
from src.models.job import Job
from src.scheduler.scheduler import Scheduler
from src.tasks.registry import TASK_REGISTRY

logger = logging.getLogger(__name__)

router = APIRouter()


def get_job_repo(request: Request) -> JobRepository:
    """Dependency: get JobRepository from app state."""
    return request.app.state.job_repo


def get_exec_repo(request: Request) -> ExecutionRepository:
    """Dependency: get ExecutionRepository from app state."""
    return request.app.state.exec_repo


def get_scheduler(request: Request) -> Scheduler:
    """Dependency: get Scheduler from app state."""
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
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


@router.post(
    "/jobs",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new job",
    responses={
        201: {"description": "Job created successfully"},
        400: {"model": ErrorResponse, "description": "Invalid request"},
        422: {"description": "Validation error"},
    },
)
async def create_job(
    request_body: CreateJobRequest,
    request: Request,
    job_repo: JobRepository = Depends(get_job_repo),
    scheduler: Scheduler = Depends(get_scheduler),
) -> JobResponse:
    """Create a new scheduled job.

    - **IMMEDIATE**: Executes as soon as possible.
    - **DATETIME**: Executes at the specified ISO 8601 datetime.
    - **CRON**: Executes on a recurring schedule defined by a cron expression.

    The `user_id` is extracted from the `X-User-Id` request header.
    """
    user_id = request.headers.get("X-User-Id", "anonymous")

    # Validate that the task_id is registered
    if request_body.task_id not in TASK_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Task '{request_body.task_id}' is not registered. "
                   f"Available tasks: {TASK_REGISTRY.list_tasks()}",
        )

    job = Job(
        job_id=uuid4(),
        user_id=user_id,
        task_id=request_body.task_id,
        schedule_type=request_body.schedule_type,
        schedule_expression=request_body.schedule_expression,
        parameters=request_body.parameters,
        status=JobStatus.ACTIVE,
    )

    # Persist job
    job_repo.create_job(job)

    # Schedule based on type
    try:
        schedule_type = request_body.schedule_type
        if schedule_type == ScheduleType.IMMEDIATE:
            scheduler.schedule_immediate_job(job)
        elif schedule_type == ScheduleType.DATETIME:
            scheduler.schedule_datetime_job(job)
        elif schedule_type == ScheduleType.CRON:
            scheduler.schedule_cron_job(job)
    except Exception as exc:
        logger.error("Failed to schedule job %s: %s", job.job_id, exc)
        # Cleanup: mark job as deleted if scheduling failed
        try:
            job_repo.update_job_status(job.job_id, JobStatus.DELETED)
        except Exception:
            pass
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to schedule job: {exc}",
        )

    logger.info("Created and scheduled job %s (%s).", job.job_id, schedule_type)
    return job_to_response(job)


@router.get(
    "/jobs",
    response_model=JobListResponse,
    summary="List jobs for a user",
    responses={
        200: {"description": "List of jobs"},
        400: {"model": ErrorResponse, "description": "Missing user_id"},
    },
)
async def list_jobs(
    user_id: Optional[str] = Query(None, description="Filter by user ID. Required."),
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by job status."),
    limit: int = Query(20, ge=1, le=100, description="Maximum results to return."),
    page: int = Query(1, ge=1, description="Page number."),
    job_repo: JobRepository = Depends(get_job_repo),
) -> JobListResponse:
    """List all jobs for a user with optional status filter."""
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Query parameter 'user_id' is required.",
        )

    jobs = job_repo.get_jobs_by_user(user_id)

    # Apply status filter
    if status_filter:
        try:
            status_enum = JobStatus(status_filter.upper())
            jobs = [j for j in jobs if str(j.status) == str(status_enum)]
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status filter '{status_filter}'. "
                       f"Valid values: {[s.value for s in JobStatus]}",
            )

    total = len(jobs)

    # Apply pagination
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    paginated_jobs = jobs[start_idx:end_idx]

    return JobListResponse(
        jobs=[job_to_response(j) for j in paginated_jobs],
        total=total,
        page=page,
        limit=limit,
    )


@router.get(
    "/jobs/{job_id}",
    response_model=JobDetailResponse,
    summary="Get job details with recent executions",
    responses={
        200: {"description": "Job details"},
        404: {"model": ErrorResponse, "description": "Job not found"},
    },
)
async def get_job(
    job_id: UUID,
    job_repo: JobRepository = Depends(get_job_repo),
    exec_repo: ExecutionRepository = Depends(get_exec_repo),
) -> JobDetailResponse:
    """Get a specific job by ID, including its 10 most recent executions."""
    job = job_repo.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )

    # Fetch recent executions for this job
    executions, _ = exec_repo.get_user_executions(
        user_id=job.user_id,
        limit=10,
    )
    # Filter to only executions for this specific job
    job_executions = [e for e in executions if str(e.job_id) == str(job_id)]

    return JobDetailResponse(
        job=job_to_response(job),
        recent_executions=[
            ExecutionResponse(
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
            )
            for e in job_executions
        ],
    )


@router.get(
    "/jobs/{job_id}/executions",
    response_model=ExecutionListResponse,
    summary="Get executions for a specific job",
    responses={
        200: {"description": "List of executions"},
        404: {"model": ErrorResponse, "description": "Job not found"},
    },
)
async def get_job_executions(
    job_id: UUID,
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status."),
    start_time: Optional[datetime] = Query(None, description="Filter: start time (ISO)."),
    end_time: Optional[datetime] = Query(None, description="Filter: end time (ISO)."),
    limit: int = Query(20, ge=1, le=100),
    page_token: Optional[str] = Query(None, description="Pagination token."),
    job_repo: JobRepository = Depends(get_job_repo),
    exec_repo: ExecutionRepository = Depends(get_exec_repo),
) -> ExecutionListResponse:
    """Get all executions for a specific job with pagination."""
    job = job_repo.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found.",
        )

    status_enum = None
    if status_filter:
        try:
            status_enum = ExecutionStatus(status_filter.upper())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status filter '{status_filter}'.",
            )

    page_state = None
    if page_token:
        try:
            page_state = decode_page_token(page_token)
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid page_token.",
            )

    executions, next_page_state = exec_repo.get_user_executions(
        user_id=job.user_id,
        status=status_enum,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        page_state=page_state,
    )

    # Filter to this job only
    executions = [e for e in executions if str(e.job_id) == str(job_id)]

    next_token = None
    if next_page_state:
        next_token = encode_page_token(next_page_state)

    return ExecutionListResponse(
        executions=[
            ExecutionResponse(
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
            )
            for e in executions
        ],
        total=len(executions),
        limit=limit,
        next_page_token=next_token,
    )


@router.get(
    "/health",
    summary="Health check",
    responses={200: {"description": "Service healthy"}},
)
async def health_check(request: Request) -> dict:
    """Health check endpoint.

    Returns the health status of the API, Cassandra, and Redis connections.
    """
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {},
    }

    # Check Cassandra
    try:
        cassandra = request.app.state.cassandra_client
        cassandra.get_session().execute("SELECT now() FROM system.local")
        health["services"]["cassandra"] = "healthy"
    except Exception as exc:
        health["services"]["cassandra"] = f"unhealthy: {exc}"
        health["status"] = "degraded"

    # Check Redis
    try:
        queue = request.app.state.queue
        queue.ping()
        queue_depth = queue.get_queue_depth()
        health["services"]["redis"] = "healthy"
        health["queue_depth"] = queue_depth
    except Exception as exc:
        health["services"]["redis"] = f"unhealthy: {exc}"
        health["status"] = "degraded"

    return health
