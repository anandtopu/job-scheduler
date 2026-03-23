"""Pydantic v2 request and response schemas for the API.

Enhancements over v1:
- CreateJobRequest: priority, description, tags, execution_timeout_seconds,
                    notification_url, notification_channel, max_executions,
                    depends_on, timezone, job_name
- INTERVAL schedule_type support with numeric expression validation
- JobResponse includes all v2 fields
- ExecutionResponse includes duration_ms, worker_id, started_at, completed_at
- New: UpdateJobRequest for partial updates (pause/resume/edit)
- New: BulkJobActionRequest for bulk pause/resume/delete
"""

import base64
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from croniter import CroniterBadCronError, croniter
from pydantic import BaseModel, Field, field_validator, model_validator

from src.models.enums import (
    ExecutionStatus, JobPriority, JobStatus,
    NotificationChannel, ScheduleType,
)


class CreateJobRequest(BaseModel):
    """Request schema for creating a new job (v2 — enhanced)."""

    task_id: str = Field(..., min_length=1, max_length=256)
    schedule_type: ScheduleType = Field(...)
    schedule_expression: Optional[str] = Field(None)
    parameters: Dict[str, Any] = Field(default_factory=dict)

    # ---- NEW FIELDS ----
    priority: JobPriority = Field(JobPriority.NORMAL, description="Execution priority")
    description: Optional[str] = Field(None, max_length=1024)
    tags: List[str] = Field(default_factory=list, max_length=20)
    execution_timeout_seconds: int = Field(30, ge=1, le=86400, description="Timeout in seconds")
    notification_url: Optional[str] = Field(None, max_length=2048, description="Webhook on failure")
    notification_channel: Optional[NotificationChannel] = None
    max_executions: Optional[int] = Field(None, ge=1, description="Max runs before auto-disable")
    depends_on: List[str] = Field(default_factory=list, description="Job IDs that must complete first")
    timezone: str = Field("UTC", description="Timezone for CRON expressions")
    job_name: Optional[str] = Field(None, max_length=256, description="Unique name per user")

    @field_validator("parameters")
    @classmethod
    def validate_parameters_size(cls, v):
        import json
        serialized = json.dumps(v)
        if len(serialized) > 65536:
            raise ValueError("Parameters must be less than 64KB.")
        return v

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v):
        if len(v) > 20:
            raise ValueError("Maximum 20 tags allowed.")
        for tag in v:
            if len(tag) > 64:
                raise ValueError(f"Tag '{tag}' exceeds 64 character limit.")
        return v

    @model_validator(mode="after")
    def validate_schedule_expression(self) -> "CreateJobRequest":
        """Validate schedule_expression based on schedule_type."""
        expr = self.schedule_expression
        validators = {
            ScheduleType.IMMEDIATE: self._validate_immediate_expression,
            ScheduleType.DATETIME: self._validate_datetime_expression,
            ScheduleType.CRON: self._validate_cron_expression,
            ScheduleType.INTERVAL: self._validate_interval_expression,
        }
        validator = validators.get(self.schedule_type)
        if validator is not None:
            validator(expr)

        return self

    def _validate_immediate_expression(self, expr: Optional[str]) -> None:
        if expr is not None:
            raise ValueError("schedule_expression must be None for IMMEDIATE schedule type.")

    def _validate_datetime_expression(self, expr: Optional[str]) -> None:
        if not expr:
            raise ValueError("schedule_expression is required for DATETIME schedule type.")
        try:
            scheduled_dt = datetime.fromisoformat(expr.rstrip("Z"))
        except ValueError as exc:
            raise ValueError(f"schedule_expression must be a valid ISO datetime: {exc}") from exc

        now = datetime.utcnow()
        if (now - scheduled_dt).total_seconds() > 60:
            raise ValueError(f"schedule_expression must be a future datetime. Got: {expr}")

    def _validate_cron_expression(self, expr: Optional[str]) -> None:
        if not expr:
            raise ValueError("schedule_expression is required for CRON schedule type.")
        try:
            cron = croniter(expr, datetime.utcnow())
            next_run = cron.get_next(datetime)
            if next_run is None:
                raise ValueError("CRON expression has no future occurrences.")
        except (CroniterBadCronError, KeyError) as exc:
            raise ValueError(f"Invalid CRON expression '{expr}': {exc}") from exc

    def _validate_interval_expression(self, expr: Optional[str]) -> None:
        # INTERVAL: schedule_expression is a positive integer (seconds)
        if not expr:
            raise ValueError("schedule_expression (seconds) is required for INTERVAL schedule type.")
        try:
            interval = int(expr)
            if interval <= 0:
                raise ValueError("INTERVAL must be a positive number of seconds.")
        except ValueError as exc:
            raise ValueError(
                f"INTERVAL schedule_expression must be a positive integer (seconds). Got: '{expr}'"
            ) from exc



class UpdateJobRequest(BaseModel):
    """Request schema for updating an existing job.

    Enhancement: v1 had no PATCH/update support at all.
    Pain point: users had to delete and re-create jobs to change any parameter.
    """

    status: Optional[JobStatus] = None
    priority: Optional[JobPriority] = None
    description: Optional[str] = Field(None, max_length=1024)
    tags: Optional[List[str]] = None
    execution_timeout_seconds: Optional[int] = Field(None, ge=1, le=86400)
    notification_url: Optional[str] = Field(None, max_length=2048)
    max_executions: Optional[int] = Field(None, ge=1)
    parameters: Optional[Dict[str, Any]] = None


class BulkJobActionRequest(BaseModel):
    """Bulk pause/resume/delete multiple jobs.

    Enhancement: v1 had no bulk operations.
    Pain point: operators had to call the API N times to pause N jobs.
    """

    job_ids: List[UUID] = Field(..., min_length=1, max_length=100)
    action: str = Field(..., pattern="^(pause|resume|delete)$")


class JobResponse(BaseModel):
    """Response schema for a job (v2 — includes all new fields)."""

    job_id: UUID
    user_id: str
    task_id: str
    schedule_type: str
    schedule_expression: Optional[str]
    parameters: Dict[str, Any]
    status: str

    # v2 fields
    priority: str = "NORMAL"
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    execution_timeout_seconds: int = 30
    notification_url: Optional[str] = None
    notification_channel: Optional[str] = None
    max_executions: Optional[int] = None
    execution_count: int = 0
    last_executed_at: Optional[datetime] = None
    depends_on: List[str] = Field(default_factory=list)
    timezone: str = "UTC"
    job_name: Optional[str] = None

    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ExecutionResponse(BaseModel):
    """Response schema for a job execution (v2 — includes timing and worker info)."""

    execution_key: str
    job_id: UUID
    user_id: str
    execution_time: datetime
    status: str
    attempt: int
    result: Optional[str]
    error: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    # v2 fields
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    worker_id: Optional[str] = None
    priority: str = "NORMAL"

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    """Paginated list of jobs."""

    jobs: List[JobResponse]
    total: int
    page: int = 1
    limit: int = 20
    next_page_token: Optional[str] = None


class ExecutionListResponse(BaseModel):
    """Paginated list of executions with cursor-based pagination."""

    executions: List[ExecutionResponse]
    total: int
    limit: int = 20
    next_page_token: Optional[str] = None


class JobDetailResponse(BaseModel):
    """Detailed job response including recent executions."""

    job: JobResponse
    recent_executions: List[ExecutionResponse]


class ErrorResponse(BaseModel):
    """Standard error response."""

    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class BulkJobActionResponse(BaseModel):
    """Response for bulk job actions."""

    succeeded: List[str] = Field(default_factory=list)
    failed: List[str] = Field(default_factory=list)
    total: int = 0


def encode_page_token(page_state: bytes) -> str:
    """Encode Cassandra paging state to a URL-safe string."""
    return base64.urlsafe_b64encode(page_state).decode("ascii")


def decode_page_token(token: str) -> bytes:
    """Decode a page token back to Cassandra paging state bytes."""
    return base64.urlsafe_b64decode(token.encode("ascii"))
