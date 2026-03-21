"""Pydantic v2 request and response schemas for the API."""

import base64
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from croniter import CroniterBadCronError, croniter
from pydantic import BaseModel, Field, field_validator, model_validator

from src.models.enums import ExecutionStatus, JobStatus, ScheduleType


class CreateJobRequest(BaseModel):
    """Request schema for creating a new job."""

    task_id: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description="Registered task identifier.",
    )
    schedule_type: ScheduleType = Field(
        ..., description="Type of schedule: IMMEDIATE, DATETIME, or CRON."
    )
    schedule_expression: Optional[str] = Field(
        None,
        description=(
            "Schedule expression. None for IMMEDIATE, ISO datetime for DATETIME, "
            "cron expression for CRON."
        ),
    )
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters passed to the task handler.",
    )

    @field_validator("parameters")
    @classmethod
    def validate_parameters_size(cls, v):
        """Ensure parameters dict is not excessively large."""
        import json

        serialized = json.dumps(v)
        if len(serialized) > 65536:  # 64KB limit
            raise ValueError("Parameters must be less than 64KB.")
        return v

    @model_validator(mode="after")
    def validate_schedule_expression(self) -> "CreateJobRequest":
        """Validate schedule_expression based on schedule_type."""
        schedule_type = self.schedule_type
        expr = self.schedule_expression

        if schedule_type == ScheduleType.IMMEDIATE:
            if expr is not None:
                raise ValueError(
                    "schedule_expression must be None for IMMEDIATE schedule type."
                )

        elif schedule_type == ScheduleType.DATETIME:
            if not expr:
                raise ValueError(
                    "schedule_expression is required for DATETIME schedule type."
                )
            try:
                scheduled_dt = datetime.fromisoformat(expr.rstrip("Z"))
            except ValueError as exc:
                raise ValueError(
                    f"schedule_expression must be a valid ISO datetime string: {exc}"
                ) from exc

            # Validate it's in the future (allow up to 60s past for clock drift)
            now = datetime.utcnow()
            if (now - scheduled_dt).total_seconds() > 60:
                raise ValueError(
                    f"schedule_expression must be a future datetime. Got: {expr}"
                )

        elif schedule_type == ScheduleType.CRON:
            if not expr:
                raise ValueError(
                    "schedule_expression is required for CRON schedule type."
                )
            try:
                cron = croniter(expr, datetime.utcnow())
                # Verify it has at least one future occurrence
                next_run = cron.get_next(datetime)
                if next_run is None:
                    raise ValueError("CRON expression has no future occurrences.")
            except (CroniterBadCronError, KeyError) as exc:
                raise ValueError(
                    f"Invalid CRON expression '{expr}': {exc}"
                ) from exc

        return self


class JobResponse(BaseModel):
    """Response schema for a job."""

    job_id: UUID
    user_id: str
    task_id: str
    schedule_type: str
    schedule_expression: Optional[str]
    parameters: Dict[str, Any]
    status: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ExecutionResponse(BaseModel):
    """Response schema for a job execution."""

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


def encode_page_token(page_state: bytes) -> str:
    """Encode Cassandra paging state to a URL-safe string."""
    return base64.urlsafe_b64encode(page_state).decode("ascii")


def decode_page_token(token: str) -> bytes:
    """Decode a page token back to Cassandra paging state bytes."""
    return base64.urlsafe_b64decode(token.encode("ascii"))
