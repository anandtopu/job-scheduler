"""Execution model representing a job execution instance.

Enhancements over v1:
- duration_ms: actual execution duration in milliseconds
- worker_id: which worker node ran this execution
- timeout_seconds: per-execution timeout
- output_size_bytes: size of result payload
- started_at / completed_at timestamps for precise timing
"""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from .enums import ExecutionStatus


def _time_bucket_from_datetime(dt: datetime) -> int:
    """Round a datetime to the nearest hour and return as unix timestamp."""
    import calendar
    rounded = dt.replace(minute=0, second=0, microsecond=0)
    return calendar.timegm(rounded.timetuple())


class Execution(BaseModel):
    """Represents a single execution instance of a job.

    Executions are partitioned by time_bucket (hour) for efficient
    range queries. The execution_key provides uniqueness within a partition.
    """

    time_bucket: int            # unix timestamp rounded to hour
    execution_key: str          # f"{execution_time_unix}#{job_id}"
    job_id: UUID
    user_id: str
    execution_time: datetime
    status: ExecutionStatus = ExecutionStatus.PENDING
    attempt: int = 0
    result: Optional[str] = None
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # ---- NEW FIELDS (enhancements) ----

    # Precise timestamps for duration analytics
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Actual execution duration in milliseconds (computed on completion)
    duration_ms: Optional[int] = None

    # Which worker node ran this — useful for worker-level debugging
    worker_id: Optional[str] = None

    # Per-execution timeout (inherited from job or global default)
    timeout_seconds: int = 30

    # Payload size tracking (helps detect abnormally large outputs)
    output_size_bytes: int = 0

    # Priority inherited from parent job (affects queue ordering)
    priority: str = "NORMAL"

    model_config = {"use_enum_values": True}

    @classmethod
    def create(
        cls,
        job_id: UUID,
        user_id: str,
        execution_time: datetime,
        priority: str = "NORMAL",
        timeout_seconds: int = 30,
    ) -> "Execution":
        """Factory method to create a new Execution with auto-computed fields."""
        import calendar
        exec_unix = int(calendar.timegm(execution_time.timetuple()))
        time_bucket = _time_bucket_from_datetime(execution_time)
        execution_key = f"{exec_unix}#{job_id}"

        return cls(
            time_bucket=time_bucket,
            execution_key=execution_key,
            job_id=job_id,
            user_id=user_id,
            execution_time=execution_time,
            priority=priority,
            timeout_seconds=timeout_seconds,
        )

    def mark_started(self, worker_id: Optional[str] = None) -> "Execution":
        """Return copy with started_at set."""
        return self.model_copy(update={
            "started_at": datetime.utcnow(),
            "worker_id": worker_id,
            "status": ExecutionStatus.IN_PROGRESS,
            "updated_at": datetime.utcnow(),
        })

    def mark_completed(self, result: Optional[str] = None) -> "Execution":
        """Return copy with completed_at and duration_ms set."""
        now = datetime.utcnow()
        dur = None
        if self.started_at:
            dur = int((now - self.started_at).total_seconds() * 1000)
        return self.model_copy(update={
            "completed_at": now,
            "duration_ms": dur,
            "result": result,
            "output_size_bytes": len(result) if result else 0,
            "status": ExecutionStatus.COMPLETED,
            "updated_at": now,
        })

    def mark_failed(self, error: str, next_status: ExecutionStatus = ExecutionStatus.FAILED) -> "Execution":
        """Return copy with error and failed status."""
        now = datetime.utcnow()
        dur = None
        if self.started_at:
            dur = int((now - self.started_at).total_seconds() * 1000)
        return self.model_copy(update={
            "completed_at": now,
            "duration_ms": dur,
            "error": error,
            "status": next_status,
            "updated_at": now,
        })

    def to_db_dict(self) -> dict:
        """Convert to dictionary suitable for database insertion."""
        return {
            "time_bucket": self.time_bucket,
            "execution_key": self.execution_key,
            "job_id": self.job_id,
            "user_id": self.user_id,
            "execution_time": self.execution_time,
            "status": str(self.status),
            "attempt": self.attempt,
            "result": self.result,
            "error": self.error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_ms": self.duration_ms,
            "worker_id": self.worker_id,
            "timeout_seconds": self.timeout_seconds,
            "output_size_bytes": self.output_size_bytes,
            "priority": self.priority,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
