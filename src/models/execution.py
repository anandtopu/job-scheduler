"""Execution model representing a job execution instance."""

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

    time_bucket: int  # unix timestamp rounded to hour
    execution_key: str  # f"{execution_time_unix}#{job_id}"
    job_id: UUID
    user_id: str
    execution_time: datetime
    status: ExecutionStatus = ExecutionStatus.PENDING
    attempt: int = 0
    result: Optional[str] = None
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"use_enum_values": True}

    @classmethod
    def create(
        cls,
        job_id: UUID,
        user_id: str,
        execution_time: datetime,
    ) -> "Execution":
        """Factory method to create a new Execution with auto-computed fields.

        Args:
            job_id: The UUID of the parent job.
            user_id: The user who owns this job.
            execution_time: The scheduled time of execution.

        Returns:
            A new Execution instance.
        """
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
        )

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
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
