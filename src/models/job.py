"""Job model representing a job definition."""

from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from .enums import JobStatus, ScheduleType


class Job(BaseModel):
    """Represents a job definition in the scheduler system.

    A job is a template/definition that describes what task to run,
    when to run it, and with what parameters.
    """

    job_id: UUID = Field(default_factory=uuid4)
    user_id: str
    task_id: str
    schedule_type: ScheduleType
    schedule_expression: Optional[str] = None
    parameters: dict = Field(default_factory=dict)
    status: JobStatus = JobStatus.ACTIVE
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"use_enum_values": True}

    def is_active(self) -> bool:
        """Check if the job is in active state."""
        return self.status == JobStatus.ACTIVE

    def to_db_dict(self) -> dict:
        """Convert to dictionary suitable for database insertion."""
        import json

        return {
            "job_id": self.job_id,
            "user_id": self.user_id,
            "task_id": self.task_id,
            "schedule_type": str(self.schedule_type),
            "schedule_expression": self.schedule_expression,
            "parameters": json.dumps(self.parameters),
            "status": str(self.status),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
