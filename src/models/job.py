"""Job model representing a job definition.

Enhancements over v1:
- priority field (LOW/NORMAL/HIGH/CRITICAL)
- tags for categorization and filtering
- execution_timeout_seconds for per-job timeout (pain point: global timeout only)
- notification_url / notification_channel for failure alerts
- max_executions for one-shot limiting (pain point: CRON ran forever)
- last_executed_at for scheduling continuity
- description field for human-readable context
- dependencies list for DAG-style job chaining (new feature)
"""

from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from .enums import JobPriority, JobStatus, NotificationChannel, ScheduleType


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

    # ---- NEW FIELDS (enhancements) ----

    # Priority queue support — pain point: v1 had no priority
    priority: JobPriority = JobPriority.NORMAL

    # Human-readable label for the job
    description: Optional[str] = None

    # Organizational tags e.g. ["billing", "prod", "daily"]
    tags: List[str] = Field(default_factory=list)

    # Per-job execution timeout in seconds (0 = use global default)
    execution_timeout_seconds: int = 0

    # Failure notification — pain point: v1 had no alerting
    notification_url: Optional[str] = None
    notification_channel: Optional[NotificationChannel] = None

    # Cap total executions (useful for CRON jobs that should stop after N runs)
    max_executions: Optional[int] = None

    # Track how many times this job has successfully executed
    execution_count: int = 0

    # Track last execution time for interval/cron continuity
    last_executed_at: Optional[datetime] = None

    # DAG dependencies: list of job_ids that must COMPLETE before this runs
    # Pain point: v1 had no job chaining or dependency support
    depends_on: List[str] = Field(default_factory=list)

    # Timezone for CRON expressions (pain point: v1 only supported UTC)
    timezone: str = "UTC"

    # Unique constraint name — prevent two jobs with same name per user
    job_name: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"use_enum_values": True}

    def is_active(self) -> bool:
        """Check if the job is in active state."""
        return self.status == JobStatus.ACTIVE

    def has_reached_max_executions(self) -> bool:
        """Check if the job has run the maximum allowed number of times."""
        if self.max_executions is None:
            return False
        return self.execution_count >= self.max_executions

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
            "priority": str(self.priority),
            "description": self.description,
            "tags": json.dumps(self.tags),
            "execution_timeout_seconds": self.execution_timeout_seconds,
            "notification_url": self.notification_url,
            "notification_channel": str(self.notification_channel) if self.notification_channel else None,
            "max_executions": self.max_executions,
            "execution_count": self.execution_count,
            "last_executed_at": self.last_executed_at,
            "depends_on": json.dumps(self.depends_on),
            "timezone": self.timezone,
            "job_name": self.job_name,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
