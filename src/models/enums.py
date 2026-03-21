"""Enumeration types for job scheduler models."""

from enum import Enum


class JobStatus(str, Enum):
    """Status of a job definition."""

    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    DELETED = "DELETED"


class ExecutionStatus(str, Enum):
    """Status of a job execution instance."""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class ScheduleType(str, Enum):
    """Type of schedule for a job."""

    IMMEDIATE = "IMMEDIATE"
    DATETIME = "DATETIME"
    CRON = "CRON"
