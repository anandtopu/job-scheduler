"""Enumeration types for job scheduler models.

Enhancements over v1:
- Added INTERVAL schedule type
- Added PAUSED execution status
- Added JobPriority enum (missing in v1 — pain point for users needing priority queues)
- Added TriggerType for webhook/event-driven jobs
"""

from enum import Enum


class JobStatus(str, Enum):
    """Status of a job definition."""

    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    DELETED = "DELETED"
    COMPLETED = "COMPLETED"   # NEW: for one-shot jobs that finished all runs


class ExecutionStatus(str, Enum):
    """Status of a job execution instance."""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"   # NEW: explicitly cancelled mid-flight
    SKIPPED = "SKIPPED"        # NEW: missed execution (e.g. job was paused)
    TIMED_OUT = "TIMED_OUT"   # NEW: exceeded execution timeout


class ScheduleType(str, Enum):
    """Type of schedule for a job."""

    IMMEDIATE = "IMMEDIATE"
    DATETIME = "DATETIME"
    CRON = "CRON"
    INTERVAL = "INTERVAL"     # NEW: run every N seconds (pain point: users wanted simple interval)


class JobPriority(str, Enum):
    """Execution priority for jobs in the queue.

    Pain point: v1 had no priority support. High-priority jobs queued
    behind low-priority jobs causing SLA violations.
    """

    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class NotificationChannel(str, Enum):
    """Channel for job failure/completion notifications.

    Pain point: v1 had no notification support at all.
    Users had to poll the API to detect failures.
    """

    WEBHOOK = "WEBHOOK"
    EMAIL = "EMAIL"
    SLACK = "SLACK"
