"""Repository for job CRUD operations in Cassandra.

Enhancements over v1:
- create_job persists all v2 fields (priority, tags, timeout, notifications, deps)
- get_jobs_by_priority for priority-based queries
- get_jobs_by_tag for tag-based queries
- pause_job / resume_job convenience methods
- increment_execution_count for tracking runs
- update_last_executed_at for scheduling continuity
- get_job_by_name for unique-name lookup
"""

import json
import logging
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from src.db.cassandra import CassandraClient
from src.models.enums import JobPriority, JobStatus
from src.models.job import Job

logger = logging.getLogger(__name__)


class JobRepository:
    """Data access layer for Job entities."""

    def __init__(self, client: CassandraClient):
        self._client = client

    def create_job(self, job: Job) -> Job:
        """Persist a new job to the database including all v2 fields."""
        query = """
            INSERT INTO jobs (
                job_id, user_id, task_id, schedule_type, schedule_expression,
                parameters, status, priority, description, tags,
                execution_timeout_seconds, notification_url, notification_channel,
                max_executions, execution_count, last_executed_at,
                depends_on, timezone, job_name, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """
        d = job.to_db_dict()
        self._client.execute(
            query,
            (
                d["job_id"], d["user_id"], d["task_id"], d["schedule_type"], d["schedule_expression"],
                d["parameters"], d["status"], d["priority"], d["description"], d["tags"],
                d["execution_timeout_seconds"], d["notification_url"], d["notification_channel"],
                d["max_executions"], d["execution_count"], d["last_executed_at"],
                d["depends_on"], d["timezone"], d["job_name"], d["created_at"], d["updated_at"],
            ),
        )
        logger.info("Created job %s (user=%s, priority=%s)", job.job_id, job.user_id, job.priority)
        return job

    def get_job(self, job_id: UUID) -> Optional[Job]:
        """Retrieve a job by its ID."""
        query = "SELECT * FROM jobs WHERE job_id = %s"
        rows = self._client.execute(query, (job_id,))
        row = rows.one()
        if row is None:
            return None
        return self._row_to_job(row)

    def get_jobs_by_user(self, user_id: str) -> List[Job]:
        """Retrieve all non-deleted jobs owned by a user."""
        query = "SELECT * FROM jobs WHERE user_id = %s ALLOW FILTERING"
        rows = self._client.execute(query, (user_id,))
        return [self._row_to_job(r) for r in rows if r.status != "DELETED"]

    def get_jobs_by_status(self, status: JobStatus) -> List[Job]:
        """Retrieve all jobs with a given status. Useful for batch operations."""
        query = "SELECT * FROM jobs WHERE status = %s ALLOW FILTERING"
        rows = self._client.execute(query, (str(status),))
        return [self._row_to_job(r) for r in rows]

    def get_jobs_by_priority(self, priority: JobPriority, user_id: Optional[str] = None) -> List[Job]:
        """Retrieve active jobs filtered by priority.

        Enhancement: v1 had no priority filtering support.
        """
        query = "SELECT * FROM jobs WHERE priority = %s ALLOW FILTERING"
        rows = self._client.execute(query, (str(priority),))
        jobs = [self._row_to_job(r) for r in rows if r.status != "DELETED"]
        if user_id:
            jobs = [j for j in jobs if j.user_id == user_id]
        return jobs

    def get_jobs_by_tag(self, tag: str, user_id: Optional[str] = None) -> List[Job]:
        """Find jobs that have a specific tag.

        Enhancement: v1 had no tag support at all.
        Cassandra doesn't support array contains natively, so we filter client-side.
        """
        query = "SELECT * FROM jobs ALLOW FILTERING"
        rows = self._client.execute(query)
        jobs = []
        for r in rows:
            if r.status == "DELETED":
                continue
            tags = []
            try:
                tags = json.loads(r.tags or "[]")
            except (json.JSONDecodeError, TypeError):
                pass
            if tag in tags:
                job = self._row_to_job(r)
                if user_id is None or job.user_id == user_id:
                    jobs.append(job)
        return jobs

    def get_job_by_name(self, job_name: str, user_id: str) -> Optional[Job]:
        """Find a job by its unique name within a user's scope.

        Enhancement: v1 had no job naming or uniqueness support.
        """
        query = "SELECT * FROM jobs WHERE user_id = %s AND job_name = %s ALLOW FILTERING"
        rows = self._client.execute(query, (user_id, job_name))
        row = rows.one()
        if row is None:
            return None
        return self._row_to_job(row)

    def update_job_status(self, job_id: UUID, status: JobStatus) -> None:
        """Update the status of a job."""
        query = "UPDATE jobs SET status = %s, updated_at = %s WHERE job_id = %s"
        self._client.execute(query, (str(status), datetime.utcnow(), job_id))
        logger.info("Updated job %s status to %s.", job_id, status)

    def pause_job(self, job_id: UUID) -> None:
        """Pause a job — it won't be scheduled until resumed.

        Enhancement: v1 had PAUSED status but no dedicated method or enforcement.
        """
        self.update_job_status(job_id, JobStatus.PAUSED)
        logger.info("Paused job %s.", job_id)

    def resume_job(self, job_id: UUID) -> None:
        """Resume a paused job.

        Enhancement: v1 had no resume mechanism.
        """
        self.update_job_status(job_id, JobStatus.ACTIVE)
        logger.info("Resumed job %s.", job_id)

    def increment_execution_count(self, job_id: UUID) -> None:
        """Increment the execution_count for a job.

        Enhancement: v1 never tracked how many times a job ran.
        This is needed for max_executions support.
        """
        query = """
            UPDATE jobs
            SET execution_count = execution_count + 1,
                last_executed_at = %s,
                updated_at = %s
            WHERE job_id = %s
        """
        now = datetime.utcnow()
        self._client.execute(query, (now, now, job_id))

    def delete_job(self, job_id: UUID) -> None:
        """Soft-delete a job by setting its status to DELETED."""
        self.update_job_status(job_id, JobStatus.DELETED)

    def _row_to_job(self, row) -> Job:
        """Convert a Cassandra row to a Job model instance."""
        parameters = {}
        if row.parameters:
            try:
                parameters = json.loads(row.parameters)
            except (json.JSONDecodeError, TypeError):
                parameters = {}

        tags = []
        if hasattr(row, "tags") and row.tags:
            try:
                tags = json.loads(row.tags)
            except (json.JSONDecodeError, TypeError):
                tags = []

        depends_on = []
        if hasattr(row, "depends_on") and row.depends_on:
            try:
                depends_on = json.loads(row.depends_on)
            except (json.JSONDecodeError, TypeError):
                depends_on = []

        return Job(
            job_id=row.job_id,
            user_id=row.user_id,
            task_id=row.task_id,
            schedule_type=row.schedule_type,
            schedule_expression=row.schedule_expression,
            parameters=parameters,
            status=row.status,
            priority=getattr(row, "priority", "NORMAL") or "NORMAL",
            description=getattr(row, "description", None),
            tags=tags,
            execution_timeout_seconds=getattr(row, "execution_timeout_seconds", 0) or 0,
            notification_url=getattr(row, "notification_url", None),
            notification_channel=getattr(row, "notification_channel", None),
            max_executions=getattr(row, "max_executions", None),
            execution_count=getattr(row, "execution_count", 0) or 0,
            last_executed_at=getattr(row, "last_executed_at", None),
            depends_on=depends_on,
            timezone=getattr(row, "timezone", "UTC") or "UTC",
            job_name=getattr(row, "job_name", None),
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
