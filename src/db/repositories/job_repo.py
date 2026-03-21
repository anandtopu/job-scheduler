"""Repository for job CRUD operations in Cassandra."""

import json
import logging
from typing import List, Optional
from uuid import UUID

from src.db.cassandra import CassandraClient
from src.models.enums import JobStatus
from src.models.job import Job

logger = logging.getLogger(__name__)


class JobRepository:
    """Data access layer for Job entities.

    Provides CRUD operations against the Cassandra jobs table.
    """

    def __init__(self, client: CassandraClient):
        """Initialize with a CassandraClient.

        Args:
            client: Connected CassandraClient instance.
        """
        self._client = client

    def create_job(self, job: Job) -> Job:
        """Persist a new job to the database.

        Args:
            job: The Job instance to create.

        Returns:
            The created Job instance (same as input).
        """
        query = """
            INSERT INTO jobs (
                job_id, user_id, task_id, schedule_type, schedule_expression,
                parameters, status, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        db_dict = job.to_db_dict()
        self._client.execute(
            query,
            (
                db_dict["job_id"],
                db_dict["user_id"],
                db_dict["task_id"],
                db_dict["schedule_type"],
                db_dict["schedule_expression"],
                db_dict["parameters"],
                db_dict["status"],
                db_dict["created_at"],
                db_dict["updated_at"],
            ),
        )
        logger.info("Created job %s for user %s.", job.job_id, job.user_id)
        return job

    def get_job(self, job_id: UUID) -> Optional[Job]:
        """Retrieve a job by its ID.

        Args:
            job_id: The UUID of the job.

        Returns:
            The Job instance, or None if not found.
        """
        query = "SELECT * FROM jobs WHERE job_id = %s"
        rows = self._client.execute(query, (job_id,))
        row = rows.one()
        if row is None:
            return None
        return self._row_to_job(row)

    def get_jobs_by_user(self, user_id: str) -> List[Job]:
        """Retrieve all jobs owned by a user.

        Args:
            user_id: The user identifier.

        Returns:
            List of Job instances.
        """
        query = "SELECT * FROM jobs WHERE user_id = %s ALLOW FILTERING"
        rows = self._client.execute(query, (user_id,))
        return [self._row_to_job(row) for row in rows]

    def update_job_status(self, job_id: UUID, status: JobStatus) -> None:
        """Update the status of a job.

        Args:
            job_id: The UUID of the job.
            status: The new status to set.
        """
        from datetime import datetime

        query = "UPDATE jobs SET status = %s, updated_at = %s WHERE job_id = %s"
        self._client.execute(query, (str(status), datetime.utcnow(), job_id))
        logger.info("Updated job %s status to %s.", job_id, status)

    def delete_job(self, job_id: UUID) -> None:
        """Soft-delete a job by setting its status to DELETED.

        Args:
            job_id: The UUID of the job to delete.
        """
        self.update_job_status(job_id, JobStatus.DELETED)
        logger.info("Soft-deleted job %s.", job_id)

    def _row_to_job(self, row) -> Job:
        """Convert a Cassandra row to a Job model instance.

        Args:
            row: A Cassandra result row.

        Returns:
            A Job instance.
        """
        parameters = {}
        if row.parameters:
            try:
                parameters = json.loads(row.parameters)
            except (json.JSONDecodeError, TypeError):
                parameters = {}

        return Job(
            job_id=row.job_id,
            user_id=row.user_id,
            task_id=row.task_id,
            schedule_type=row.schedule_type,
            schedule_expression=row.schedule_expression,
            parameters=parameters,
            status=row.status,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )
