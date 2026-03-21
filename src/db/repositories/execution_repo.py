"""Repository for execution CRUD operations in Cassandra."""

import logging
from datetime import datetime
from typing import List, Optional, Tuple
from uuid import UUID

from src.db.cassandra import CassandraClient
from src.models.enums import ExecutionStatus
from src.models.execution import Execution

logger = logging.getLogger(__name__)


class ExecutionRepository:
    """Data access layer for Execution entities.

    Writes to both the executions (time-bucketed) and user_executions
    (user-query optimized) tables to support multiple access patterns.
    """

    def __init__(self, client: CassandraClient):
        """Initialize with a CassandraClient.

        Args:
            client: Connected CassandraClient instance.
        """
        self._client = client

    def create_execution(self, execution: Execution) -> Execution:
        """Persist a new execution to both executions and user_executions tables.

        Args:
            execution: The Execution instance to create.

        Returns:
            The created Execution instance.
        """
        self._insert_execution(execution)
        self._insert_user_execution(execution)
        logger.info(
            "Created execution %s for job %s.", execution.execution_key, execution.job_id
        )
        return execution

    def _insert_execution(self, execution: Execution) -> None:
        """Write to the main executions table."""
        query = """
            INSERT INTO executions (
                time_bucket, execution_key, job_id, user_id, execution_time,
                status, attempt, result, error, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
        """
        self._client.execute(
            query,
            (
                execution.time_bucket,
                execution.execution_key,
                execution.job_id,
                execution.user_id,
                execution.execution_time,
                str(execution.status),
                execution.attempt,
                execution.result,
                execution.error,
                execution.created_at,
                execution.updated_at,
            ),
        )

    def _insert_user_execution(self, execution: Execution) -> None:
        """Write to the user_executions denormalized table."""
        query = """
            INSERT INTO user_executions (
                user_id, execution_time, execution_key, time_bucket,
                job_id, status, attempt
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        self._client.execute(
            query,
            (
                execution.user_id,
                execution.execution_time,
                execution.execution_key,
                execution.time_bucket,
                execution.job_id,
                str(execution.status),
                execution.attempt,
            ),
        )

    def get_executions_by_time_bucket(self, time_bucket: int) -> List[Execution]:
        """Retrieve all executions in a specific time bucket.

        Args:
            time_bucket: The hour-aligned unix timestamp bucket.

        Returns:
            List of Execution instances.
        """
        query = "SELECT * FROM executions WHERE time_bucket = %s"
        rows = self._client.execute(query, (time_bucket,))
        return [self._row_to_execution(row) for row in rows]

    def get_executions_in_range(
        self,
        start_bucket: int,
        end_bucket: int,
        statuses: List[ExecutionStatus],
    ) -> List[Execution]:
        """Retrieve executions within a range of time buckets filtered by status.

        Since Cassandra doesn't support IN on partition keys efficiently,
        we query each bucket individually and filter client-side.

        Args:
            start_bucket: Start time bucket (inclusive).
            end_bucket: End time bucket (inclusive).
            statuses: Filter to executions with these statuses.

        Returns:
            List of Execution instances matching the criteria.
        """
        status_values = {str(s) for s in statuses}
        results = []

        # Generate all hourly buckets in range
        current = start_bucket
        hour_seconds = 3600
        while current <= end_bucket:
            executions = self.get_executions_by_time_bucket(current)
            for ex in executions:
                if str(ex.status) in status_values:
                    results.append(ex)
            current += hour_seconds

        return results

    def update_execution_status(
        self,
        execution: Execution,
        status: ExecutionStatus,
        attempt: int,
        result: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """Update execution status in both tables.

        Args:
            execution: The Execution to update.
            status: New ExecutionStatus.
            attempt: Current attempt number.
            result: Optional result string on success.
            error: Optional error message on failure.
        """
        updated_at = datetime.utcnow()

        # Update executions table
        query_exec = """
            UPDATE executions SET status = %s, attempt = %s, result = %s,
                error = %s, updated_at = %s
            WHERE time_bucket = %s AND execution_key = %s
        """
        self._client.execute(
            query_exec,
            (
                str(status),
                attempt,
                result,
                error,
                updated_at,
                execution.time_bucket,
                execution.execution_key,
            ),
        )

        # Update user_executions table
        query_user = """
            UPDATE user_executions SET status = %s, attempt = %s
            WHERE user_id = %s AND execution_time = %s AND execution_key = %s
        """
        self._client.execute(
            query_user,
            (
                str(status),
                attempt,
                execution.user_id,
                execution.execution_time,
                execution.execution_key,
            ),
        )

        logger.info(
            "Updated execution %s to status %s (attempt %d).",
            execution.execution_key,
            status,
            attempt,
        )

    def get_user_executions(
        self,
        user_id: str,
        status: Optional[ExecutionStatus] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 20,
        page_state: Optional[bytes] = None,
    ) -> Tuple[List[Execution], Optional[bytes]]:
        """Query executions for a user with optional filters and pagination.

        Args:
            user_id: The user to query for.
            status: Optional status filter.
            start_time: Optional start time filter.
            end_time: Optional end time filter.
            limit: Maximum number of results to return.
            page_state: Cassandra paging state for pagination.

        Returns:
            Tuple of (list of Executions, next page state or None).
        """
        from cassandra.query import SimpleStatement

        base_query = "SELECT * FROM user_executions WHERE user_id = %s"
        params = [user_id]

        if start_time:
            base_query += " AND execution_time >= %s"
            params.append(start_time)
        if end_time:
            base_query += " AND execution_time <= %s"
            params.append(end_time)

        stmt = SimpleStatement(base_query, fetch_size=limit)

        try:
            result_set = self._client.get_session().execute(
                stmt, params, paging_state=page_state
            )
        except Exception as exc:
            logger.error("Failed to query user executions: %s", exc)
            return [], None

        executions = []
        for row in result_set.current_rows:
            if status and str(row.status) != str(status):
                continue
            executions.append(self._user_row_to_execution(row))

        next_page_state = result_set.paging_state if result_set.has_more_pages else None
        return executions, next_page_state

    def get_execution_by_key(
        self, time_bucket: int, execution_key: str
    ) -> Optional[Execution]:
        """Retrieve a single execution by its primary key.

        Args:
            time_bucket: The time bucket partition key.
            execution_key: The execution key clustering key.

        Returns:
            The Execution instance, or None if not found.
        """
        query = """
            SELECT * FROM executions
            WHERE time_bucket = %s AND execution_key = %s
        """
        rows = self._client.execute(query, (time_bucket, execution_key))
        row = rows.one()
        if row is None:
            return None
        return self._row_to_execution(row)

    def _row_to_execution(self, row) -> Execution:
        """Convert a Cassandra executions row to an Execution instance."""
        return Execution(
            time_bucket=row.time_bucket,
            execution_key=row.execution_key,
            job_id=row.job_id,
            user_id=row.user_id,
            execution_time=row.execution_time,
            status=row.status,
            attempt=row.attempt or 0,
            result=row.result,
            error=row.error,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    def _user_row_to_execution(self, row) -> Execution:
        """Convert a Cassandra user_executions row to a partial Execution instance."""
        return Execution(
            time_bucket=row.time_bucket,
            execution_key=row.execution_key,
            job_id=row.job_id,
            user_id=row.user_id,
            execution_time=row.execution_time,
            status=row.status,
            attempt=row.attempt or 0,
        )
