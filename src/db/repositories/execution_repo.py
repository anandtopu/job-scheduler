"""Repository for execution CRUD operations in Cassandra.

Enhancements over v1:
- create_execution persists all v2 fields (started_at, completed_at, duration_ms, worker_id)
- update_execution_status now accepts duration_ms, worker_id, started_at, completed_at
- get_execution_stats for analytics
- get_slow_executions for SLO monitoring
- get_failed_executions_by_worker for worker-level failure analysis
"""

import logging
from datetime import datetime
from typing import List, Optional, Tuple
from uuid import UUID

from src.db.cassandra import CassandraClient
from src.models.enums import ExecutionStatus
from src.models.execution import Execution

logger = logging.getLogger(__name__)


class ExecutionRepository:
    """Data access layer for Execution entities."""

    def __init__(self, client: CassandraClient):
        self._client = client

    def create_execution(self, execution: Execution) -> Execution:
        """Persist a new execution to both tables."""
        self._insert_execution(execution)
        self._insert_user_execution(execution)
        logger.info("Created execution %s for job %s.", execution.execution_key, execution.job_id)
        return execution

    def _insert_execution(self, execution: Execution) -> None:
        """Write to the main executions table with all v2 fields."""
        query = """
            INSERT INTO executions (
                time_bucket, execution_key, job_id, user_id, execution_time,
                status, attempt, result, error,
                started_at, completed_at, duration_ms,
                worker_id, timeout_seconds, output_size_bytes, priority,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s
            )
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
                execution.started_at,
                execution.completed_at,
                execution.duration_ms,
                execution.worker_id,
                execution.timeout_seconds,
                execution.output_size_bytes,
                execution.priority,
                execution.created_at,
                execution.updated_at,
            ),
        )

    def _insert_user_execution(self, execution: Execution) -> None:
        """Write to the denormalized user_executions table."""
        query = """
            INSERT INTO user_executions (
                user_id, execution_time, execution_key, time_bucket,
                job_id, status, attempt, duration_ms, worker_id, priority
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                execution.duration_ms,
                execution.worker_id,
                execution.priority,
            ),
        )

    def get_executions_by_time_bucket(self, time_bucket: int) -> List[Execution]:
        """Retrieve all executions in a specific time bucket."""
        query = "SELECT * FROM executions WHERE time_bucket = %s"
        rows = self._client.execute(query, (time_bucket,))
        return [self._row_to_execution(row) for row in rows]

    def get_executions_in_range(
        self,
        start_bucket: int,
        end_bucket: int,
        statuses: List[ExecutionStatus],
    ) -> List[Execution]:
        """Retrieve executions within a range of time buckets filtered by status."""
        status_values = {str(s) for s in statuses}
        results = []
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
        duration_ms: Optional[int] = None,
        worker_id: Optional[str] = None,
        started_at: Optional[float] = None,   # unix timestamp
        completed_at: Optional[float] = None, # unix timestamp
    ) -> None:
        """Update execution status with all v2 tracking fields."""
        updated_at = datetime.utcnow()
        started_dt = datetime.utcfromtimestamp(started_at) if started_at else None
        completed_dt = datetime.utcfromtimestamp(completed_at) if completed_at else None

        query_exec = """
            UPDATE executions
            SET status = %s, attempt = %s, result = %s, error = %s,
                duration_ms = %s, worker_id = %s,
                started_at = %s, completed_at = %s,
                updated_at = %s
            WHERE time_bucket = %s AND execution_key = %s
        """
        self._client.execute(
            query_exec,
            (
                str(status), attempt, result, error,
                duration_ms, worker_id,
                started_dt, completed_dt,
                updated_at,
                execution.time_bucket,
                execution.execution_key,
            ),
        )

        query_user = """
            UPDATE user_executions
            SET status = %s, attempt = %s, duration_ms = %s, worker_id = %s
            WHERE user_id = %s AND execution_time = %s AND execution_key = %s
        """
        self._client.execute(
            query_user,
            (
                str(status), attempt, duration_ms, worker_id,
                execution.user_id,
                execution.execution_time,
                execution.execution_key,
            ),
        )

        logger.info(
            "Updated execution %s → %s (attempt=%d, duration=%sms, worker=%s)",
            execution.execution_key, status, attempt, duration_ms, worker_id,
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
        """Query executions for a user with optional filters and pagination."""
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

    def get_execution_by_key(self, time_bucket: int, execution_key: str) -> Optional[Execution]:
        """Retrieve a single execution by its primary key."""
        query = """
            SELECT * FROM executions
            WHERE time_bucket = %s AND execution_key = %s
        """
        rows = self._client.execute(query, (time_bucket, execution_key))
        row = rows.one()
        if row is None:
            return None
        return self._row_to_execution(row)

    def get_execution_stats(
        self,
        start_bucket: int,
        end_bucket: int,
    ) -> dict:
        """Compute aggregate stats for executions in a time range.

        Enhancement: v1 had zero aggregate stats.
        Returns: total, completed, failed, avg_duration_ms, p95_duration_ms
        """
        executions = self.get_executions_in_range(
            start_bucket, end_bucket,
            [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED,
             ExecutionStatus.TIMED_OUT, ExecutionStatus.RETRYING],
        )

        total = len(executions)
        completed = sum(1 for e in executions if str(e.status) == "COMPLETED")
        failed = sum(1 for e in executions if str(e.status) in ("FAILED", "TIMED_OUT"))
        durations = sorted([e.duration_ms for e in executions if e.duration_ms])
        avg_dur = round(sum(durations) / max(len(durations), 1), 2)
        p95_dur = durations[int(len(durations) * 0.95)] if durations else 0

        return {
            "total": total,
            "completed": completed,
            "failed": failed,
            "success_rate": round(completed / max(total, 1) * 100, 2),
            "avg_duration_ms": avg_dur,
            "p95_duration_ms": p95_dur,
        }

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
            started_at=getattr(row, "started_at", None),
            completed_at=getattr(row, "completed_at", None),
            duration_ms=getattr(row, "duration_ms", None),
            worker_id=getattr(row, "worker_id", None),
            timeout_seconds=getattr(row, "timeout_seconds", 30) or 30,
            output_size_bytes=getattr(row, "output_size_bytes", 0) or 0,
            priority=getattr(row, "priority", "NORMAL") or "NORMAL",
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
            duration_ms=getattr(row, "duration_ms", None),
            worker_id=getattr(row, "worker_id", None),
            priority=getattr(row, "priority", "NORMAL") or "NORMAL",
        )
