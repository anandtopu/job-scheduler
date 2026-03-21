"""Integration tests for the full job scheduling and execution flow.

These tests require running Cassandra and Redis instances.
They are skipped automatically if the services are unavailable.

To run:
    docker-compose up -d cassandra redis
    pytest tests/integration/ -v
"""

import time
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
import redis as redis_lib

from src.core.config import settings
from src.db.cassandra import CassandraClient
from src.db.repositories.execution_repo import ExecutionRepository
from src.db.repositories.job_repo import JobRepository
from src.models.enums import ExecutionStatus, JobStatus, ScheduleType
from src.models.job import Job
from src.queue.redis_queue import RedisQueue
from src.scheduler.scheduler import Scheduler
from src.tasks.builtin import log_task  # noqa: F401 - Register tasks
from src.worker.worker import Worker


def check_cassandra_available() -> bool:
    """Check if Cassandra is available for integration tests."""
    try:
        client = CassandraClient()
        client.connect(max_retries=1, retry_delay=1.0)
        client.disconnect()
        return True
    except Exception:
        return False


def check_redis_available() -> bool:
    """Check if Redis is available for integration tests."""
    try:
        r = redis_lib.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            socket_connect_timeout=2,
        )
        r.ping()
        return True
    except Exception:
        return False


requires_cassandra = pytest.mark.skipif(
    not check_cassandra_available(),
    reason="Cassandra not available",
)

requires_redis = pytest.mark.skipif(
    not check_redis_available(),
    reason="Redis not available",
)

requires_all = pytest.mark.skipif(
    not (check_cassandra_available() and check_redis_available()),
    reason="Cassandra and/or Redis not available",
)


@pytest.fixture(scope="module")
def cassandra_client():
    """Module-scoped Cassandra client for integration tests."""
    client = CassandraClient()
    client.connect()
    client.initialize_schema()
    yield client
    client.disconnect()


@pytest.fixture(scope="module")
def redis_queue():
    """Module-scoped Redis queue for integration tests."""
    return RedisQueue()


@pytest.fixture(scope="module")
def job_repo(cassandra_client):
    return JobRepository(cassandra_client)


@pytest.fixture(scope="module")
def exec_repo(cassandra_client):
    return ExecutionRepository(cassandra_client)


@pytest.fixture(scope="module")
def scheduler(redis_queue, cassandra_client):
    return Scheduler(queue=redis_queue, cassandra_client=cassandra_client)


def make_test_job(
    schedule_type: ScheduleType = ScheduleType.IMMEDIATE,
    schedule_expression=None,
    user_id: str = None,
) -> Job:
    return Job(
        job_id=uuid4(),
        user_id=user_id or f"integration-test-{uuid4().hex[:8]}",
        task_id="log_message",
        schedule_type=schedule_type,
        schedule_expression=schedule_expression,
        parameters={"message": "Integration test message", "level": "INFO"},
        status=JobStatus.ACTIVE,
    )


@requires_all
class TestCreateAndExecuteImmediateJob:
    def test_create_and_execute_immediate_job(
        self, job_repo, exec_repo, scheduler, redis_queue, cassandra_client
    ):
        """Create an IMMEDIATE job, execute it, and verify COMPLETED status."""
        job = make_test_job(ScheduleType.IMMEDIATE)
        job_repo.create_job(job)

        execution = scheduler.schedule_immediate_job(job)
        assert execution is not None

        # Execute via worker
        worker = Worker(
            queue=redis_queue,
            cassandra_client=cassandra_client,
            concurrency=1,
            max_retries=1,
        )

        messages = redis_queue.dequeue(count=1)
        assert len(messages) == 1

        worker.execute_job(messages[0])

        # Verify execution status
        result_exec = exec_repo.get_execution_by_key(
            execution.time_bucket, execution.execution_key
        )
        assert result_exec is not None
        assert str(result_exec.status) == ExecutionStatus.COMPLETED


@requires_all
class TestCreateAndExecuteDatetimeJob:
    def test_create_and_execute_datetime_job(
        self, job_repo, exec_repo, scheduler, redis_queue, cassandra_client
    ):
        """Create a DATETIME job within the lookahead window and execute it."""
        future = datetime.utcnow() + timedelta(seconds=30)
        job = make_test_job(
            ScheduleType.DATETIME,
            schedule_expression=future.isoformat(),
        )
        job_repo.create_job(job)

        execution = scheduler.schedule_datetime_job(job)
        assert execution is not None

        # Job within lookahead should be enqueued
        depth = redis_queue.get_queue_depth()
        assert depth["pending"] >= 1


@requires_all
class TestCreateRecurringCronJob:
    def test_create_recurring_cron_job(self, job_repo, exec_repo, scheduler):
        """Create a CRON job and verify multiple future executions are created."""
        job = make_test_job(
            ScheduleType.CRON,
            schedule_expression="*/5 * * * *",  # Every 5 minutes
        )
        job_repo.create_job(job)

        executions = scheduler.schedule_cron_job(job)

        assert len(executions) == 10
        for ex in executions:
            assert ex.execution_time > datetime.utcnow()


@requires_all
class TestJobRetryOnFailure:
    def test_job_retry_on_failure(
        self, job_repo, exec_repo, scheduler, redis_queue, cassandra_client
    ):
        """Failed job is retried with exponential backoff."""
        from unittest.mock import patch

        job = make_test_job(ScheduleType.IMMEDIATE)
        job_repo.create_job(job)

        execution = scheduler.schedule_immediate_job(job)

        worker = Worker(
            queue=redis_queue,
            cassandra_client=cassandra_client,
            concurrency=1,
            max_retries=2,
            retry_base_delay=1,
        )

        messages = redis_queue.dequeue(count=1)
        assert len(messages) == 1

        with patch(
            "src.tasks.registry.TASK_REGISTRY.execute",
            side_effect=RuntimeError("Simulated failure"),
        ):
            worker.execute_job(messages[0])

        # Verify the execution is marked as RETRYING
        result_exec = exec_repo.get_execution_by_key(
            execution.time_bucket, execution.execution_key
        )
        assert result_exec is not None
        assert str(result_exec.status) in [ExecutionStatus.RETRYING, ExecutionStatus.FAILED]


@requires_all
class TestQueryJobStatus:
    def test_query_job_status(self, job_repo, exec_repo, scheduler, redis_queue):
        """Query job and execution status via repositories."""
        user_id = f"query-test-{uuid4().hex[:8]}"
        job = make_test_job(ScheduleType.IMMEDIATE, user_id=user_id)
        job_repo.create_job(job)

        execution = scheduler.schedule_immediate_job(job)

        # Query by user
        jobs = job_repo.get_jobs_by_user(user_id)
        assert len(jobs) == 1
        assert str(jobs[0].job_id) == str(job.job_id)

        # Query executions by user
        executions, _ = exec_repo.get_user_executions(user_id=user_id, limit=10)
        assert len(executions) >= 1
