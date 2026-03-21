"""Shared pytest fixtures for unit and integration tests."""

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import fakeredis
import pytest
from fastapi.testclient import TestClient

from src.core.config import settings
from src.models.enums import ExecutionStatus, JobStatus, ScheduleType
from src.models.execution import Execution
from src.models.job import Job
from src.queue.redis_queue import RedisQueue


# ---------------------------------------------------------------------------
# Infrastructure mocks
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_cassandra_session():
    """Provide a MagicMock Cassandra session."""
    session = MagicMock()
    # Default: execute returns an empty result set
    result = MagicMock()
    result.one.return_value = None
    result.__iter__ = MagicMock(return_value=iter([]))
    result.current_rows = []
    result.has_more_pages = False
    result.paging_state = None
    session.execute.return_value = result
    return session


@pytest.fixture
def mock_cassandra_client(mock_cassandra_session):
    """Provide a MagicMock CassandraClient backed by mock_cassandra_session."""
    client = MagicMock()
    client.get_session.return_value = mock_cassandra_session
    client.execute.return_value = mock_cassandra_session.execute.return_value
    return client


@pytest.fixture
def fake_redis():
    """Provide a fakeredis client for in-memory Redis simulation."""
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def mock_redis_queue(fake_redis):
    """Provide a RedisQueue backed by fakeredis."""
    queue = RedisQueue(client=fake_redis)
    return queue


# ---------------------------------------------------------------------------
# Model fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_job():
    """Return a sample IMMEDIATE Job instance."""
    return Job(
        job_id=uuid4(),
        user_id="test-user-123",
        task_id="log_message",
        schedule_type=ScheduleType.IMMEDIATE,
        schedule_expression=None,
        parameters={"message": "Hello, World!", "level": "INFO"},
        status=JobStatus.ACTIVE,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


@pytest.fixture
def sample_cron_job():
    """Return a sample CRON Job instance."""
    return Job(
        job_id=uuid4(),
        user_id="test-user-123",
        task_id="log_message",
        schedule_type=ScheduleType.CRON,
        schedule_expression="0 * * * *",  # Every hour
        parameters={"message": "Hourly ping", "level": "INFO"},
        status=JobStatus.ACTIVE,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


@pytest.fixture
def sample_datetime_job():
    """Return a sample DATETIME Job instance scheduled 1 hour from now."""
    future = datetime.utcnow() + timedelta(hours=1)
    return Job(
        job_id=uuid4(),
        user_id="test-user-123",
        task_id="log_message",
        schedule_type=ScheduleType.DATETIME,
        schedule_expression=future.isoformat(),
        parameters={"message": "Future message", "level": "INFO"},
        status=JobStatus.ACTIVE,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


@pytest.fixture
def sample_execution(sample_job):
    """Return a sample Execution for sample_job."""
    return Execution.create(
        job_id=sample_job.job_id,
        user_id=sample_job.user_id,
        execution_time=datetime.utcnow(),
    )


# ---------------------------------------------------------------------------
# API test client
# ---------------------------------------------------------------------------


@pytest.fixture
def test_client(mock_cassandra_client, mock_redis_queue, sample_job):
    """Provide a FastAPI TestClient with mocked dependencies."""
    from src.api.app import create_app
    from src.db.repositories.execution_repo import ExecutionRepository
    from src.db.repositories.job_repo import JobRepository
    from src.scheduler.scheduler import Scheduler

    app = create_app()

    # Build dependency instances backed by mocks
    job_repo = JobRepository(mock_cassandra_client)
    exec_repo = ExecutionRepository(mock_cassandra_client)
    scheduler = Scheduler(queue=mock_redis_queue, cassandra_client=mock_cassandra_client)

    # Override app state directly
    app.state.cassandra_client = mock_cassandra_client
    app.state.job_repo = job_repo
    app.state.exec_repo = exec_repo
    app.state.queue = mock_redis_queue
    app.state.scheduler = scheduler

    with TestClient(app, raise_server_exceptions=False) as client:
        yield client
