"""Unit tests for the FastAPI API endpoints."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import fakeredis
import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app
from src.db.repositories.execution_repo import ExecutionRepository
from src.db.repositories.job_repo import JobRepository
from src.models.enums import JobStatus, ScheduleType
from src.models.job import Job
from src.queue.redis_queue import RedisQueue
from src.scheduler.scheduler import Scheduler


@pytest.fixture
def fake_redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def api_client(mock_cassandra_client, fake_redis_client):
    """Create a TestClient with all dependencies mocked."""
    app = create_app()

    queue = RedisQueue(client=fake_redis_client)
    job_repo = JobRepository(mock_cassandra_client)
    exec_repo = ExecutionRepository(mock_cassandra_client)
    scheduler = Scheduler(queue=queue, cassandra_client=mock_cassandra_client)

    app.state.cassandra_client = mock_cassandra_client
    app.state.job_repo = job_repo
    app.state.exec_repo = exec_repo
    app.state.queue = queue
    app.state.scheduler = scheduler

    with TestClient(app, raise_server_exceptions=False) as client:
        yield client, job_repo, exec_repo, scheduler, mock_cassandra_client


def make_sample_job(
    schedule_type: str = "IMMEDIATE",
    schedule_expression=None,
    task_id: str = "log_message",
    user_id: str = "user-123",
) -> Job:
    return Job(
        job_id=uuid4(),
        user_id=user_id,
        task_id=task_id,
        schedule_type=ScheduleType(schedule_type),
        schedule_expression=schedule_expression,
        parameters={"message": "test", "level": "INFO"},
        status=JobStatus.ACTIVE,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


class TestCreateJob:
    def test_create_job_immediate_success(self, api_client):
        client, job_repo, exec_repo, scheduler, cassandra = api_client

        with patch.object(job_repo, "create_job") as mock_create, \
             patch.object(scheduler, "schedule_immediate_job") as mock_sched:

            mock_job = make_sample_job("IMMEDIATE")
            mock_create.return_value = mock_job
            mock_sched.return_value = MagicMock()

            response = client.post(
                "/api/v1/jobs",
                json={
                    "task_id": "log_message",
                    "schedule_type": "IMMEDIATE",
                    "parameters": {"message": "hello", "level": "INFO"},
                },
                headers={"X-User-Id": "user-123"},
            )

        assert response.status_code == 201
        data = response.json()
        assert data["task_id"] == "log_message"
        assert data["schedule_type"] == "IMMEDIATE"

    def test_create_job_cron_success(self, api_client):
        client, job_repo, exec_repo, scheduler, cassandra = api_client

        with patch.object(job_repo, "create_job") as mock_create, \
             patch.object(scheduler, "schedule_cron_job") as mock_sched:

            mock_job = make_sample_job("CRON", "0 * * * *")
            mock_create.return_value = mock_job
            mock_sched.return_value = []

            response = client.post(
                "/api/v1/jobs",
                json={
                    "task_id": "log_message",
                    "schedule_type": "CRON",
                    "schedule_expression": "0 * * * *",
                    "parameters": {"message": "hourly", "level": "INFO"},
                },
                headers={"X-User-Id": "user-123"},
            )

        assert response.status_code == 201
        data = response.json()
        assert data["schedule_expression"] == "0 * * * *"

    def test_create_job_datetime_success(self, api_client):
        client, job_repo, exec_repo, scheduler, cassandra = api_client
        future = (datetime.utcnow() + timedelta(hours=2)).isoformat()

        with patch.object(job_repo, "create_job") as mock_create, \
             patch.object(scheduler, "schedule_datetime_job") as mock_sched:

            mock_job = make_sample_job("DATETIME", future)
            mock_create.return_value = mock_job
            mock_sched.return_value = MagicMock()

            response = client.post(
                "/api/v1/jobs",
                json={
                    "task_id": "log_message",
                    "schedule_type": "DATETIME",
                    "schedule_expression": future,
                    "parameters": {"message": "future", "level": "INFO"},
                },
                headers={"X-User-Id": "user-123"},
            )

        assert response.status_code == 201

    def test_create_job_invalid_cron(self, api_client):
        client, *_ = api_client

        response = client.post(
            "/api/v1/jobs",
            json={
                "task_id": "log_message",
                "schedule_type": "CRON",
                "schedule_expression": "not a valid cron",
            },
            headers={"X-User-Id": "user-123"},
        )

        assert response.status_code == 422

    def test_create_job_past_datetime(self, api_client):
        client, *_ = api_client
        past = (datetime.utcnow() - timedelta(hours=2)).isoformat()

        response = client.post(
            "/api/v1/jobs",
            json={
                "task_id": "log_message",
                "schedule_type": "DATETIME",
                "schedule_expression": past,
            },
            headers={"X-User-Id": "user-123"},
        )

        assert response.status_code == 422

    def test_create_job_missing_task_id(self, api_client):
        client, *_ = api_client

        response = client.post(
            "/api/v1/jobs",
            json={"schedule_type": "IMMEDIATE"},
            headers={"X-User-Id": "user-123"},
        )

        assert response.status_code == 422

    def test_create_job_unregistered_task_id(self, api_client):
        client, *_ = api_client

        response = client.post(
            "/api/v1/jobs",
            json={
                "task_id": "nonexistent_task",
                "schedule_type": "IMMEDIATE",
            },
            headers={"X-User-Id": "user-123"},
        )

        assert response.status_code == 400


class TestGetJobs:
    def test_get_jobs_success(self, api_client):
        client, job_repo, *_ = api_client

        jobs = [make_sample_job() for _ in range(3)]
        with patch.object(job_repo, "get_jobs_by_user", return_value=jobs):
            response = client.get("/api/v1/jobs?user_id=user-123")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert len(data["jobs"]) == 3

    def test_get_jobs_with_filters(self, api_client):
        client, job_repo, *_ = api_client

        active_job = make_sample_job()
        paused_job = make_sample_job()
        paused_job = paused_job.model_copy(update={"status": JobStatus.PAUSED})

        with patch.object(job_repo, "get_jobs_by_user", return_value=[active_job, paused_job]):
            response = client.get("/api/v1/jobs?user_id=user-123&status=ACTIVE")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["jobs"][0]["status"] == "ACTIVE"

    def test_get_jobs_missing_user_id(self, api_client):
        client, *_ = api_client

        response = client.get("/api/v1/jobs")

        assert response.status_code == 400

    def test_get_jobs_pagination(self, api_client):
        client, job_repo, *_ = api_client

        jobs = [make_sample_job() for _ in range(25)]
        with patch.object(job_repo, "get_jobs_by_user", return_value=jobs):
            response = client.get("/api/v1/jobs?user_id=user-123&limit=10&page=1")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 25
        assert len(data["jobs"]) == 10


class TestGetJobById:
    def test_get_job_by_id(self, api_client):
        client, job_repo, exec_repo, *_ = api_client
        job = make_sample_job()

        with patch.object(job_repo, "get_job", return_value=job), \
             patch.object(exec_repo, "get_user_executions", return_value=([], None)):
            response = client.get(f"/api/v1/jobs/{job.job_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["job"]["task_id"] == job.task_id

    def test_get_job_not_found(self, api_client):
        client, job_repo, *_ = api_client
        random_id = uuid4()

        with patch.object(job_repo, "get_job", return_value=None):
            response = client.get(f"/api/v1/jobs/{random_id}")

        assert response.status_code == 404


class TestHealthCheck:
    def test_health_check(self, api_client):
        client, *rest = api_client
        cassandra = rest[3]

        session_mock = MagicMock()
        session_mock.execute.return_value = MagicMock()
        cassandra.get_session.return_value = session_mock

        response = client.get("/api/v1/health")

        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "services" in data
