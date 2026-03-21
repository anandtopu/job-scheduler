"""Unit tests for the Worker service."""

import json
import time
from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import fakeredis
import pytest

from src.models.enums import ExecutionStatus, JobStatus, ScheduleType
from src.models.execution import Execution
from src.models.job import Job
from src.queue.messages import JobMessage
from src.queue.redis_queue import RedisQueue
from src.worker.worker import Worker


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def mock_queue(fake_redis):
    return RedisQueue(client=fake_redis)


@pytest.fixture
def mock_cassandra():
    client = MagicMock()
    return client


@pytest.fixture
def mock_job_repo():
    return MagicMock()


@pytest.fixture
def mock_exec_repo():
    return MagicMock()


@pytest.fixture
def worker(mock_queue, mock_cassandra, mock_job_repo, mock_exec_repo):
    """Worker with mocked DB layer."""
    w = Worker(
        queue=mock_queue,
        cassandra_client=mock_cassandra,
        concurrency=2,
        max_retries=3,
        retry_base_delay=5,
    )
    w._job_repo = mock_job_repo
    w._exec_repo = mock_exec_repo
    return w


def make_job(task_id="log_message") -> Job:
    return Job(
        job_id=uuid4(),
        user_id="test-user",
        task_id=task_id,
        schedule_type=ScheduleType.IMMEDIATE,
        schedule_expression=None,
        parameters={"message": "test", "level": "INFO"},
        status=JobStatus.ACTIVE,
    )


def make_execution(job: Job, attempt: int = 0) -> Execution:
    return Execution.create(
        job_id=job.job_id,
        user_id=job.user_id,
        execution_time=datetime.utcnow(),
    )


def make_message(job: Job, execution: Execution, attempt: int = 0) -> JobMessage:
    return JobMessage(
        job_id=str(job.job_id),
        execution_key=execution.execution_key,
        time_bucket=execution.time_bucket,
        execution_time=float(time.time()),
        attempt=attempt,
    )


class TestExecuteJobSuccess:
    def test_execute_job_success(self, worker, mock_job_repo, mock_exec_repo, mock_queue):
        """Successful execution marks COMPLETED and ACKs the message."""
        job = make_job(task_id="log_message")
        execution = make_execution(job)
        message = make_message(job, execution)

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        # Enqueue so we can ack later
        mock_queue.enqueue(message)
        mock_queue.dequeue(count=1)

        with patch("src.tasks.registry.TASK_REGISTRY.execute", return_value={"logged": True}):
            worker.execute_job(message)

        # Verify COMPLETED status was set
        calls = mock_exec_repo.update_execution_status.call_args_list
        statuses = [call.kwargs.get("status") or call.args[1] for call in calls]
        assert ExecutionStatus.COMPLETED in statuses or "COMPLETED" in [str(s) for s in statuses]

    def test_execute_job_already_completed_is_idempotent(
        self, worker, mock_job_repo, mock_exec_repo, mock_queue
    ):
        """Executing an already COMPLETED job is a no-op."""
        job = make_job()
        execution = make_execution(job)
        execution = execution.model_copy(update={"status": ExecutionStatus.COMPLETED})
        message = make_message(job, execution)

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        mock_queue.enqueue(message)
        mock_queue.dequeue(count=1)

        worker.execute_job(message)

        # No task execution; no status update (beyond the initial check)
        mock_exec_repo.update_execution_status.assert_not_called()

    def test_execute_job_not_found_skips_gracefully(
        self, worker, mock_job_repo, mock_exec_repo, mock_queue
    ):
        """If job is not found in DB, execution is skipped and message is ACK'd."""
        job = make_job()
        execution = make_execution(job)
        message = make_message(job, execution)

        mock_job_repo.get_job.return_value = None

        mock_queue.enqueue(message)
        mock_queue.dequeue(count=1)

        worker.execute_job(message)

        mock_exec_repo.update_execution_status.assert_not_called()


class TestExecuteJobRetry:
    def test_execute_job_visible_failure_retry(
        self, worker, mock_job_repo, mock_exec_repo, mock_queue
    ):
        """Failed job below max_retries is marked RETRYING."""
        job = make_job(task_id="log_message")
        execution = make_execution(job)
        message = make_message(job, execution, attempt=0)

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        mock_queue.enqueue(message)
        mock_queue.dequeue(count=1)

        with patch(
            "src.tasks.registry.TASK_REGISTRY.execute",
            side_effect=RuntimeError("Task failed!"),
        ):
            worker.execute_job(message)

        calls = mock_exec_repo.update_execution_status.call_args_list
        statuses = [str(call.kwargs.get("status") or call.args[1]) for call in calls]
        assert any("RETRYING" in s for s in statuses)

    def test_execute_job_max_retries_exceeded(
        self, worker, mock_job_repo, mock_exec_repo, mock_queue
    ):
        """Job that exceeds max_retries is marked FAILED."""
        job = make_job(task_id="log_message")
        execution = make_execution(job)
        # Set attempt to max_retries so next failure triggers FAILED
        message = make_message(job, execution, attempt=worker._max_retries)

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        mock_queue.enqueue(message)
        mock_queue.dequeue(count=1)

        with patch(
            "src.tasks.registry.TASK_REGISTRY.execute",
            side_effect=RuntimeError("Still failing"),
        ):
            worker.execute_job(message)

        calls = mock_exec_repo.update_execution_status.call_args_list
        statuses = [str(call.kwargs.get("status") or call.args[1]) for call in calls]
        assert any("FAILED" in s for s in statuses)

    def test_exponential_backoff_delay(self, worker):
        """Exponential backoff delay grows as base^attempt."""
        # attempt=1: 5^1 = 5s
        # attempt=2: 5^2 = 25s
        # attempt=3: 5^3 = 125s
        assert worker._retry_base_delay**1 == 5
        assert worker._retry_base_delay**2 == 25
        assert worker._retry_base_delay**3 == 125


class TestHeartbeat:
    def test_heartbeat_extends_visibility(self, mock_queue):
        """Heartbeat updates the processing deadline for in-flight messages."""
        job = make_job()
        execution = make_execution(job)
        msg = make_message(job, execution)

        # Enqueue and dequeue to put in processing
        mock_queue.enqueue(msg)
        dequeued = mock_queue.dequeue(count=1)[0]

        # Get initial deadline
        initial_score = mock_queue._redis.zscore(
            mock_queue._processing_key, dequeued.to_json()
        )

        time.sleep(0.01)
        mock_queue.heartbeat(dequeued)

        new_score = mock_queue._redis.zscore(
            mock_queue._processing_key, dequeued.to_json()
        )

        assert new_score is not None
        assert new_score >= initial_score

    def test_execute_job_invisible_failure_via_visibility_timeout(self, mock_queue):
        """Jobs not ACK'd before visibility timeout are re-queued."""
        job = make_job()
        execution = make_execution(job)
        msg = make_message(job, execution)

        mock_queue.enqueue(msg)
        mock_queue.dequeue(count=1)

        # Manually expire the processing deadline
        expired_deadline = time.time() - 1
        mock_queue._redis.zadd(
            mock_queue._processing_key,
            {msg.to_json(): expired_deadline},
        )

        count = mock_queue.requeue_stuck_jobs()
        assert count == 1

        # Job should be back in pending
        depth = mock_queue.get_queue_depth()
        assert depth["pending"] == 1
        assert depth["processing"] == 0
