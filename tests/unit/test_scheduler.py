"""Unit tests for the Scheduler service."""

import calendar
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import fakeredis
import pytest
from freezegun import freeze_time

from src.models.enums import ExecutionStatus, JobStatus, ScheduleType
from src.models.execution import Execution
from src.models.job import Job
from src.queue.messages import JobMessage
from src.queue.redis_queue import RedisQueue
from src.scheduler.scheduler import Scheduler


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def mock_queue(fake_redis):
    return RedisQueue(client=fake_redis)


@pytest.fixture
def mock_cassandra():
    client = MagicMock()
    session = MagicMock()
    client.get_session.return_value = session
    result = MagicMock()
    result.one.return_value = None
    result.__iter__ = MagicMock(return_value=iter([]))
    client.execute.return_value = result
    return client


@pytest.fixture
def mock_job_repo():
    return MagicMock()


@pytest.fixture
def mock_exec_repo():
    repo = MagicMock()
    repo.create_execution.side_effect = lambda e: e
    repo.get_executions_in_range.return_value = []
    return repo


@pytest.fixture
def scheduler(mock_queue, mock_cassandra, mock_job_repo, mock_exec_repo):
    """Scheduler with mocked repositories."""
    sched = Scheduler(queue=mock_queue, cassandra_client=mock_cassandra)
    sched._job_repo = mock_job_repo
    sched._exec_repo = mock_exec_repo
    return sched


def make_job(schedule_type=ScheduleType.IMMEDIATE, schedule_expression=None):
    return Job(
        job_id=uuid4(),
        user_id="test-user",
        task_id="log_message",
        schedule_type=schedule_type,
        schedule_expression=schedule_expression,
        parameters={"message": "test", "level": "INFO"},
        status=JobStatus.ACTIVE,
    )


def make_execution(job, execution_time=None, status=ExecutionStatus.PENDING):
    dt = execution_time or datetime.utcnow()
    exec = Execution.create(job_id=job.job_id, user_id=job.user_id, execution_time=dt)
    exec = exec.model_copy(update={"status": status})
    return exec


class TestCalculateTimeBuckets:
    def test_calculate_time_buckets(self):
        """Time buckets are hourly unix timestamps."""
        now = datetime(2024, 1, 15, 10, 30, 0)
        now_ts = calendar.timegm(now.timetuple())
        # start_bucket should be the hour containing now
        start_bucket = (now_ts // 3600) * 3600
        # Verify it rounds down to the hour
        assert start_bucket == calendar.timegm(datetime(2024, 1, 15, 10, 0, 0).timetuple())


class TestPollAndEnqueue:
    def test_poll_and_enqueue_pending_jobs(self, scheduler, mock_exec_repo, mock_queue):
        """poll_and_enqueue enqueues pending jobs found in the DB."""
        job = make_job()
        execution_time = datetime.utcnow() + timedelta(minutes=2)
        execution = make_execution(job, execution_time=execution_time)

        mock_exec_repo.get_executions_in_range.return_value = [execution]

        scheduler.poll_and_enqueue()

        depth = mock_queue.get_queue_depth()
        assert depth["pending"] == 1

    def test_poll_and_enqueue_no_jobs(self, scheduler, mock_exec_repo, mock_queue):
        """poll_and_enqueue handles empty result gracefully."""
        mock_exec_repo.get_executions_in_range.return_value = []

        scheduler.poll_and_enqueue()

        assert mock_queue.get_queue_depth()["pending"] == 0

    def test_no_double_enqueue(self, scheduler, mock_exec_repo, mock_queue):
        """poll_and_enqueue does not enqueue the same execution twice."""
        job = make_job()
        execution_time = datetime.utcnow() + timedelta(minutes=2)
        execution = make_execution(job, execution_time=execution_time)

        mock_exec_repo.get_executions_in_range.return_value = [execution]

        scheduler.poll_and_enqueue()
        scheduler.poll_and_enqueue()  # Second poll should not duplicate

        assert mock_queue.get_queue_depth()["pending"] == 1


class TestScheduleImmediateJob:
    def test_schedule_immediate_job(self, scheduler, mock_exec_repo, mock_queue):
        """schedule_immediate_job creates an execution and enqueues immediately."""
        job = make_job(schedule_type=ScheduleType.IMMEDIATE)

        execution = scheduler.schedule_immediate_job(job)

        mock_exec_repo.create_execution.assert_called_once()
        assert mock_queue.get_queue_depth()["pending"] == 1
        assert execution is not None
        assert str(execution.job_id) == str(job.job_id)


class TestScheduleDatetimeJob:
    def test_schedule_datetime_job_within_window(self, scheduler, mock_exec_repo, mock_queue):
        """Jobs within the lookahead window are enqueued immediately."""
        future = datetime.utcnow() + timedelta(minutes=2)  # within 6-min lookahead
        job = make_job(
            schedule_type=ScheduleType.DATETIME,
            schedule_expression=future.isoformat(),
        )

        execution = scheduler.schedule_datetime_job(job)

        mock_exec_repo.create_execution.assert_called_once()
        assert mock_queue.get_queue_depth()["pending"] == 1

    def test_schedule_datetime_job_outside_window(self, scheduler, mock_exec_repo, mock_queue):
        """Jobs beyond the lookahead window are persisted but NOT enqueued."""
        far_future = datetime.utcnow() + timedelta(hours=2)  # well beyond 6-min lookahead
        job = make_job(
            schedule_type=ScheduleType.DATETIME,
            schedule_expression=far_future.isoformat(),
        )

        execution = scheduler.schedule_datetime_job(job)

        mock_exec_repo.create_execution.assert_called_once()
        assert mock_queue.get_queue_depth()["pending"] == 0

    def test_schedule_datetime_job_in_past_executes_immediately(
        self, scheduler, mock_exec_repo, mock_queue
    ):
        """Jobs scheduled in the past are clamped to now and enqueued."""
        past = datetime.utcnow() - timedelta(hours=1)
        job = make_job(
            schedule_type=ScheduleType.DATETIME,
            schedule_expression=past.isoformat(),
        )

        execution = scheduler.schedule_datetime_job(job)

        mock_exec_repo.create_execution.assert_called_once()
        assert mock_queue.get_queue_depth()["pending"] == 1


class TestScheduleCronJob:
    def test_schedule_cron_job_generates_executions(self, scheduler, mock_exec_repo):
        """schedule_cron_job pre-computes multiple future executions."""
        job = make_job(
            schedule_type=ScheduleType.CRON,
            schedule_expression="0 * * * *",  # every hour
        )

        executions = scheduler.schedule_cron_job(job)

        # Should generate CRON_LOOKAHEAD_COUNT executions
        assert len(executions) == 10
        assert mock_exec_repo.create_execution.call_count == 10

    def test_schedule_cron_job_invalid_expression(self, scheduler):
        """schedule_cron_job raises ValueError for invalid cron expressions."""
        job = make_job(
            schedule_type=ScheduleType.CRON,
            schedule_expression="invalid cron expression",
        )

        with pytest.raises(ValueError, match="Invalid CRON expression"):
            scheduler.schedule_cron_job(job)

    def test_schedule_cron_job_executions_in_future(self, scheduler, mock_exec_repo):
        """All pre-computed CRON executions are in the future."""
        now = datetime.utcnow()
        job = make_job(
            schedule_type=ScheduleType.CRON,
            schedule_expression="*/15 * * * *",  # every 15 minutes
        )

        executions = scheduler.schedule_cron_job(job)

        for execution in executions:
            assert execution.execution_time > now
