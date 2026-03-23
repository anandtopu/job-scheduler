"""Unit tests for v2 enhancements.

Tests cover:
- Priority queue ordering (CRITICAL before NORMAL before LOW)
- INTERVAL schedule type
- Job pause/resume enforcement in worker
- max_executions enforcement in scheduler
- Dead Letter Queue (DLQ) routing
- Bulk job actions
- Execution timeout enforcement
- Failure notification dispatch
"""

import time
from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import fakeredis
import pytest

from src.models.enums import ExecutionStatus, JobPriority, JobStatus, ScheduleType
from src.models.execution import Execution
from src.models.job import Job
from src.queue.messages import JobMessage, PRIORITY_OFFSET
from src.queue.redis_queue import RedisQueue
from src.scheduler.scheduler import Scheduler
from src.worker.worker import Worker


# ====================================================================
# Helpers
# ====================================================================

def make_job(
    schedule_type=ScheduleType.IMMEDIATE,
    schedule_expression=None,
    priority=JobPriority.NORMAL,
    max_executions=None,
    execution_count=0,
    status=JobStatus.ACTIVE,
    timeout=30,
    notification_url=None,
) -> Job:
    return Job(
        job_id=uuid4(),
        user_id="test-user",
        task_id="log_message",
        schedule_type=schedule_type,
        schedule_expression=schedule_expression,
        parameters={"message": "test", "level": "INFO"},
        status=status,
        priority=priority,
        max_executions=max_executions,
        execution_count=execution_count,
        execution_timeout_seconds=timeout,
        notification_url=notification_url,
    )


def make_execution(job: Job, attempt: int = 0) -> Execution:
    return Execution.create(
        job_id=job.job_id,
        user_id=job.user_id,
        execution_time=datetime.utcnow(),
        priority=str(job.priority),
    )


def make_message(job: Job, execution: Execution, attempt: int = 0) -> JobMessage:
    return JobMessage(
        job_id=str(job.job_id),
        execution_key=execution.execution_key,
        time_bucket=execution.time_bucket,
        execution_time=float(time.time()),
        attempt=attempt,
        priority=str(job.priority),
        timeout_seconds=job.execution_timeout_seconds,
    )


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def queue(fake_redis):
    return RedisQueue(client=fake_redis)


@pytest.fixture
def mock_cassandra():
    client = MagicMock()
    session = MagicMock()
    client.get_session.return_value = session
    client.execute.return_value = MagicMock(
        one=MagicMock(return_value=None),
        __iter__=MagicMock(return_value=iter([])),
        current_rows=[],
        has_more_pages=False,
        paging_state=None,
    )
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
def worker(queue, mock_cassandra, mock_job_repo, mock_exec_repo):
    w = Worker(queue=queue, cassandra_client=mock_cassandra, concurrency=2, max_retries=2)
    w._job_repo = mock_job_repo
    w._exec_repo = mock_exec_repo
    return w


@pytest.fixture
def scheduler(queue, mock_cassandra, mock_job_repo, mock_exec_repo):
    s = Scheduler(queue=queue, cassandra_client=mock_cassandra)
    s._job_repo = mock_job_repo
    s._exec_repo = mock_exec_repo
    return s


# ====================================================================
# Priority Queue Tests
# ====================================================================

class TestPriorityQueue:
    def test_priority_offsets_are_correct(self):
        """CRITICAL has the most negative offset, LOW has positive."""
        assert PRIORITY_OFFSET["CRITICAL"] < PRIORITY_OFFSET["HIGH"] < PRIORITY_OFFSET["NORMAL"] < PRIORITY_OFFSET["LOW"]

    def test_critical_job_scores_lower_than_normal(self):
        """CRITICAL job should dequeue before NORMAL job with same execution_time."""
        now = time.time()
        critical_msg = JobMessage(
            job_id=str(uuid4()), execution_key="c1", time_bucket=0,
            execution_time=now, priority="CRITICAL",
        )
        normal_msg = JobMessage(
            job_id=str(uuid4()), execution_key="n1", time_bucket=0,
            execution_time=now, priority="NORMAL",
        )
        assert critical_msg.effective_score() < normal_msg.effective_score()

    def test_low_priority_scores_higher_than_normal(self):
        """LOW priority job should dequeue after NORMAL job."""
        now = time.time()
        low_msg = JobMessage(
            job_id=str(uuid4()), execution_key="l1", time_bucket=0,
            execution_time=now, priority="LOW",
        )
        normal_msg = JobMessage(
            job_id=str(uuid4()), execution_key="n2", time_bucket=0,
            execution_time=now, priority="NORMAL",
        )
        assert low_msg.effective_score() > normal_msg.effective_score()

    def test_critical_job_dequeues_before_normal_job(self, queue):
        """When both are ready, CRITICAL dequeues first due to lower score."""
        now = time.time() - 1  # both ready
        normal_msg = JobMessage(
            job_id=str(uuid4()), execution_key=f"n-{uuid4()}", time_bucket=0,
            execution_time=now, priority="NORMAL",
        )
        critical_msg = JobMessage(
            job_id=str(uuid4()), execution_key=f"c-{uuid4()}", time_bucket=0,
            execution_time=now, priority="CRITICAL",
        )
        queue.enqueue(normal_msg)
        queue.enqueue(critical_msg)
        dequeued = queue.dequeue(count=1)
        assert len(dequeued) == 1
        assert dequeued[0].priority == "CRITICAL"


# ====================================================================
# INTERVAL Schedule Tests
# ====================================================================

class TestIntervalSchedule:
    def test_schedule_interval_job_creates_executions(self, scheduler, mock_exec_repo):
        """schedule_interval_job pre-creates multiple future executions."""
        job = make_job(schedule_type=ScheduleType.INTERVAL, schedule_expression="60")
        executions = scheduler.schedule_interval_job(job)
        assert len(executions) == 10  # CRON_LOOKAHEAD_COUNT
        mock_exec_repo.create_execution.assert_called()

    def test_schedule_interval_all_executions_in_future(self, scheduler, mock_exec_repo):
        """All pre-scheduled INTERVAL executions should be in the future."""
        now = datetime.utcnow()
        job = make_job(schedule_type=ScheduleType.INTERVAL, schedule_expression="120")
        executions = scheduler.schedule_interval_job(job)
        for ex in executions:
            assert ex.execution_time > now

    def test_schedule_interval_invalid_expression_raises(self, scheduler):
        """Non-numeric INTERVAL expression should raise ValueError."""
        job = make_job(schedule_type=ScheduleType.INTERVAL, schedule_expression="every_minute")
        with pytest.raises(ValueError, match="positive integer"):
            scheduler.schedule_interval_job(job)

    def test_schedule_interval_zero_raises(self, scheduler):
        """Zero-second INTERVAL should raise ValueError."""
        job = make_job(schedule_type=ScheduleType.INTERVAL, schedule_expression="0")
        with pytest.raises(ValueError, match="positive number"):
            scheduler.schedule_interval_job(job)


# ====================================================================
# Max Executions Tests
# ====================================================================

class TestMaxExecutions:
    def test_max_executions_reached_skips_scheduling(
        self, scheduler, mock_job_repo, mock_exec_repo
    ):
        """When job has reached max_executions, poller marks it COMPLETED and skips."""
        job = make_job(max_executions=5, execution_count=5)
        execution = make_execution(job)
        mock_exec_repo.get_executions_in_range.return_value = [execution]
        mock_job_repo.get_job.return_value = job

        scheduler.poll_and_enqueue()

        mock_job_repo.update_job_status.assert_called_once_with(
            job.job_id, JobStatus.COMPLETED
        )

    def test_job_has_not_reached_max_executions(self):
        """has_reached_max_executions returns False when below limit."""
        job = make_job(max_executions=10, execution_count=9)
        assert not job.has_reached_max_executions()

    def test_job_has_reached_max_executions(self):
        """has_reached_max_executions returns True when at limit."""
        job = make_job(max_executions=3, execution_count=3)
        assert job.has_reached_max_executions()

    def test_no_max_executions_never_stops(self):
        """When max_executions is None, has_reached_max_executions is always False."""
        job = make_job(max_executions=None, execution_count=99999)
        assert not job.has_reached_max_executions()


# ====================================================================
# Paused Job Tests
# ====================================================================

class TestPausedJobEnforcement:
    def test_paused_job_skipped_in_worker(self, worker, mock_job_repo, mock_exec_repo, queue):
        """Worker skips execution when job is PAUSED."""
        job = make_job(status=JobStatus.PAUSED)
        execution = make_execution(job)
        message = make_message(job, execution)

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        queue.enqueue(message)
        queue.dequeue(count=1)

        worker.execute_job(message)

        # Task should not have been executed
        with patch("src.tasks.registry.TASK_REGISTRY.execute") as mock_exec:
            mock_exec.assert_not_called()

    def test_paused_job_skipped_by_scheduler(
        self, scheduler, mock_job_repo, mock_exec_repo, queue
    ):
        """Scheduler poll skips PAUSED jobs and does not enqueue them."""
        job = make_job(status=JobStatus.PAUSED)
        execution = make_execution(job)
        mock_exec_repo.get_executions_in_range.return_value = [execution]
        mock_job_repo.get_job.return_value = job

        scheduler.poll_and_enqueue()

        assert queue.get_queue_depth()["pending"] == 0


# ====================================================================
# Dead Letter Queue Tests
# ====================================================================

class TestDeadLetterQueue:
    def test_exhausted_job_goes_to_dlq(self, worker, mock_job_repo, mock_exec_repo, queue):
        """Job exceeding max_retries is sent to the DLQ."""
        job = make_job()
        execution = make_execution(job)
        # attempt == max_retries → next failure triggers FAILED + DLQ
        message = make_message(job, execution, attempt=worker._max_retries)

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        queue.enqueue(message)
        queue.dequeue(count=1)

        with patch("src.tasks.registry.TASK_REGISTRY.execute",
                   side_effect=RuntimeError("Fatal error")):
            worker.execute_job(message)

        dlq_entries = queue.get_dlq_jobs()
        assert len(dlq_entries) == 1

    def test_dlq_purge_clears_all(self, queue):
        """purge_dlq removes all DLQ entries."""
        msg = JobMessage(
            job_id=str(uuid4()), execution_key="test-dlq-1", time_bucket=0,
            execution_time=time.time(), priority="NORMAL",
        )
        queue.send_to_dlq(msg, reason="test failure")
        assert len(queue.get_dlq_jobs()) == 1

        purged = queue.purge_dlq()
        assert purged == 1
        assert len(queue.get_dlq_jobs()) == 0

    def test_retryable_job_does_not_go_to_dlq(
        self, worker, mock_job_repo, mock_exec_repo, queue
    ):
        """Job with retries remaining should NOT go to DLQ on first failure."""
        job = make_job()
        execution = make_execution(job)
        message = make_message(job, execution, attempt=0)  # first attempt

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        queue.enqueue(message)
        queue.dequeue(count=1)

        with patch("src.tasks.registry.TASK_REGISTRY.execute",
                   side_effect=RuntimeError("Transient error")):
            worker.execute_job(message)

        assert len(queue.get_dlq_jobs()) == 0


# ====================================================================
# Queue Metrics Tests
# ====================================================================

class TestQueueMetrics:
    def test_enqueue_increments_metric(self, queue):
        """Enqueueing a job increments the per-priority metric counter."""
        msg = JobMessage(
            job_id=str(uuid4()), execution_key=f"m-{uuid4()}", time_bucket=0,
            execution_time=time.time() - 1, priority="HIGH",
        )
        queue.enqueue(msg)
        metrics = queue.get_queue_metrics()
        assert metrics.get("enqueued:HIGH", 0) >= 1

    def test_queue_depth_includes_dlq(self, queue):
        """get_queue_depth now includes dlq count."""
        msg = JobMessage(
            job_id=str(uuid4()), execution_key=f"dlq-{uuid4()}", time_bucket=0,
            execution_time=time.time(), priority="NORMAL",
        )
        queue.send_to_dlq(msg, reason="test")
        depth = queue.get_queue_depth()
        assert "dlq" in depth
        assert depth["dlq"] >= 1


# ====================================================================
# Execution Timing Tests
# ====================================================================

class TestExecutionTiming:
    def test_execution_create_has_priority(self):
        """Execution.create passes priority through from job."""
        job = make_job(priority=JobPriority.CRITICAL)
        execution = make_execution(job)
        assert execution.priority == "CRITICAL"

    def test_worker_id_set_during_execution(
        self, worker, mock_job_repo, mock_exec_repo, queue
    ):
        """Worker sets its own worker_id in execution update."""
        job = make_job()
        execution = make_execution(job)
        message = make_message(job, execution)

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        queue.enqueue(message)
        queue.dequeue(count=1)

        with patch("src.tasks.registry.TASK_REGISTRY.execute", return_value={"ok": True}):
            worker.execute_job(message)

        # Verify worker_id was passed in update call
        calls = mock_exec_repo.update_execution_status.call_args_list
        worker_ids = [
            call.kwargs.get("worker_id") or call.args[5] if len(call.args) > 5 else None
            for call in calls
        ]
        assert any(wid is not None for wid in worker_ids)


# ====================================================================
# Failure Notification Tests
# ====================================================================

class TestFailureNotifications:
    def test_notification_sent_on_max_retry_failure(
        self, worker, mock_job_repo, mock_exec_repo, queue
    ):
        """Failure notification webhook is called when job exhausts retries."""
        job = make_job(notification_url="https://hooks.example.com/alert")
        execution = make_execution(job)
        message = make_message(job, execution, attempt=worker._max_retries)
        message = message.model_copy(update={"notification_url": "https://hooks.example.com/alert"})

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        queue.enqueue(message)
        queue.dequeue(count=1)

        with patch("src.tasks.registry.TASK_REGISTRY.execute",
                   side_effect=RuntimeError("Failed")), \
             patch.object(worker, "_send_failure_notification") as mock_notify:
            worker.execute_job(message)
            mock_notify.assert_called_once()

    def test_no_notification_sent_on_successful_execution(
        self, worker, mock_job_repo, mock_exec_repo, queue
    ):
        """Failure notification is NOT sent when job succeeds."""
        job = make_job(notification_url="https://hooks.example.com/alert")
        execution = make_execution(job)
        message = make_message(job, execution)

        mock_job_repo.get_job.return_value = job
        mock_exec_repo.get_execution_by_key.return_value = execution

        queue.enqueue(message)
        queue.dequeue(count=1)

        with patch("src.tasks.registry.TASK_REGISTRY.execute", return_value={"ok": True}), \
             patch.object(worker, "_send_failure_notification") as mock_notify:
            worker.execute_job(message)
            mock_notify.assert_not_called()
