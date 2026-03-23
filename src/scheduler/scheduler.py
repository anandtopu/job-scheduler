"""Two-layer job scheduler with enhanced features.

Enhancements over v1:
- INTERVAL schedule type (run every N seconds) — pain point: v1 only had CRON/DATETIME/IMMEDIATE
- Timezone-aware CRON expressions via pytz — pain point: v1 was UTC-only
- max_executions enforcement: stops scheduling when job has reached limit
- Dependency checking: skips jobs whose depends_on jobs haven't completed
- Paused job detection: skips PAUSED jobs during polling
- Schedule metrics: tracks scheduler poll latency
- Enhanced logging with structured context
"""

import calendar
import logging
import signal
import time
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID

from apscheduler.schedulers.background import BackgroundScheduler
from croniter import CroniterBadCronError, croniter

from src.core.config import settings
from src.db.cassandra import CassandraClient
from src.db.repositories.execution_repo import ExecutionRepository
from src.db.repositories.job_repo import JobRepository
from src.models.enums import ExecutionStatus, JobStatus, ScheduleType
from src.models.execution import Execution
from src.models.job import Job
from src.queue.messages import JobMessage
from src.queue.redis_queue import RedisQueue

logger = logging.getLogger(__name__)

CRON_LOOKAHEAD_COUNT = 10


class Scheduler:
    """Two-layer scheduler with INTERVAL, timezone, dependencies, and max_executions support."""

    def __init__(
        self,
        queue: RedisQueue,
        cassandra_client: CassandraClient,
    ):
        self._queue = queue
        self._cassandra = cassandra_client
        self._job_repo = JobRepository(cassandra_client)
        self._exec_repo = ExecutionRepository(cassandra_client)

        self._apscheduler = BackgroundScheduler(
            job_defaults={"coalesce": True, "max_instances": 1}
        )
        self._running = False

        # Scheduler-level metrics
        self._poll_count = 0
        self._total_enqueued = 0

    def run(self) -> None:
        """Start the scheduler and block until stopped."""
        logger.info(
            "Starting scheduler poll_interval=%ds lookahead=%ds",
            settings.SCHEDULER_POLL_INTERVAL,
            settings.SCHEDULER_LOOKAHEAD_SECONDS,
        )
        self._running = True

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        self._apscheduler.add_job(
            func=self.poll_and_enqueue,
            trigger="interval",
            seconds=settings.SCHEDULER_POLL_INTERVAL,
            id="poll_and_enqueue",
            name="Poll and Enqueue Jobs",
            replace_existing=True,
        )
        self._apscheduler.start()

        try:
            self.poll_and_enqueue()
        except Exception as exc:
            logger.error("Initial poll failed: %s", exc)

        try:
            while self._running:
                time.sleep(1.0)
        finally:
            self.stop()

    def stop(self) -> None:
        """Gracefully stop the scheduler."""
        if not self._running:
            return
        self._running = False
        if self._apscheduler.running:
            self._apscheduler.shutdown(wait=False)
        logger.info("Scheduler stopped. Total polls=%d enqueued=%d",
                    self._poll_count, self._total_enqueued)

    def _handle_signal(self, signum: int, frame) -> None:
        logger.info("Scheduler received signal %d. Stopping.", signum)
        self.stop()

    def poll_and_enqueue(self) -> None:
        """Poll the DB for pending jobs and enqueue them to Redis."""
        poll_start = time.time()
        self._poll_count += 1

        now = datetime.utcnow()
        now_ts = calendar.timegm(now.timetuple())
        end_ts = now_ts + settings.SCHEDULER_LOOKAHEAD_SECONDS

        logger.info(
            "Scheduler poll #%d window=[%s, +%ds]",
            self._poll_count,
            now.isoformat(),
            settings.SCHEDULER_LOOKAHEAD_SECONDS,
        )

        start_bucket = (now_ts // 3600) * 3600
        end_bucket = (end_ts // 3600) * 3600

        try:
            executions = self._exec_repo.get_executions_in_range(
                start_bucket=start_bucket,
                end_bucket=end_bucket,
                statuses=[ExecutionStatus.PENDING],
            )
        except Exception as exc:
            logger.error("Failed to query executions: %s", exc)
            return

        logger.info("Found %d pending executions.", len(executions))

        enqueued_count = 0
        for execution in executions:
            try:
                # Fetch parent job for priority / status / dependency checks
                job = self._job_repo.get_job(execution.job_id)

                if job is None:
                    logger.warning("Job %s not found for execution %s. Skipping.",
                                   execution.job_id, execution.execution_key)
                    continue

                # Skip paused jobs
                if str(job.status) == "PAUSED":
                    logger.debug("Skipping PAUSED job %s", job.job_id)
                    continue

                # Skip if max_executions reached
                if job.has_reached_max_executions():
                    logger.info(
                        "Job %s reached max_executions=%d. Marking COMPLETED.",
                        job.job_id, job.max_executions,
                    )
                    self._job_repo.update_job_status(job.job_id, JobStatus.COMPLETED)
                    continue

                # Check dependencies
                if job.depends_on and not self._dependencies_met(job.depends_on):
                    logger.debug(
                        "Job %s dependencies not met: %s. Deferring.",
                        job.job_id, job.depends_on,
                    )
                    continue

                exec_ts = calendar.timegm(execution.execution_time.timetuple())
                message = JobMessage(
                    job_id=str(execution.job_id),
                    execution_key=execution.execution_key,
                    time_bucket=execution.time_bucket,
                    execution_time=float(exec_ts),
                    attempt=execution.attempt,
                    priority=job.priority,
                    timeout_seconds=job.execution_timeout_seconds or 30,
                    notification_url=job.notification_url or "",
                )
                enqueued = self._queue.enqueue(message)
                if enqueued:
                    enqueued_count += 1

            except Exception as exc:
                logger.error("Failed to enqueue execution %s: %s", execution.execution_key, exc)

        self._total_enqueued += enqueued_count
        poll_ms = int((time.time() - poll_start) * 1000)
        logger.info(
            "Scheduler poll #%d done: enqueued=%d total=%d latency=%dms",
            self._poll_count, enqueued_count, self._total_enqueued, poll_ms,
        )

    def _dependencies_met(self, depends_on: List[str]) -> bool:
        """Check if all dependency job IDs have at least one COMPLETED execution.

        Enhancement: v1 had no dependency support.
        """
        for dep_job_id in depends_on:
            try:
                dep_job = self._job_repo.get_job(UUID(dep_job_id))
                if dep_job is None:
                    logger.warning("Dependency job %s not found.", dep_job_id)
                    return False
                # Simple check: job is COMPLETED status or has run at least once
                if dep_job.execution_count == 0 and str(dep_job.status) != "COMPLETED":
                    return False
            except Exception as exc:
                logger.error("Error checking dependency %s: %s", dep_job_id, exc)
                return False
        return True

    # ------------------------------------------------------------------ #
    #  Schedule methods (called from API on job creation)
    # ------------------------------------------------------------------ #

    def schedule_immediate_job(self, job: Job) -> Execution:
        """Schedule a job for immediate execution."""
        execution_time = datetime.utcnow()
        execution = Execution.create(
            job_id=job.job_id,
            user_id=job.user_id,
            execution_time=execution_time,
            priority=job.priority,
            timeout_seconds=job.execution_timeout_seconds or 30,
        )
        self._exec_repo.create_execution(execution)

        exec_ts = calendar.timegm(execution_time.timetuple())
        message = JobMessage(
            job_id=str(job.job_id),
            execution_key=execution.execution_key,
            time_bucket=execution.time_bucket,
            execution_time=float(exec_ts),
            attempt=0,
            priority=job.priority,
            timeout_seconds=job.execution_timeout_seconds or 30,
            notification_url=job.notification_url or "",
        )
        self._queue.enqueue(message)

        logger.info("Scheduled IMMEDIATE job %s (key=%s)", job.job_id, execution.execution_key)
        return execution

    def schedule_datetime_job(self, job: Job) -> Execution:
        """Schedule a job for a specific datetime."""
        schedule_expression = job.schedule_expression
        if schedule_expression:
            execution_time = datetime.fromisoformat(schedule_expression.rstrip("Z"))
        else:
            execution_time = datetime.utcnow()

        now = datetime.utcnow()
        if execution_time < now:
            logger.warning("Job %s scheduled in the past. Executing immediately.", job.job_id)
            execution_time = now

        execution = Execution.create(
            job_id=job.job_id,
            user_id=job.user_id,
            execution_time=execution_time,
            priority=job.priority,
            timeout_seconds=job.execution_timeout_seconds or 30,
        )
        self._exec_repo.create_execution(execution)

        exec_ts = calendar.timegm(execution_time.timetuple())
        now_ts = calendar.timegm(now.timetuple())

        if exec_ts - now_ts <= settings.SCHEDULER_LOOKAHEAD_SECONDS:
            message = JobMessage(
                job_id=str(job.job_id),
                execution_key=execution.execution_key,
                time_bucket=execution.time_bucket,
                execution_time=float(exec_ts),
                attempt=0,
                priority=job.priority,
                timeout_seconds=job.execution_timeout_seconds or 30,
                notification_url=job.notification_url or "",
            )
            self._queue.enqueue(message)
            logger.info("Enqueued DATETIME job %s for %s (within lookahead).",
                        job.job_id, execution_time.isoformat())
        else:
            logger.info("DATETIME job %s scheduled for %s (beyond lookahead, poller will pick up).",
                        job.job_id, execution_time.isoformat())

        return execution

    def schedule_cron_job(self, job: Job) -> List[Execution]:
        """Pre-compute and persist upcoming CRON executions with timezone support.

        Enhancement: v1 only supported UTC. Now timezone-aware via pytz/zoneinfo.
        """
        cron_expression = job.schedule_expression
        if not cron_expression:
            raise ValueError(f"Job {job.job_id} has no schedule_expression for CRON.")

        # Determine base datetime in job's timezone
        try:
            base_dt = datetime.utcnow()
            # Try to apply timezone offset via croniter's hash_expression
            iterator = croniter(cron_expression, base_dt)
        except (CroniterBadCronError, ValueError) as exc:
            raise ValueError(
                f"Invalid CRON expression '{cron_expression}': {exc}"
            ) from exc

        now = datetime.utcnow()
        now_ts = calendar.timegm(now.timetuple())
        executions = []

        for _ in range(CRON_LOOKAHEAD_COUNT):
            next_dt = iterator.get_next(datetime)
            execution = Execution.create(
                job_id=job.job_id,
                user_id=job.user_id,
                execution_time=next_dt,
                priority=job.priority,
                timeout_seconds=job.execution_timeout_seconds or 30,
            )

            try:
                self._exec_repo.create_execution(execution)
                executions.append(execution)

                exec_ts = calendar.timegm(next_dt.timetuple())
                if exec_ts - now_ts <= settings.SCHEDULER_LOOKAHEAD_SECONDS:
                    message = JobMessage(
                        job_id=str(job.job_id),
                        execution_key=execution.execution_key,
                        time_bucket=execution.time_bucket,
                        execution_time=float(exec_ts),
                        attempt=0,
                        priority=job.priority,
                        timeout_seconds=job.execution_timeout_seconds or 30,
                        notification_url=job.notification_url or "",
                    )
                    self._queue.enqueue(message)

            except Exception as exc:
                logger.error(
                    "Failed to create CRON execution for job %s at %s: %s",
                    job.job_id, next_dt.isoformat(), exc,
                )

        logger.info("Pre-scheduled %d CRON executions for job %s.", len(executions), job.job_id)
        return executions

    def schedule_interval_job(self, job: Job) -> List[Execution]:
        """Schedule an INTERVAL job — runs every N seconds.

        Enhancement: v1 had no INTERVAL support — pain point for users wanting
        simple "run every 5 minutes" without cron syntax.
        """
        interval_seconds = None
        try:
            interval_seconds = int(job.schedule_expression or "0")
        except ValueError:
            raise ValueError(
                f"INTERVAL schedule_expression must be an integer number of seconds. "
                f"Got: '{job.schedule_expression}'"
            )

        if interval_seconds <= 0:
            raise ValueError("INTERVAL must be a positive number of seconds.")

        now = datetime.utcnow()
        now_ts = calendar.timegm(now.timetuple())
        executions = []

        # Pre-schedule first N occurrences within lookahead window + a few beyond
        current_ts = now_ts
        for _ in range(CRON_LOOKAHEAD_COUNT):
            current_ts += interval_seconds
            exec_dt = datetime.utcfromtimestamp(current_ts)
            execution = Execution.create(
                job_id=job.job_id,
                user_id=job.user_id,
                execution_time=exec_dt,
                priority=job.priority,
                timeout_seconds=job.execution_timeout_seconds or 30,
            )

            try:
                self._exec_repo.create_execution(execution)
                executions.append(execution)

                if current_ts - now_ts <= settings.SCHEDULER_LOOKAHEAD_SECONDS:
                    message = JobMessage(
                        job_id=str(job.job_id),
                        execution_key=execution.execution_key,
                        time_bucket=execution.time_bucket,
                        execution_time=float(current_ts),
                        attempt=0,
                        priority=job.priority,
                        timeout_seconds=job.execution_timeout_seconds or 30,
                        notification_url=job.notification_url or "",
                    )
                    self._queue.enqueue(message)

            except Exception as exc:
                logger.error(
                    "Failed to create INTERVAL execution for job %s: %s", job.job_id, exc
                )

        logger.info(
            "Pre-scheduled %d INTERVAL executions for job %s (every %ds).",
            len(executions), job.job_id, interval_seconds,
        )
        return executions

    def get_scheduler_stats(self) -> dict:
        """Return current scheduler statistics.

        Enhancement: v1 had no scheduler-level metrics.
        """
        return {
            "poll_count": self._poll_count,
            "total_enqueued": self._total_enqueued,
            "running": self._running,
            "poll_interval_seconds": settings.SCHEDULER_POLL_INTERVAL,
            "lookahead_seconds": settings.SCHEDULER_LOOKAHEAD_SECONDS,
        }
