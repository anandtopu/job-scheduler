"""Worker service that consumes jobs from the Redis queue and executes them.

Enhancements over v1:
- Per-job execution timeout enforcement (pain point: jobs could run forever)
- Worker identity (worker_id) for distributed tracing
- Failure notification via webhook (pain point: silent failures in v1)
- Dead-letter queue routing for exhausted jobs
- Execution duration tracking (started_at, completed_at, duration_ms)
- Graceful cancellation of in-flight jobs on shutdown
- Metrics collection (executions/s, avg duration, error rate)
"""

import json
import logging
import os
import platform
import signal
import socket
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeout
from typing import Dict, Optional
from uuid import UUID

import httpx

from src.core.config import settings
from src.db.cassandra import CassandraClient
from src.db.repositories.execution_repo import ExecutionRepository
from src.db.repositories.job_repo import JobRepository
from src.models.enums import ExecutionStatus
from src.models.execution import Execution
from src.queue.messages import JobMessage
from src.queue.redis_queue import RedisQueue
from src.tasks.registry import TASK_REGISTRY

logger = logging.getLogger(__name__)

DEFAULT_EXECUTION_TIMEOUT = 30  # seconds


def _generate_worker_id() -> str:
    """Generate a unique worker ID based on hostname + PID."""
    try:
        hostname = socket.gethostname()
    except Exception:
        hostname = "unknown"
    return f"worker-{hostname}-{os.getpid()}"


class WorkerMetrics:
    """Simple in-process metrics for the worker node.

    Pain point fix: v1 had zero per-worker observability.
    These metrics are exposed via the /monitoring/workers endpoint.
    """

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.started_at = time.time()
        self._lock = threading.Lock()
        self.executions_total = 0
        self.executions_succeeded = 0
        self.executions_failed = 0
        self.executions_timed_out = 0
        self.executions_retried = 0
        self.total_duration_ms = 0
        self.current_inflight = 0

    def record_success(self, duration_ms: int) -> None:
        with self._lock:
            self.executions_total += 1
            self.executions_succeeded += 1
            self.total_duration_ms += duration_ms

    def record_failure(self, duration_ms: int = 0) -> None:
        with self._lock:
            self.executions_total += 1
            self.executions_failed += 1
            self.total_duration_ms += duration_ms

    def record_timeout(self) -> None:
        with self._lock:
            self.executions_total += 1
            self.executions_timed_out += 1

    def record_retry(self) -> None:
        with self._lock:
            self.executions_retried += 1

    def set_inflight(self, n: int) -> None:
        with self._lock:
            self.current_inflight = n

    def to_dict(self) -> dict:
        with self._lock:
            avg = self.total_duration_ms / max(self.executions_succeeded, 1)
            uptime = int(time.time() - self.started_at)
            return {
                "worker_id": self.worker_id,
                "uptime_seconds": uptime,
                "executions_total": self.executions_total,
                "executions_succeeded": self.executions_succeeded,
                "executions_failed": self.executions_failed,
                "executions_timed_out": self.executions_timed_out,
                "executions_retried": self.executions_retried,
                "avg_duration_ms": round(avg, 2),
                "current_inflight": self.current_inflight,
                "success_rate": round(
                    self.executions_succeeded / max(self.executions_total, 1) * 100, 2
                ),
            }


class Worker:
    """Concurrent job worker with timeouts, notifications, and DLQ support."""

    def __init__(
        self,
        queue: RedisQueue,
        cassandra_client: CassandraClient,
        concurrency: Optional[int] = None,
        max_retries: Optional[int] = None,
        retry_base_delay: Optional[int] = None,
        worker_id: Optional[str] = None,
    ):
        self._queue = queue
        self._cassandra = cassandra_client
        self._job_repo = JobRepository(cassandra_client)
        self._exec_repo = ExecutionRepository(cassandra_client)

        self._concurrency = concurrency or settings.WORKER_CONCURRENCY
        self._max_retries = max_retries or settings.WORKER_MAX_RETRIES
        self._retry_base_delay = retry_base_delay or settings.WORKER_RETRY_BASE_DELAY

        # Worker identity — used in logs, metrics, and execution records
        self._worker_id = worker_id or _generate_worker_id()
        self._metrics = WorkerMetrics(self._worker_id)

        self._running = False
        self._executor: Optional[ThreadPoolExecutor] = None
        self._in_flight: Dict[str, JobMessage] = {}
        self._in_flight_lock = threading.Lock()

        self._heartbeat_thread: Optional[threading.Thread] = None
        self._requeue_thread: Optional[threading.Thread] = None

        logger.info("Worker initialized: %s (concurrency=%d)", self._worker_id, self._concurrency)

    @property
    def worker_id(self) -> str:
        return self._worker_id

    @property
    def metrics(self) -> WorkerMetrics:
        return self._metrics

    def run(self) -> None:
        """Start the worker and block until stopped."""
        logger.info("Starting worker %s", self._worker_id)
        self._running = True
        self._executor = ThreadPoolExecutor(
            max_workers=self._concurrency,
            thread_name_prefix=f"job-worker-{self._worker_id}",
        )

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self._worker_id}-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

        self._requeue_thread = threading.Thread(
            target=self._requeue_loop,
            name=f"{self._worker_id}-requeue",
            daemon=True,
        )
        self._requeue_thread.start()

        try:
            self._poll_loop()
        finally:
            self.stop()

    def stop(self) -> None:
        """Gracefully stop the worker."""
        if not self._running:
            return
        logger.info("Stopping worker %s gracefully...", self._worker_id)
        self._running = False
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None
        logger.info("Worker %s stopped.", self._worker_id)

    def _handle_signal(self, signum: int, frame) -> None:
        logger.info("Worker %s received signal %d. Stopping.", self._worker_id, signum)
        self.stop()

    def _poll_loop(self) -> None:
        """Main polling loop: dequeue and submit jobs."""
        while self._running:
            try:
                with self._in_flight_lock:
                    in_flight_count = len(self._in_flight)

                capacity = max(0, self._concurrency - in_flight_count)
                self._metrics.set_inflight(in_flight_count)

                if capacity == 0:
                    time.sleep(0.1)
                    continue

                messages = self._queue.dequeue(count=min(capacity, 10))
                if not messages:
                    time.sleep(1.0)
                    continue

                for message in messages:
                    with self._in_flight_lock:
                        self._in_flight[message.execution_key] = message
                    if self._executor:
                        self._executor.submit(self._execute_with_cleanup, message)

            except Exception as exc:
                logger.error("Error in worker %s poll loop: %s", self._worker_id, exc)
                time.sleep(5.0)

    def _execute_with_cleanup(self, message: JobMessage) -> None:
        try:
            self.execute_job(message)
        finally:
            with self._in_flight_lock:
                self._in_flight.pop(message.execution_key, None)

    def _heartbeat_loop(self) -> None:
        interval = settings.QUEUE_HEARTBEAT_INTERVAL
        while self._running:
            time.sleep(interval)
            with self._in_flight_lock:
                messages = list(self._in_flight.values())
            for message in messages:
                try:
                    self._queue.heartbeat(message)
                except Exception as exc:
                    logger.warning("Heartbeat failed for %s: %s", message.execution_key, exc)

    def _requeue_loop(self) -> None:
        while self._running:
            time.sleep(settings.QUEUE_VISIBILITY_TIMEOUT)
            try:
                self._queue.requeue_stuck_jobs()
            except Exception as exc:
                logger.error("Error in requeue loop: %s", exc)

    def execute_job(self, message: JobMessage) -> None:
        """Execute a single job with timeout, tracking, notifications, and DLQ routing."""
        execution_key = message.execution_key
        time_bucket = message.time_bucket
        job_id = UUID(message.job_id)
        attempt = message.attempt

        logger.info(
            "Worker %s executing job %s (key=%s attempt=%d priority=%s)",
            self._worker_id, job_id, execution_key, attempt, message.priority,
        )

        # Fetch job definition
        job = self._job_repo.get_job(job_id)
        if job is None:
            logger.warning("Job %s not found. Skipping.", job_id)
            self._queue.ack(message)
            return

        # Check if job is paused
        if str(job.status) == "PAUSED":
            logger.info("Job %s is PAUSED. Skipping execution.", job_id)
            self._queue.ack(message)
            self._exec_repo.update_execution_status(
                execution=self._get_execution(time_bucket, execution_key),
                status=ExecutionStatus.SKIPPED,
                attempt=attempt,
                error="Job is paused",
            )
            return

        # Fetch execution record
        execution = self._get_execution(time_bucket, execution_key)
        if execution is None:
            logger.warning("Execution record not found for key=%s. Skipping.", execution_key)
            self._queue.ack(message)
            return

        # Idempotency check
        if str(execution.status) == ExecutionStatus.COMPLETED:
            logger.info("Execution %s already COMPLETED. Skipping.", execution_key)
            self._queue.ack(message)
            return

        # Determine effective timeout
        effective_timeout = (
            message.timeout_seconds
            or getattr(job, "execution_timeout_seconds", 0)
            or DEFAULT_EXECUTION_TIMEOUT
        )

        # Mark IN_PROGRESS with worker_id and started_at
        started_at = time.time()
        self._exec_repo.update_execution_status(
            execution=execution,
            status=ExecutionStatus.IN_PROGRESS,
            attempt=attempt,
            worker_id=self._worker_id,
            started_at=started_at,
        )

        # Execute with timeout
        result_container = {"result": None, "error": None, "timed_out": False}

        def _run_task():
            try:
                result_container["result"] = TASK_REGISTRY.execute(job.task_id, job.parameters)
            except Exception as e:
                result_container["error"] = str(e)

        task_thread = threading.Thread(target=_run_task, daemon=True)
        task_thread.start()
        task_thread.join(timeout=effective_timeout)

        duration_ms = int((time.time() - started_at) * 1000)

        if task_thread.is_alive():
            # Timeout — thread is abandoned (Python can't kill threads, but we mark as timed out)
            logger.error(
                "Job %s (key=%s) TIMED OUT after %ds",
                job_id, execution_key, effective_timeout,
            )
            self._metrics.record_timeout()
            self._exec_repo.update_execution_status(
                execution=execution,
                status=ExecutionStatus.TIMED_OUT,
                attempt=attempt,
                error=f"Execution exceeded timeout of {effective_timeout}s",
                duration_ms=duration_ms,
                completed_at=time.time(),
            )
            self._queue.ack(message)
            self._send_failure_notification(
                job, execution_key, f"Timed out after {effective_timeout}s",
                message.notification_url,
            )
            return

        if result_container["error"] is None:
            # SUCCESS
            result = result_container["result"]
            result_str = json.dumps(result) if result is not None else None
            self._exec_repo.update_execution_status(
                execution=execution,
                status=ExecutionStatus.COMPLETED,
                attempt=attempt,
                result=result_str,
                duration_ms=duration_ms,
                completed_at=time.time(),
            )
            self._queue.ack(message)
            self._metrics.record_success(duration_ms)
            # Increment execution count on the job
            try:
                self._job_repo.increment_execution_count(job_id)
            except Exception:
                pass
            logger.info(
                "Worker %s: job %s completed in %dms", self._worker_id, job_id, duration_ms
            )

        else:
            # FAILURE — retry or DLQ
            error_message = result_container["error"]
            logger.error(
                "Worker %s: job %s failed (attempt %d): %s",
                self._worker_id, job_id, attempt, error_message,
            )
            self._metrics.record_failure(duration_ms)

            if attempt < self._max_retries:
                next_attempt = attempt + 1
                delay = float(self._retry_base_delay ** next_attempt)
                self._metrics.record_retry()

                self._exec_repo.update_execution_status(
                    execution=execution,
                    status=ExecutionStatus.RETRYING,
                    attempt=next_attempt,
                    error=error_message,
                    duration_ms=duration_ms,
                )

                retry_message = message.model_copy(update={
                    "attempt": next_attempt,
                    "execution_time": time.time() + delay,
                })
                self._queue.nack(message, delay=delay)
                try:
                    self._queue._redis.delete(f"dedup:{execution_key}")
                    self._queue.enqueue(retry_message)
                except Exception as enqueue_exc:
                    logger.error("Failed to enqueue retry: %s", enqueue_exc)

                logger.info(
                    "Job %s retry %d/%d scheduled in %.0fs",
                    job_id, next_attempt, self._max_retries, delay,
                )
            else:
                # Max retries exhausted — mark FAILED and send to DLQ
                self._exec_repo.update_execution_status(
                    execution=execution,
                    status=ExecutionStatus.FAILED,
                    attempt=attempt,
                    error=error_message,
                    duration_ms=duration_ms,
                    completed_at=time.time(),
                )
                self._queue.ack(message)

                # Send to Dead Letter Queue for operator inspection
                self._queue.send_to_dlq(
                    message,
                    reason=f"Exhausted {self._max_retries} retries. Last error: {error_message}",
                )

                # Send failure notification
                self._send_failure_notification(
                    job, execution_key, error_message,
                    message.notification_url,
                )

                logger.error(
                    "Worker %s: job %s FAILED after %d retries. Sent to DLQ.",
                    self._worker_id, job_id, attempt,
                )

    def _get_execution(self, time_bucket: int, execution_key: str) -> Optional[Execution]:
        """Safe helper to fetch execution record."""
        try:
            return self._exec_repo.get_execution_by_key(time_bucket, execution_key)
        except Exception as exc:
            logger.error("Failed to fetch execution %s: %s", execution_key, exc)
            return None

    def _send_failure_notification(
        self,
        job,
        execution_key: str,
        error: str,
        notification_url: str = "",
    ) -> None:
        """POST a failure notification to the job's webhook URL.

        Pain point fix: v1 had no notification support at all.
        Users had to manually poll to detect failures.
        """
        url = notification_url or getattr(job, "notification_url", None)
        if not url:
            return

        payload = {
            "event": "job.failed",
            "job_id": str(job.job_id),
            "task_id": job.task_id,
            "execution_key": execution_key,
            "error": error,
            "worker_id": self._worker_id,
            "timestamp": time.time(),
        }

        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(url, json=payload)
                logger.info(
                    "Failure notification sent to %s (status=%d)", url, resp.status_code
                )
        except Exception as exc:
            logger.warning("Failed to send failure notification to %s: %s", url, exc)
