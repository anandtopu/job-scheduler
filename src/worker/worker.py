"""Worker service that consumes jobs from the Redis queue and executes them."""

import json
import logging
import signal
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Dict, Optional
from uuid import UUID

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


class Worker:
    """Concurrent job worker that processes messages from the Redis queue.

    Uses a ThreadPoolExecutor for parallel job execution and a background
    heartbeat thread to extend visibility timeouts for in-flight jobs.

    Guarantees at-least-once execution:
    - Jobs are moved to "processing" set on dequeue
    - If the worker crashes, visibility timeout expires and job is redelivered
    - Successful jobs are ACK'd; failed jobs are NACK'd with retry delay
    """

    def __init__(
        self,
        queue: RedisQueue,
        cassandra_client: CassandraClient,
        concurrency: Optional[int] = None,
        max_retries: Optional[int] = None,
        retry_base_delay: Optional[int] = None,
    ):
        """Initialize the Worker.

        Args:
            queue: RedisQueue instance for message consumption.
            cassandra_client: Connected CassandraClient for DB operations.
            concurrency: Number of concurrent worker threads.
            max_retries: Maximum retry attempts per job.
            retry_base_delay: Base delay in seconds for exponential backoff.
        """
        self._queue = queue
        self._cassandra = cassandra_client
        self._job_repo = JobRepository(cassandra_client)
        self._exec_repo = ExecutionRepository(cassandra_client)

        self._concurrency = concurrency or settings.WORKER_CONCURRENCY
        self._max_retries = max_retries or settings.WORKER_MAX_RETRIES
        self._retry_base_delay = retry_base_delay or settings.WORKER_RETRY_BASE_DELAY

        self._running = False
        self._executor: Optional[ThreadPoolExecutor] = None

        # Track in-flight messages for heartbeating: {execution_key: JobMessage}
        self._in_flight: Dict[str, JobMessage] = {}
        self._in_flight_lock = threading.Lock()

        self._heartbeat_thread: Optional[threading.Thread] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._requeue_thread: Optional[threading.Thread] = None

    def run(self) -> None:
        """Start the worker and block until stopped.

        Sets up signal handlers for graceful shutdown on SIGTERM/SIGINT.
        """
        logger.info(
            "Starting worker with concurrency=%d, max_retries=%d.",
            self._concurrency,
            self._max_retries,
        )
        self._running = True
        self._executor = ThreadPoolExecutor(
            max_workers=self._concurrency, thread_name_prefix="job-worker"
        )

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        # Start background threads
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="worker-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

        self._requeue_thread = threading.Thread(
            target=self._requeue_loop,
            name="worker-requeue",
            daemon=True,
        )
        self._requeue_thread.start()

        # Main poll loop runs in current thread
        try:
            self._poll_loop()
        finally:
            self.stop()

    def stop(self) -> None:
        """Gracefully stop the worker.

        Waits for in-flight jobs to complete before shutting down.
        """
        if not self._running:
            return

        logger.info("Stopping worker gracefully...")
        self._running = False

        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None

        logger.info("Worker stopped.")

    def _handle_signal(self, signum: int, frame) -> None:
        """Handle SIGTERM/SIGINT for graceful shutdown."""
        logger.info("Received signal %d. Initiating graceful shutdown.", signum)
        self.stop()

    def _poll_loop(self) -> None:
        """Main polling loop: dequeue messages and submit to executor."""
        while self._running:
            try:
                # Calculate available capacity
                with self._in_flight_lock:
                    in_flight_count = len(self._in_flight)

                capacity = max(0, self._concurrency - in_flight_count)
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
                logger.error("Error in poll loop: %s", exc)
                time.sleep(5.0)

    def _execute_with_cleanup(self, message: JobMessage) -> None:
        """Execute a job and clean up from in-flight tracking."""
        try:
            self.execute_job(message)
        finally:
            with self._in_flight_lock:
                self._in_flight.pop(message.execution_key, None)

    def _heartbeat_loop(self) -> None:
        """Periodically extend visibility timeout for in-flight messages."""
        interval = settings.QUEUE_HEARTBEAT_INTERVAL
        while self._running:
            time.sleep(interval)
            with self._in_flight_lock:
                messages = list(self._in_flight.values())

            for message in messages:
                try:
                    self._queue.heartbeat(message)
                except Exception as exc:
                    logger.warning(
                        "Heartbeat failed for execution_key=%s: %s",
                        message.execution_key,
                        exc,
                    )

    def _requeue_loop(self) -> None:
        """Periodically requeue stuck jobs that missed their visibility timeout."""
        while self._running:
            time.sleep(settings.QUEUE_VISIBILITY_TIMEOUT)
            try:
                self._queue.requeue_stuck_jobs()
            except Exception as exc:
                logger.error("Error in requeue loop: %s", exc)

    def execute_job(self, message: JobMessage) -> None:
        """Execute a single job from a queue message.

        Workflow:
        1. Fetch job definition from DB
        2. Check if execution already completed (idempotency)
        3. Mark execution as IN_PROGRESS
        4. Execute the registered task
        5. On success: mark COMPLETED, ACK message
        6. On failure: retry or mark FAILED, NACK message

        Args:
            message: The JobMessage to process.
        """
        execution_key = message.execution_key
        time_bucket = message.time_bucket
        job_id = UUID(message.job_id)
        attempt = message.attempt

        logger.info(
            "Executing job %s (execution_key=%s, attempt=%d).",
            job_id,
            execution_key,
            attempt,
        )

        # Fetch job definition
        job = self._job_repo.get_job(job_id)
        if job is None:
            logger.warning("Job %s not found in DB. Skipping.", job_id)
            self._queue.ack(message)
            return

        # Fetch execution record
        execution = self._exec_repo.get_execution_by_key(time_bucket, execution_key)
        if execution is None:
            logger.warning(
                "Execution record not found for key=%s. Skipping.", execution_key
            )
            self._queue.ack(message)
            return

        # Idempotency check: skip if already completed
        if str(execution.status) == ExecutionStatus.COMPLETED:
            logger.info(
                "Execution %s already COMPLETED. Skipping (idempotent).", execution_key
            )
            self._queue.ack(message)
            return

        # Mark as IN_PROGRESS
        self._exec_repo.update_execution_status(
            execution=execution,
            status=ExecutionStatus.IN_PROGRESS,
            attempt=attempt,
        )

        # Execute the task
        try:
            result = TASK_REGISTRY.execute(job.task_id, job.parameters)
            result_str = json.dumps(result) if result is not None else None

            # Mark as COMPLETED
            self._exec_repo.update_execution_status(
                execution=execution,
                status=ExecutionStatus.COMPLETED,
                attempt=attempt,
                result=result_str,
            )
            self._queue.ack(message)
            logger.info(
                "Job %s (execution_key=%s) completed successfully.",
                job_id,
                execution_key,
            )

        except Exception as exc:
            error_message = str(exc)
            logger.error(
                "Job %s (execution_key=%s, attempt=%d) failed: %s",
                job_id,
                execution_key,
                attempt,
                error_message,
            )

            if attempt < self._max_retries:
                # Retry with exponential backoff
                next_attempt = attempt + 1
                delay = float(self._retry_base_delay**next_attempt)

                self._exec_repo.update_execution_status(
                    execution=execution,
                    status=ExecutionStatus.RETRYING,
                    attempt=next_attempt,
                    error=error_message,
                )

                retry_message = message.model_copy(
                    update={
                        "attempt": next_attempt,
                        "execution_time": time.time() + delay,
                    }
                )
                self._queue.nack(message, delay=delay)
                # Remove dedup key to allow retry enqueue
                try:
                    self._queue._redis.delete(f"dedup:{execution_key}")
                    self._queue.enqueue(retry_message)
                except Exception as enqueue_exc:
                    logger.error("Failed to enqueue retry: %s", enqueue_exc)

                logger.info(
                    "Job %s scheduled for retry (attempt %d) in %.1fs.",
                    job_id,
                    next_attempt,
                    delay,
                )
            else:
                # Max retries exceeded; mark as FAILED
                self._exec_repo.update_execution_status(
                    execution=execution,
                    status=ExecutionStatus.FAILED,
                    attempt=attempt,
                    error=error_message,
                )
                self._queue.ack(message)
                logger.error(
                    "Job %s (execution_key=%s) FAILED after %d attempts.",
                    job_id,
                    execution_key,
                    attempt,
                )
