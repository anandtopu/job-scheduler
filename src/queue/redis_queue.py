"""Redis sorted-set based distributed queue with visibility timeout semantics."""

import logging
import time
from typing import Dict, List, Optional

import redis

from src.core.config import settings
from src.queue.messages import JobMessage

logger = logging.getLogger(__name__)

# Lua script for atomic dequeue: move ready jobs from pending to processing
DEQUEUE_LUA_SCRIPT = """
local now = tonumber(ARGV[1])
local deadline = tonumber(ARGV[2])
local count = tonumber(ARGV[3])
local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, count)
if #jobs == 0 then return {} end
for _, job in ipairs(jobs) do
    redis.call('ZREM', KEYS[1], job)
    redis.call('ZADD', KEYS[2], deadline, job)
end
return jobs
"""

# Lua script for atomic requeue: move expired jobs from processing back to pending
REQUEUE_STUCK_LUA_SCRIPT = """
local now = tonumber(ARGV[1])
local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, 100)
if #jobs == 0 then return 0 end
for _, job in ipairs(jobs) do
    redis.call('ZREM', KEYS[1], job)
    redis.call('ZADD', KEYS[2], now, job)
end
return #jobs
"""

# Lua script for enqueue with deduplication using NX flag
ENQUEUE_DEDUP_LUA_SCRIPT = """
local score = tonumber(ARGV[1])
local member = ARGV[2]
local dedup_key = ARGV[3]
local dedup_ttl = tonumber(ARGV[4])

-- Check dedup key
if redis.call('EXISTS', dedup_key) == 1 then
    return 0
end

-- Set dedup key with TTL
redis.call('SET', dedup_key, '1', 'EX', dedup_ttl)

-- Add to pending sorted set
redis.call('ZADD', KEYS[1], score, member)
return 1
"""


class RedisQueue:
    """Distributed job queue backed by Redis sorted sets.

    Uses two sorted sets:
    - queue:pending - jobs waiting to be executed, scored by execution_time
    - queue:processing - jobs being processed, scored by visibility deadline

    Provides at-least-once delivery with visibility timeout semantics.
    Jobs not acknowledged within the visibility timeout are automatically
    re-queued for redelivery.
    """

    def __init__(self, client: Optional[redis.Redis] = None):
        """Initialize the RedisQueue.

        Args:
            client: Optional pre-configured Redis client. Creates one from
                    settings if not provided.
        """
        if client is not None:
            self._redis = client
        else:
            self._redis = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=10,
                retry_on_timeout=True,
            )

        self._pending_key = settings.QUEUE_PENDING_KEY
        self._processing_key = settings.QUEUE_PROCESSING_KEY
        self._visibility_timeout = settings.QUEUE_VISIBILITY_TIMEOUT

        # Register Lua scripts
        self._dequeue_script = self._redis.register_script(DEQUEUE_LUA_SCRIPT)
        self._requeue_stuck_script = self._redis.register_script(REQUEUE_STUCK_LUA_SCRIPT)
        self._enqueue_dedup_script = self._redis.register_script(ENQUEUE_DEDUP_LUA_SCRIPT)

    def enqueue(self, message: JobMessage) -> bool:
        """Add a job to the pending queue with deduplication.

        Uses a dedup key with TTL to prevent the same execution from being
        enqueued multiple times.

        Args:
            message: The JobMessage to enqueue.

        Returns:
            True if enqueued, False if deduplicated (already exists).
        """
        score = message.execution_time
        member = message.to_json()
        dedup_key = f"dedup:{message.execution_key}"
        # Dedup window: 2 hours (covers execution + retry window)
        dedup_ttl = 7200

        try:
            result = self._enqueue_dedup_script(
                keys=[self._pending_key],
                args=[score, member, dedup_key, dedup_ttl],
            )
            if result == 1:
                logger.debug(
                    "Enqueued message for execution_key=%s at score=%s.",
                    message.execution_key,
                    score,
                )
                return True
            else:
                logger.debug(
                    "Deduplicated enqueue for execution_key=%s.", message.execution_key
                )
                return False
        except Exception as exc:
            logger.error("Failed to enqueue message: %s", exc)
            raise

    def dequeue(self, count: int = 1) -> List[JobMessage]:
        """Atomically dequeue ready jobs and move them to processing.

        Jobs are "ready" when their score (execution_time) <= now.
        Dequeued jobs are moved to the processing set with a deadline
        of now + visibility_timeout.

        Args:
            count: Maximum number of jobs to dequeue.

        Returns:
            List of JobMessage instances.
        """
        now = time.time()
        deadline = now + self._visibility_timeout

        try:
            raw_jobs = self._dequeue_script(
                keys=[self._pending_key, self._processing_key],
                args=[now, deadline, count],
            )
        except Exception as exc:
            logger.error("Failed to dequeue messages: %s", exc)
            return []

        messages = []
        for raw in raw_jobs:
            try:
                msg = JobMessage.from_json(raw)
                messages.append(msg)
            except Exception as exc:
                logger.error("Failed to parse queue message '%s': %s", raw, exc)

        if messages:
            logger.debug("Dequeued %d messages from queue.", len(messages))

        return messages

    def ack(self, message: JobMessage) -> None:
        """Acknowledge successful processing of a message.

        Removes the message from the processing set. Also removes the
        dedup key to allow re-scheduling if needed.

        Args:
            message: The message to acknowledge.
        """
        try:
            removed = self._redis.zrem(self._processing_key, message.to_json())
            if removed:
                logger.debug(
                    "ACK'd message for execution_key=%s.", message.execution_key
                )
            else:
                logger.warning(
                    "ACK for unknown message execution_key=%s.", message.execution_key
                )
        except Exception as exc:
            logger.error("Failed to ACK message: %s", exc)
            raise

    def nack(self, message: JobMessage, delay: float = 0.0) -> None:
        """Negatively acknowledge a message and re-queue it with optional delay.

        Removes the message from processing and re-adds it to pending
        with score = now + delay.

        Args:
            message: The message to nack.
            delay: Seconds to wait before re-delivering. Defaults to 0.
        """
        requeue_time = time.time() + delay
        requeue_message = message.model_copy(update={"execution_time": requeue_time})

        try:
            pipe = self._redis.pipeline(transaction=True)
            pipe.zrem(self._processing_key, message.to_json())
            pipe.zadd(self._pending_key, {requeue_message.to_json(): requeue_time})
            pipe.execute()
            logger.debug(
                "NACK'd message execution_key=%s, requeue delay=%.1fs.",
                message.execution_key,
                delay,
            )
        except Exception as exc:
            logger.error("Failed to NACK message: %s", exc)
            raise

    def heartbeat(self, message: JobMessage) -> None:
        """Extend the visibility timeout for a message being processed.

        Updates the score in the processing set to deadline = now + visibility_timeout.

        Args:
            message: The message whose deadline should be extended.
        """
        new_deadline = time.time() + self._visibility_timeout
        try:
            self._redis.zadd(
                self._processing_key,
                {message.to_json(): new_deadline},
                xx=True,  # Only update, don't add new members
            )
            logger.debug(
                "Heartbeat for execution_key=%s, new deadline=%.0f.",
                message.execution_key,
                new_deadline,
            )
        except Exception as exc:
            logger.error("Failed to send heartbeat: %s", exc)

    def requeue_stuck_jobs(self) -> int:
        """Move expired jobs from processing back to pending.

        Jobs whose visibility deadline has passed are considered stuck
        (worker crashed or failed to heartbeat) and are re-delivered.

        Returns:
            Number of jobs requeued.
        """
        now = time.time()
        try:
            count = self._requeue_stuck_script(
                keys=[self._processing_key, self._pending_key],
                args=[now],
            )
            if count > 0:
                logger.info("Requeued %d stuck jobs from processing.", count)
            return int(count)
        except Exception as exc:
            logger.error("Failed to requeue stuck jobs: %s", exc)
            return 0

    def get_queue_depth(self) -> Dict[str, int]:
        """Get the current number of jobs in each queue state.

        Returns:
            Dictionary with 'pending' and 'processing' counts.
        """
        try:
            pending = self._redis.zcard(self._pending_key)
            processing = self._redis.zcard(self._processing_key)
            return {"pending": pending, "processing": processing}
        except Exception as exc:
            logger.error("Failed to get queue depth: %s", exc)
            return {"pending": -1, "processing": -1}

    def ping(self) -> bool:
        """Check if Redis is reachable.

        Returns:
            True if Redis responds to PING.
        """
        try:
            return self._redis.ping()
        except Exception:
            return False
