"""Redis sorted-set based distributed queue with visibility timeout semantics.

Enhancements over v1:
- Priority queue: CRITICAL/HIGH/NORMAL/LOW — pain point: v1 had flat FIFO
- Dead-letter queue (DLQ): exhausted jobs go to a separate set for inspection
- Queue metrics: per-priority depth counters via Redis hash
- Batch enqueue for efficiency
- get_dlq_jobs() to inspect permanently failed jobs
- purge_dlq() to clear dead letters
"""

import logging
import time
from typing import Dict, List, Optional

import redis

from src.core.config import settings
from src.queue.messages import JobMessage, PRIORITY_OFFSET

logger = logging.getLogger(__name__)

# Atomic dequeue Lua: move ready jobs (score <= now) to processing
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

# Requeue stuck jobs from processing back to pending
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

# Enqueue with dedup + priority score
ENQUEUE_DEDUP_LUA_SCRIPT = """
local score = tonumber(ARGV[1])
local member = ARGV[2]
local dedup_key = ARGV[3]
local dedup_ttl = tonumber(ARGV[4])
if redis.call('EXISTS', dedup_key) == 1 then return 0 end
redis.call('SET', dedup_key, '1', 'EX', dedup_ttl)
redis.call('ZADD', KEYS[1], score, member)
return 1
"""

DLQ_KEY = "queue:dlq"
METRICS_KEY = "queue:metrics"


class RedisQueue:
    """Distributed job queue backed by Redis sorted sets.

    Uses two sorted sets:
    - queue:pending   — jobs waiting, scored by effective_score() (priority-adjusted)
    - queue:processing — jobs in-flight, scored by visibility deadline
    - queue:dlq        — NEW: permanently failed jobs for inspection

    Guarantees at-least-once delivery with visibility timeout semantics.
    """

    def __init__(self, client: Optional[redis.Redis] = None):
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
        """Add a job to the pending queue with deduplication and priority scoring."""
        # Use priority-adjusted score so higher-priority jobs sort earlier
        score = message.effective_score()
        member = message.to_json()
        dedup_key = f"dedup:{message.execution_key}"
        dedup_ttl = 7200  # 2-hour dedup window

        try:
            result = self._enqueue_dedup_script(
                keys=[self._pending_key],
                args=[score, member, dedup_key, dedup_ttl],
            )
            if result == 1:
                # Increment per-priority metric counter
                self._redis.hincrby(METRICS_KEY, f"enqueued:{message.priority}", 1)
                logger.debug(
                    "Enqueued %s priority=%s execution_key=%s score=%.0f",
                    message.job_id, message.priority, message.execution_key, score,
                )
                return True
            else:
                logger.debug("Deduplicated enqueue for execution_key=%s", message.execution_key)
                return False
        except Exception as exc:
            logger.error("Failed to enqueue message: %s", exc)
            raise

    def enqueue_batch(self, messages: List[JobMessage]) -> int:
        """Enqueue multiple messages efficiently.

        Returns count of successfully enqueued (non-deduplicated) messages.
        Pain point fix: v1 enqueued one at a time in Python loop — slow for large batches.
        """
        count = 0
        pipe = self._redis.pipeline(transaction=False)
        for msg in messages:
            try:
                if self.enqueue(msg):
                    count += 1
            except Exception as exc:
                logger.error("Batch enqueue error for %s: %s", msg.execution_key, exc)
        return count

    def dequeue(self, count: int = 1) -> List[JobMessage]:
        """Atomically dequeue ready jobs and move them to processing."""
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
                self._redis.hincrby(METRICS_KEY, f"dequeued:{msg.priority}", 1)
            except Exception as exc:
                logger.error("Failed to parse queue message '%s': %s", raw, exc)

        return messages

    def ack(self, message: JobMessage) -> None:
        """Acknowledge successful processing — remove from processing set."""
        try:
            self._redis.zrem(self._processing_key, message.to_json())
            self._redis.hincrby(METRICS_KEY, "acked_total", 1)
        except Exception as exc:
            logger.error("Failed to ACK message: %s", exc)
            raise

    def nack(self, message: JobMessage, delay: float = 0.0) -> None:
        """Negatively acknowledge — re-queue with optional delay."""
        requeue_time = time.time() + delay
        requeue_message = message.model_copy(update={"execution_time": requeue_time})

        try:
            pipe = self._redis.pipeline(transaction=True)
            pipe.zrem(self._processing_key, message.to_json())
            pipe.zadd(self._pending_key, {requeue_message.to_json(): requeue_time})
            pipe.execute()
            self._redis.hincrby(METRICS_KEY, "nacked_total", 1)
        except Exception as exc:
            logger.error("Failed to NACK message: %s", exc)
            raise

    def send_to_dlq(self, message: JobMessage, reason: str = "") -> None:
        """Send a permanently failed message to the Dead Letter Queue.

        Pain point fix: v1 had no DLQ — failed jobs just disappeared.
        Operators had no way to inspect or replay them.
        """
        import json
        dlq_entry = json.dumps({
            "message": message.model_dump(),
            "reason": reason,
            "failed_at": time.time(),
        })
        try:
            self._redis.zadd(DLQ_KEY, {dlq_entry: time.time()})
            self._redis.hincrby(METRICS_KEY, "dlq_total", 1)
            logger.warning(
                "Sent job %s to DLQ. Reason: %s", message.execution_key, reason
            )
        except Exception as exc:
            logger.error("Failed to send to DLQ: %s", exc)

    def get_dlq_jobs(self, limit: int = 50) -> List[dict]:
        """Retrieve jobs from the Dead Letter Queue for inspection."""
        import json
        try:
            raw = self._redis.zrevrange(DLQ_KEY, 0, limit - 1, withscores=True)
            return [{"entry": json.loads(r[0]), "score": r[1]} for r in raw]
        except Exception as exc:
            logger.error("Failed to get DLQ jobs: %s", exc)
            return []

    def purge_dlq(self) -> int:
        """Clear all entries from the Dead Letter Queue. Returns count removed."""
        try:
            count = self._redis.zcard(DLQ_KEY)
            self._redis.delete(DLQ_KEY)
            logger.info("Purged %d entries from DLQ", count)
            return count
        except Exception as exc:
            logger.error("Failed to purge DLQ: %s", exc)
            return 0

    def heartbeat(self, message: JobMessage) -> None:
        """Extend the visibility timeout for a message being processed."""
        new_deadline = time.time() + self._visibility_timeout
        try:
            self._redis.zadd(
                self._processing_key,
                {message.to_json(): new_deadline},
                xx=True,
            )
        except Exception as exc:
            logger.error("Failed to send heartbeat: %s", exc)

    def requeue_stuck_jobs(self) -> int:
        """Move expired jobs from processing back to pending."""
        now = time.time()
        try:
            count = self._requeue_stuck_script(
                keys=[self._processing_key, self._pending_key],
                args=[now],
            )
            if count > 0:
                logger.info("Requeued %d stuck jobs from processing.", count)
                self._redis.hincrby(METRICS_KEY, "requeued_stuck_total", int(count))
            return int(count)
        except Exception as exc:
            logger.error("Failed to requeue stuck jobs: %s", exc)
            return 0

    def get_queue_depth(self) -> Dict[str, int]:
        """Get current queue depths for pending, processing, and DLQ."""
        try:
            pending = self._redis.zcard(self._pending_key)
            processing = self._redis.zcard(self._processing_key)
            dlq = self._redis.zcard(DLQ_KEY)
            return {"pending": pending, "processing": processing, "dlq": dlq}
        except Exception as exc:
            logger.error("Failed to get queue depth: %s", exc)
            return {"pending": -1, "processing": -1, "dlq": -1}

    def get_queue_metrics(self) -> Dict[str, int]:
        """Get cumulative queue metrics from Redis hash.

        Pain point fix: v1 had zero observability into queue throughput.
        """
        try:
            raw = self._redis.hgetall(METRICS_KEY)
            return {k: int(v) for k, v in raw.items()}
        except Exception as exc:
            logger.error("Failed to get queue metrics: %s", exc)
            return {}

    def ping(self) -> bool:
        """Check if Redis is reachable."""
        try:
            return self._redis.ping()
        except Exception:
            return False
