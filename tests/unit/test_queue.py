"""Unit tests for the Redis queue implementation."""

import time
from unittest.mock import patch

import fakeredis
import pytest

from src.core.config import settings
from src.queue.messages import JobMessage
from src.queue.redis_queue import RedisQueue


@pytest.fixture
def redis_client():
    """Return an in-memory fakeredis client."""
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def queue(redis_client):
    """Return a RedisQueue backed by fakeredis."""
    return RedisQueue(client=redis_client)


def make_message(execution_time: float = None, attempt: int = 0) -> JobMessage:
    """Helper to create a test JobMessage."""
    from uuid import uuid4
    job_id = str(uuid4())
    exec_ts = execution_time or time.time()
    return JobMessage(
        job_id=job_id,
        execution_key=f"{int(exec_ts)}#{job_id}",
        time_bucket=int(exec_ts // 3600) * 3600,
        execution_time=exec_ts,
        attempt=attempt,
    )


class TestEnqueue:
    def test_enqueue_future_job(self, queue, redis_client):
        """Enqueuing a future job adds it to the pending sorted set."""
        future_time = time.time() + 3600  # 1 hour from now
        msg = make_message(execution_time=future_time)

        enqueued = queue.enqueue(msg)

        assert enqueued is True
        depth = queue.get_queue_depth()
        assert depth["pending"] == 1
        assert depth["processing"] == 0

    def test_enqueue_deduplication(self, queue):
        """Enqueueing the same execution_key twice is deduplicated."""
        msg = make_message()

        result1 = queue.enqueue(msg)
        result2 = queue.enqueue(msg)

        assert result1 is True
        assert result2 is False
        assert queue.get_queue_depth()["pending"] == 1

    def test_enqueue_past_job_is_immediately_ready(self, queue):
        """Jobs with past execution_time are immediately ready for dequeue."""
        past_time = time.time() - 10
        msg = make_message(execution_time=past_time)

        queue.enqueue(msg)
        messages = queue.dequeue(count=1)

        assert len(messages) == 1
        assert messages[0].job_id == msg.job_id


class TestDequeue:
    def test_dequeue_ready_job(self, queue):
        """dequeue() returns a job that is ready (execution_time <= now)."""
        past_msg = make_message(execution_time=time.time() - 5)
        queue.enqueue(past_msg)

        messages = queue.dequeue(count=1)

        assert len(messages) == 1
        assert messages[0].execution_key == past_msg.execution_key

    def test_dequeue_not_yet_ready(self, queue):
        """dequeue() does NOT return a job whose execution_time is in the future."""
        future_msg = make_message(execution_time=time.time() + 3600)
        queue.enqueue(future_msg)

        messages = queue.dequeue(count=1)

        assert len(messages) == 0

    def test_dequeue_moves_to_processing(self, queue):
        """Dequeued jobs are moved from pending to processing set."""
        msg = make_message(execution_time=time.time() - 1)
        queue.enqueue(msg)

        queue.dequeue(count=1)

        depth = queue.get_queue_depth()
        assert depth["pending"] == 0
        assert depth["processing"] == 1

    def test_dequeue_count_limit(self, queue):
        """dequeue() respects the count parameter."""
        for _ in range(5):
            queue.enqueue(make_message(execution_time=time.time() - 1))
            # Remove dedup key so each message can be enqueued

        # Enqueue fresh messages (different execution_keys)
        msgs = []
        for i in range(5):
            msg = make_message(execution_time=time.time() - 1)
            queue._redis.delete(f"dedup:{msg.execution_key}")
            queue.enqueue(msg)
            msgs.append(msg)

        result = queue.dequeue(count=3)
        assert len(result) <= 3


class TestAck:
    def test_ack_removes_from_processing(self, queue):
        """ack() removes the message from the processing set."""
        msg = make_message(execution_time=time.time() - 1)
        queue.enqueue(msg)
        dequeued = queue.dequeue(count=1)

        queue.ack(dequeued[0])

        depth = queue.get_queue_depth()
        assert depth["processing"] == 0
        assert depth["pending"] == 0


class TestNack:
    def test_nack_requeues_with_delay(self, queue):
        """nack() moves the message back to pending with a delay."""
        msg = make_message(execution_time=time.time() - 1)
        queue.enqueue(msg)
        dequeued = queue.dequeue(count=1)

        # NACK with 60s delay
        queue.nack(dequeued[0], delay=60.0)

        depth = queue.get_queue_depth()
        assert depth["processing"] == 0
        assert depth["pending"] == 1

        # Should not be immediately dequeue-able
        retry_msgs = queue.dequeue(count=1)
        assert len(retry_msgs) == 0

    def test_nack_no_delay_immediately_ready(self, queue):
        """nack() with delay=0 makes the job immediately available."""
        msg = make_message(execution_time=time.time() - 1)
        queue.enqueue(msg)
        dequeued = queue.dequeue(count=1)

        queue.nack(dequeued[0], delay=0.0)

        retry_msgs = queue.dequeue(count=1)
        assert len(retry_msgs) == 1


class TestRequeueStuck:
    def test_requeue_stuck_jobs(self, queue):
        """requeue_stuck_jobs() returns expired processing jobs to pending."""
        # Manually add a job to processing with an expired deadline (in the past)
        msg = make_message(execution_time=time.time() - 100)
        expired_deadline = time.time() - 10  # Already expired

        # pylint: disable=protected-access
        queue._redis.zadd(
            settings.QUEUE_PROCESSING_KEY,
            {msg.to_json(): expired_deadline},
        )

        count = queue.requeue_stuck_jobs()

        assert count == 1
        depth = queue.get_queue_depth()
        assert depth["processing"] == 0
        assert depth["pending"] == 1

    def test_requeue_does_not_touch_active_jobs(self, queue):
        """requeue_stuck_jobs() leaves non-expired processing jobs alone."""
        msg = make_message(execution_time=time.time() - 1)
        future_deadline = time.time() + 300  # Still valid

        queue._redis.zadd(
            settings.QUEUE_PROCESSING_KEY,
            {msg.to_json(): future_deadline},
        )

        count = queue.requeue_stuck_jobs()

        assert count == 0
        depth = queue.get_queue_depth()
        assert depth["processing"] == 1


class TestHeartbeat:
    def test_heartbeat_extends_deadline(self, queue):
        """heartbeat() updates the processing score to extend the deadline."""
        msg = make_message(execution_time=time.time() - 1)
        queue.enqueue(msg)
        dequeued = queue.dequeue(count=1)[0]

        # Get initial score
        initial_score = queue._redis.zscore(
            settings.QUEUE_PROCESSING_KEY, dequeued.to_json()
        )

        time.sleep(0.01)  # Tiny sleep so time advances
        queue.heartbeat(dequeued)

        new_score = queue._redis.zscore(
            settings.QUEUE_PROCESSING_KEY, dequeued.to_json()
        )

        assert new_score is not None
        assert new_score >= initial_score
