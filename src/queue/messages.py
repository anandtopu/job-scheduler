"""Message models for the job queue.

Enhancements over v1:
- priority field so workers can prefer high-priority messages
- worker_id for sticky routing (optional)
- timeout_seconds passed through to worker
"""

import json

from pydantic import BaseModel

# Priority score offsets — subtracted from execution_time so higher priority
# jobs sort earlier in the sorted set
PRIORITY_OFFSET = {
    "CRITICAL": -3000,   # 50 min ahead
    "HIGH": -1200,       # 20 min ahead
    "NORMAL": 0,
    "LOW": 600,          # 10 min behind
}


class JobMessage(BaseModel):
    """Message representing a job to be executed.

    Serialized to JSON and stored in the Redis sorted set.
    The execution_key is used as a deduplication key.
    """

    job_id: str
    execution_key: str
    time_bucket: int
    execution_time: float        # unix timestamp
    attempt: int = 0
    priority: str = "NORMAL"     # NEW: priority level
    timeout_seconds: int = 30    # NEW: per-job timeout passed to worker
    notification_url: str = ""   # NEW: failure notification endpoint

    def effective_score(self) -> float:
        """Compute the Redis sorted-set score with priority offset applied.

        Higher-priority jobs get a lower score so they dequeue first.
        """
        offset = PRIORITY_OFFSET.get(self.priority, 0)
        return self.execution_time + offset

    def to_json(self) -> str:
        """Serialize the message to a JSON string for Redis storage."""
        return json.dumps(self.model_dump())

    @classmethod
    def from_json(cls, data: str) -> "JobMessage":
        """Deserialize a JSON string to a JobMessage."""
        return cls(**json.loads(data))
