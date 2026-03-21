"""Message models for the job queue."""

import json

from pydantic import BaseModel


class JobMessage(BaseModel):
    """Message representing a job to be executed.

    This message is serialized to JSON and stored in the Redis sorted set.
    The execution_key is used as a deduplication key.
    """

    job_id: str
    execution_key: str
    time_bucket: int
    execution_time: float  # unix timestamp
    attempt: int = 0

    def to_json(self) -> str:
        """Serialize the message to a JSON string for Redis storage.

        Returns:
            JSON string representation.
        """
        return json.dumps(self.model_dump())

    @classmethod
    def from_json(cls, data: str) -> "JobMessage":
        """Deserialize a JSON string to a JobMessage.

        Args:
            data: JSON string from Redis.

        Returns:
            JobMessage instance.

        Raises:
            ValueError: If data cannot be parsed.
        """
        return cls(**json.loads(data))
