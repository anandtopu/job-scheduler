"""Application configuration using pydantic-settings."""

from typing import List

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Cassandra settings
    CASSANDRA_HOSTS: List[str] = ["localhost"]
    CASSANDRA_PORT: int = 9042
    CASSANDRA_KEYSPACE: str = "job_scheduler"
    CASSANDRA_REPLICATION_FACTOR: int = 1

    # Redis settings
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0

    # Queue settings
    QUEUE_PENDING_KEY: str = "queue:pending"
    QUEUE_PROCESSING_KEY: str = "queue:processing"
    QUEUE_VISIBILITY_TIMEOUT: int = 30  # seconds
    QUEUE_HEARTBEAT_INTERVAL: int = 15  # seconds

    # Scheduler settings
    SCHEDULER_POLL_INTERVAL: int = 300  # seconds (5 minutes)
    SCHEDULER_LOOKAHEAD_SECONDS: int = 360  # seconds (6 minutes lookahead)

    # Worker settings
    WORKER_CONCURRENCY: int = 10
    WORKER_MAX_RETRIES: int = 3
    WORKER_RETRY_BASE_DELAY: int = 5  # seconds

    # API settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

    # Logging
    LOG_LEVEL: str = "INFO"

    @field_validator("CASSANDRA_HOSTS", mode="before")
    @classmethod
    def parse_cassandra_hosts(cls, v):
        """Parse CASSANDRA_HOSTS from comma-separated string or list."""
        if isinstance(v, str):
            return [host.strip() for host in v.split(",")]
        return v


settings = Settings()
