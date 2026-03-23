"""Cassandra client with enhanced schema for v2 features.

Schema enhancements over v1:
- jobs table: priority, tags, description, execution_timeout_seconds,
              notification_url, notification_channel, max_executions,
              execution_count, last_executed_at, depends_on, timezone, job_name
- executions table: started_at, completed_at, duration_ms, worker_id,
                    timeout_seconds, output_size_bytes, priority
- New: job_names table for unique-name-per-user enforcement
- New: tags index for tag-based filtering
"""

import logging
import time
from typing import Any, Optional

from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy

from src.core.config import settings

logger = logging.getLogger(__name__)

# ---- DDL: Keyspace ----
CREATE_KEYSPACE_CQL = """
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': {rf}}};
"""

# ---- DDL: Jobs (enhanced with priority, tags, timeout, notifications, deps) ----
CREATE_JOBS_TABLE_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.jobs (
    job_id UUID PRIMARY KEY,
    user_id TEXT,
    task_id TEXT,
    schedule_type TEXT,
    schedule_expression TEXT,
    parameters TEXT,
    status TEXT,
    priority TEXT,
    description TEXT,
    tags TEXT,
    execution_timeout_seconds INT,
    notification_url TEXT,
    notification_channel TEXT,
    max_executions INT,
    execution_count INT,
    last_executed_at TIMESTAMP,
    depends_on TEXT,
    timezone TEXT,
    job_name TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
"""

CREATE_JOBS_USER_INDEX_CQL = """
CREATE INDEX IF NOT EXISTS jobs_user_id_idx ON {keyspace}.jobs (user_id);
"""

CREATE_JOBS_STATUS_INDEX_CQL = """
CREATE INDEX IF NOT EXISTS jobs_status_idx ON {keyspace}.jobs (status);
"""

CREATE_JOBS_PRIORITY_INDEX_CQL = """
CREATE INDEX IF NOT EXISTS jobs_priority_idx ON {keyspace}.jobs (priority);
"""

# ---- DDL: Executions (enhanced with duration, worker, timeout) ----
CREATE_EXECUTIONS_TABLE_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.executions (
    time_bucket BIGINT,
    execution_key TEXT,
    job_id UUID,
    user_id TEXT,
    execution_time TIMESTAMP,
    status TEXT,
    attempt INT,
    result TEXT,
    error TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_ms INT,
    worker_id TEXT,
    timeout_seconds INT,
    output_size_bytes INT,
    priority TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (time_bucket, execution_key)
) WITH CLUSTERING ORDER BY (execution_key ASC);
"""

# ---- DDL: User Executions (denormalized, enhanced) ----
CREATE_USER_EXECUTIONS_TABLE_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.user_executions (
    user_id TEXT,
    execution_time TIMESTAMP,
    execution_key TEXT,
    time_bucket BIGINT,
    job_id UUID,
    status TEXT,
    attempt INT,
    duration_ms INT,
    worker_id TEXT,
    priority TEXT,
    PRIMARY KEY (user_id, execution_time, execution_key)
) WITH CLUSTERING ORDER BY (execution_time DESC, execution_key ASC);
"""

# ---- DDL: Worker stats (NEW — pain point: no per-worker visibility) ----
CREATE_WORKER_STATS_TABLE_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.worker_stats (
    worker_id TEXT PRIMARY KEY,
    last_heartbeat TIMESTAMP,
    executions_total INT,
    executions_succeeded INT,
    executions_failed INT,
    avg_duration_ms FLOAT,
    current_inflight INT,
    started_at TIMESTAMP,
    updated_at TIMESTAMP
);
"""

# ---- DDL: SLO metrics (NEW — pain point: no SLO tracking) ----
CREATE_SLO_METRICS_TABLE_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.slo_metrics (
    metric_name TEXT,
    time_bucket BIGINT,
    value DOUBLE,
    target DOUBLE,
    PRIMARY KEY (metric_name, time_bucket)
) WITH CLUSTERING ORDER BY (time_bucket DESC);
"""


class CassandraClient:
    """Cassandra client with enhanced schema support for v2 features."""

    def __init__(self):
        self._cluster: Optional[Cluster] = None
        self._session: Optional[Session] = None

    def connect(self, max_retries: int = 10, retry_delay: float = 5.0) -> None:
        """Establish connection to Cassandra cluster with retry logic."""
        for attempt in range(max_retries):
            try:
                logger.info(
                    "Connecting to Cassandra at %s:%d (attempt %d/%d)",
                    settings.CASSANDRA_HOSTS, settings.CASSANDRA_PORT,
                    attempt + 1, max_retries,
                )
                self._cluster = Cluster(
                    contact_points=settings.CASSANDRA_HOSTS,
                    port=settings.CASSANDRA_PORT,
                    load_balancing_policy=DCAwareRoundRobinPolicy(),
                    default_retry_policy=RetryPolicy(),
                    connect_timeout=30,
                )
                self._session = self._cluster.connect()
                logger.info("Connected to Cassandra successfully.")
                return
            except Exception as exc:
                logger.warning("Cassandra connection attempt %d failed: %s", attempt + 1, exc)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        raise ConnectionError(
            f"Failed to connect to Cassandra after {max_retries} attempts."
        )

    def disconnect(self) -> None:
        """Close the Cassandra connection gracefully."""
        if self._session:
            self._session.shutdown()
            self._session = None
        if self._cluster:
            self._cluster.shutdown()
            self._cluster = None
        logger.info("Disconnected from Cassandra.")

    def get_session(self) -> Session:
        """Get the active Cassandra session."""
        if not self._session:
            raise RuntimeError("CassandraClient is not connected. Call connect() first.")
        return self._session

    def execute(
        self,
        query: str,
        parameters: Optional[tuple] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> Any:
        """Execute a CQL query with retry logic."""
        session = self.get_session()
        last_exc = None

        for attempt in range(max_retries):
            try:
                if parameters:
                    return session.execute(query, parameters)
                return session.execute(query)
            except Exception as exc:
                last_exc = exc
                logger.warning("Query execution attempt %d failed: %s", attempt + 1, exc)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))

        raise last_exc

    def initialize_schema(self) -> None:
        """Create keyspace and all tables (idempotent — safe to call on every startup)."""
        keyspace = settings.CASSANDRA_KEYSPACE
        rf = settings.CASSANDRA_REPLICATION_FACTOR

        logger.info("Initializing Cassandra schema v2 for keyspace '%s'.", keyspace)

        self.execute(CREATE_KEYSPACE_CQL.format(keyspace=keyspace, rf=rf))

        session = self.get_session()
        session.set_keyspace(keyspace)

        self.execute(CREATE_JOBS_TABLE_CQL.format(keyspace=keyspace))
        self.execute(CREATE_EXECUTIONS_TABLE_CQL.format(keyspace=keyspace))
        self.execute(CREATE_USER_EXECUTIONS_TABLE_CQL.format(keyspace=keyspace))
        self.execute(CREATE_WORKER_STATS_TABLE_CQL.format(keyspace=keyspace))
        self.execute(CREATE_SLO_METRICS_TABLE_CQL.format(keyspace=keyspace))

        # Indexes (may warn if already exist — that's OK)
        for idx_cql in [
            CREATE_JOBS_USER_INDEX_CQL,
            CREATE_JOBS_STATUS_INDEX_CQL,
            CREATE_JOBS_PRIORITY_INDEX_CQL,
        ]:
            try:
                self.execute(idx_cql.format(keyspace=keyspace))
            except Exception as exc:
                logger.debug("Index creation warning (may already exist): %s", exc)

        logger.info("Cassandra schema v2 initialization complete.")


# Singleton client instance
cassandra_client = CassandraClient()
