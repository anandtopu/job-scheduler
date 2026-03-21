"""Cassandra client with connection management and DDL initialization."""

import logging
import time
from typing import Any, Optional

from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
from cassandra.query import SimpleStatement

from src.core.config import settings

logger = logging.getLogger(__name__)

CREATE_KEYSPACE_CQL = """
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': {rf}}};
"""

CREATE_JOBS_TABLE_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.jobs (
    job_id UUID PRIMARY KEY,
    user_id TEXT,
    task_id TEXT,
    schedule_type TEXT,
    schedule_expression TEXT,
    parameters TEXT,
    status TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
"""

CREATE_JOBS_USER_INDEX_CQL = """
CREATE INDEX IF NOT EXISTS jobs_user_id_idx ON {keyspace}.jobs (user_id);
"""

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
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY (time_bucket, execution_key)
) WITH CLUSTERING ORDER BY (execution_key ASC);
"""

CREATE_USER_EXECUTIONS_TABLE_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.user_executions (
    user_id TEXT,
    execution_time TIMESTAMP,
    execution_key TEXT,
    time_bucket BIGINT,
    job_id UUID,
    status TEXT,
    attempt INT,
    PRIMARY KEY (user_id, execution_time, execution_key)
) WITH CLUSTERING ORDER BY (execution_time DESC, execution_key ASC);
"""


class CassandraClient:
    """Client for interacting with Apache Cassandra.

    Manages connections, provides retry logic, and initializes the schema.
    """

    def __init__(self):
        """Initialize CassandraClient with configuration from settings."""
        self._cluster: Optional[Cluster] = None
        self._session: Optional[Session] = None

    def connect(self, max_retries: int = 10, retry_delay: float = 5.0) -> None:
        """Establish connection to Cassandra cluster.

        Args:
            max_retries: Maximum number of connection attempts.
            retry_delay: Seconds to wait between retries.

        Raises:
            ConnectionError: If unable to connect after max_retries attempts.
        """
        for attempt in range(max_retries):
            try:
                logger.info(
                    "Connecting to Cassandra at %s:%d (attempt %d/%d)",
                    settings.CASSANDRA_HOSTS,
                    settings.CASSANDRA_PORT,
                    attempt + 1,
                    max_retries,
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
                logger.warning(
                    "Cassandra connection attempt %d failed: %s", attempt + 1, exc
                )
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
        """Get the active Cassandra session.

        Returns:
            The active Cassandra Session.

        Raises:
            RuntimeError: If not connected.
        """
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
        """Execute a CQL query with retry logic.

        Args:
            query: The CQL query string or SimpleStatement.
            parameters: Query bind parameters.
            max_retries: Number of times to retry on transient failures.
            retry_delay: Base delay between retries (exponential backoff).

        Returns:
            The result set from Cassandra.

        Raises:
            Exception: If all retries fail.
        """
        session = self.get_session()
        last_exc = None

        for attempt in range(max_retries):
            try:
                if parameters:
                    return session.execute(query, parameters)
                return session.execute(query)
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Query execution attempt %d failed: %s", attempt + 1, exc
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2**attempt))

        raise last_exc

    def initialize_schema(self) -> None:
        """Create keyspace and all tables if they do not exist.

        This method is idempotent and safe to call on every startup.
        """
        keyspace = settings.CASSANDRA_KEYSPACE
        rf = settings.CASSANDRA_REPLICATION_FACTOR

        logger.info("Initializing Cassandra schema for keyspace '%s'.", keyspace)

        # Create keyspace
        self.execute(
            CREATE_KEYSPACE_CQL.format(keyspace=keyspace, rf=rf)
        )

        # Set keyspace on session
        session = self.get_session()
        session.set_keyspace(keyspace)

        # Create tables
        self.execute(CREATE_JOBS_TABLE_CQL.format(keyspace=keyspace))
        self.execute(CREATE_EXECUTIONS_TABLE_CQL.format(keyspace=keyspace))
        self.execute(CREATE_USER_EXECUTIONS_TABLE_CQL.format(keyspace=keyspace))

        # Create indexes (may warn if already exists, that's OK)
        try:
            self.execute(CREATE_JOBS_USER_INDEX_CQL.format(keyspace=keyspace))
        except Exception as exc:
            logger.debug("Index creation warning (may already exist): %s", exc)

        logger.info("Cassandra schema initialization complete.")


# Singleton client instance
cassandra_client = CassandraClient()
