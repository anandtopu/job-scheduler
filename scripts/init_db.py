#!/usr/bin/env python3
"""Script to initialize Cassandra keyspace and tables for v2.

Includes all new v2 columns: priority, tags, timeout, notifications,
execution duration tracking, worker stats, SLO metrics tables.

Usage:
    python scripts/init_db.py
"""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.config import settings
from src.db.cassandra import CassandraClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Initialize the Cassandra schema v2."""
    logger.info("=== Job Scheduler DB Initialization v2 ===")
    logger.info(
        "Connecting to Cassandra at %s:%d",
        settings.CASSANDRA_HOSTS,
        settings.CASSANDRA_PORT,
    )

    client = CassandraClient()

    try:
        client.connect(max_retries=15, retry_delay=5.0)
        client.initialize_schema()

        # Attempt to run migration for existing v1 deployments
        _run_v1_migration(client)

        logger.info("=== Schema initialization v2 complete ===")
        return 0
    except Exception as exc:
        logger.error("Failed to initialize schema: %s", exc)
        return 1
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


def _run_v1_migration(client: CassandraClient) -> None:
    """Add new columns to existing v1 tables (safe ALTER TABLE ADD IF NOT EXISTS).

    This makes the v2 schema backward compatible with existing v1 deployments.
    Cassandra supports adding new columns to existing tables without data loss.
    """
    keyspace = settings.CASSANDRA_KEYSPACE
    logger.info("Running v1 → v2 migration for keyspace '%s'...", keyspace)

    v1_migrations = [
        # jobs table: new v2 columns
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS priority TEXT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS description TEXT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS tags TEXT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS execution_timeout_seconds INT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS notification_url TEXT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS notification_channel TEXT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS max_executions INT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS execution_count INT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS last_executed_at TIMESTAMP",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS depends_on TEXT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS timezone TEXT",
        f"ALTER TABLE {keyspace}.jobs ADD IF NOT EXISTS job_name TEXT",

        # executions table: new v2 columns
        f"ALTER TABLE {keyspace}.executions ADD IF NOT EXISTS started_at TIMESTAMP",
        f"ALTER TABLE {keyspace}.executions ADD IF NOT EXISTS completed_at TIMESTAMP",
        f"ALTER TABLE {keyspace}.executions ADD IF NOT EXISTS duration_ms INT",
        f"ALTER TABLE {keyspace}.executions ADD IF NOT EXISTS worker_id TEXT",
        f"ALTER TABLE {keyspace}.executions ADD IF NOT EXISTS timeout_seconds INT",
        f"ALTER TABLE {keyspace}.executions ADD IF NOT EXISTS output_size_bytes INT",
        f"ALTER TABLE {keyspace}.executions ADD IF NOT EXISTS priority TEXT",

        # user_executions: new v2 columns
        f"ALTER TABLE {keyspace}.user_executions ADD IF NOT EXISTS duration_ms INT",
        f"ALTER TABLE {keyspace}.user_executions ADD IF NOT EXISTS worker_id TEXT",
        f"ALTER TABLE {keyspace}.user_executions ADD IF NOT EXISTS priority TEXT",
    ]

    for cql in v1_migrations:
        try:
            client.execute(cql)
            logger.debug("Migration OK: %s", cql[:80])
        except Exception as exc:
            # Column may already exist or syntax differs — log and continue
            logger.debug("Migration skipped (%s): %s", type(exc).__name__, cql[:80])

    logger.info("v1 → v2 migration complete.")


if __name__ == "__main__":
    sys.exit(main())
