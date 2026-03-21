#!/usr/bin/env python3
"""Script to initialize Cassandra keyspace and tables.

Run this script before starting the application for the first time,
or after changing the schema.

Usage:
    python scripts/init_db.py
"""

import logging
import sys
import os

# Add the project root to the Python path
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
    """Initialize the Cassandra schema.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    logger.info("=== Job Scheduler DB Initialization ===")
    logger.info(
        "Connecting to Cassandra at %s:%d",
        settings.CASSANDRA_HOSTS,
        settings.CASSANDRA_PORT,
    )

    client = CassandraClient()

    try:
        client.connect(max_retries=15, retry_delay=5.0)
        client.initialize_schema()
        logger.info("=== Schema initialization complete ===")
        return 0
    except Exception as exc:
        logger.error("Failed to initialize schema: %s", exc)
        return 1
    finally:
        try:
            client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
