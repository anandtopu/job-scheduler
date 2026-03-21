"""Built-in log message task."""

import logging
from typing import Any

from src.tasks.registry import TASK_REGISTRY

logger = logging.getLogger(__name__)

TASK_ID = "log_message"

VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def log_message(parameters: dict) -> Any:
    """Log a message at the specified level.

    Parameters:
        message (str): The message to log. Required.
        level (str): Log level. One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
                     Defaults to INFO.

    Returns:
        dict: A summary of the logged message.

    Raises:
        ValueError: If message is missing or level is invalid.
    """
    message = parameters.get("message")
    if not message:
        raise ValueError("Parameter 'message' is required for log_message task.")

    level = parameters.get("level", "INFO").upper()
    if level not in VALID_LEVELS:
        raise ValueError(
            f"Invalid log level '{level}'. Must be one of: {sorted(VALID_LEVELS)}"
        )

    log_fn = getattr(logger, level.lower())
    log_fn("[log_message task] %s", message)

    return {"logged": True, "message": message, "level": level}


# Register with the global task registry
TASK_REGISTRY.register(TASK_ID, log_message)
