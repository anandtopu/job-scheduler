"""Task registry for registering and executing tasks by ID."""

import logging
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class TaskRegistry:
    """Registry that maps task IDs to callable handlers.

    Supports decorator-based registration and provides a unified
    execute interface with parameter passing.
    """

    def __init__(self):
        """Initialize an empty task registry."""
        self._handlers: Dict[str, Callable] = {}

    def register(self, task_id: str, handler: Optional[Callable] = None):
        """Register a task handler by task_id.

        Can be used as a decorator or called directly.

        Args:
            task_id: Unique identifier for the task.
            handler: Callable that implements the task. If None, returns
                     a decorator.

        Returns:
            The handler (for use as decorator) or self.

        Example:
            @registry.register("my_task")
            def my_handler(parameters: dict) -> Any:
                ...

            # or directly:
            registry.register("my_task", my_handler)
        """
        if handler is None:
            def decorator(fn: Callable) -> Callable:
                self._handlers[task_id] = fn
                logger.debug("Registered task '%s'.", task_id)
                return fn
            return decorator

        self._handlers[task_id] = handler
        logger.debug("Registered task '%s'.", task_id)
        return handler

    def get(self, task_id: str) -> Optional[Callable]:
        """Retrieve a task handler by task_id.

        Args:
            task_id: The task identifier.

        Returns:
            The callable handler, or None if not registered.
        """
        return self._handlers.get(task_id)

    def list_tasks(self) -> List[str]:
        """List all registered task IDs.

        Returns:
            Sorted list of registered task IDs.
        """
        return sorted(self._handlers.keys())

    def execute(self, task_id: str, parameters: dict) -> Any:
        """Execute a task by its ID.

        Args:
            task_id: The task to execute.
            parameters: Parameters to pass to the task handler.

        Returns:
            The return value of the task handler.

        Raises:
            KeyError: If task_id is not registered.
            Exception: Any exception raised by the task handler.
        """
        handler = self._handlers.get(task_id)
        if handler is None:
            raise KeyError(
                f"Task '{task_id}' is not registered. "
                f"Available tasks: {self.list_tasks()}"
            )

        logger.info("Executing task '%s' with parameters: %s.", task_id, parameters)
        try:
            result = handler(parameters)
            logger.info("Task '%s' completed successfully.", task_id)
            return result
        except Exception as exc:
            logger.error("Task '%s' failed: %s", task_id, exc)
            raise

    def __contains__(self, task_id: str) -> bool:
        """Check if a task_id is registered."""
        return task_id in self._handlers


# Global singleton instance
TASK_REGISTRY = TaskRegistry()
