"""Worker service for consuming and executing jobs from the queue."""

from .worker import Worker

__all__ = ["Worker"]
