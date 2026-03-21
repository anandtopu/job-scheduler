"""FastAPI application factory with lifespan management."""

import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from src.api.routes.jobs import router as jobs_router
from src.api.schemas import ErrorResponse
from src.core.config import settings
from src.db.cassandra import cassandra_client
from src.db.repositories.execution_repo import ExecutionRepository
from src.db.repositories.job_repo import JobRepository
from src.queue.redis_queue import RedisQueue
from src.scheduler.scheduler import Scheduler

# Import built-in tasks to register them
from src.tasks.builtin import email_task, http_task, log_task  # noqa: F401

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure structured JSON logging."""
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}',
        stream=sys.stdout,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Manage application lifespan: startup and shutdown."""
    setup_logging()
    logger.info("Starting Job Scheduler API...")

    # Connect to Cassandra
    cassandra_client.connect()
    cassandra_client.initialize_schema()

    # Set up repositories
    app.state.cassandra_client = cassandra_client
    app.state.job_repo = JobRepository(cassandra_client)
    app.state.exec_repo = ExecutionRepository(cassandra_client)

    # Connect to Redis queue
    queue = RedisQueue()
    app.state.queue = queue

    # Initialize scheduler (not running its blocking loop, just for scheduling calls)
    scheduler = Scheduler(queue=queue, cassandra_client=cassandra_client)
    app.state.scheduler = scheduler

    logger.info("Job Scheduler API startup complete.")

    yield

    # Shutdown
    logger.info("Shutting down Job Scheduler API...")
    cassandra_client.disconnect()
    logger.info("Shutdown complete.")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Returns:
        Configured FastAPI application instance.
    """
    app = FastAPI(
        title="Job Scheduler API",
        description=(
            "Production-grade distributed job scheduling system. "
            "Supports immediate, datetime, and recurring CRON-based job scheduling."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(jobs_router, prefix="/api/v1", tags=["Jobs"])

    # Custom exception handlers
    @app.exception_handler(ValidationError)
    async def validation_exception_handler(
        request: Request, exc: ValidationError
    ) -> JSONResponse:
        """Handle Pydantic validation errors."""
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=ErrorResponse(
                error="Validation Error",
                detail=str(exc),
            ).model_dump(mode="json"),
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(
        request: Request, exc: ValueError
    ) -> JSONResponse:
        """Handle ValueError as 400 Bad Request."""
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=ErrorResponse(
                error="Bad Request",
                detail=str(exc),
            ).model_dump(mode="json"),
        )

    @app.exception_handler(KeyError)
    async def key_error_handler(
        request: Request, exc: KeyError
    ) -> JSONResponse:
        """Handle KeyError as 400 Bad Request."""
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=ErrorResponse(
                error="Not Found",
                detail=str(exc),
            ).model_dump(mode="json"),
        )

    @app.exception_handler(Exception)
    async def generic_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        """Handle unexpected errors as 500 Internal Server Error."""
        logger.error("Unhandled exception: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=ErrorResponse(
                error="Internal Server Error",
                detail="An unexpected error occurred.",
            ).model_dump(mode="json"),
        )

    return app


# Application instance
app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api.app:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=False,
        log_level=settings.LOG_LEVEL.lower(),
    )
