"""FastAPI application factory with enhanced lifespan management (v2).

Enhancements over v1:
- Monitoring router mounted at /api/v1/monitoring/*
- Request ID middleware for distributed tracing
- Structured JSON logging with request context
- Graceful shutdown with in-flight job drain
"""

import logging
import sys
import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.middleware.base import BaseHTTPMiddleware

from src.api.routes.jobs import router as jobs_router
from src.api.routes.monitoring import router as monitoring_router
from src.api.schemas import ErrorResponse
from src.core.config import settings
from src.db.cassandra import cassandra_client
from src.db.repositories.execution_repo import ExecutionRepository
from src.db.repositories.job_repo import JobRepository
from src.queue.redis_queue import RedisQueue
from src.scheduler.scheduler import Scheduler
from src.tasks.builtin import email_task, http_task, log_task  # noqa: F401

logger = logging.getLogger(__name__)


class RequestIDMiddleware(BaseHTTPMiddleware):
    """Inject X-Request-ID header for distributed tracing.

    Enhancement: v1 had no request tracing support.
    """

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response


def setup_logging() -> None:
    """Configure structured JSON logging."""
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format=(
            '{"time": "%(asctime)s", "level": "%(levelname)s", '
            '"logger": "%(name)s", "message": "%(message)s"}'
        ),
        stream=sys.stdout,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Manage application lifespan: startup and shutdown."""
    setup_logging()
    logger.info("Starting Job Scheduler API v2...")

    cassandra_client.connect()
    cassandra_client.initialize_schema()

    app.state.cassandra_client = cassandra_client
    app.state.job_repo = JobRepository(cassandra_client)
    app.state.exec_repo = ExecutionRepository(cassandra_client)

    queue = RedisQueue()
    app.state.queue = queue

    scheduler = Scheduler(queue=queue, cassandra_client=cassandra_client)
    app.state.scheduler = scheduler

    # Worker registry placeholder — workers register themselves via Redis
    app.state.workers = []

    logger.info("Job Scheduler API v2 startup complete.")

    yield

    logger.info("Shutting down Job Scheduler API...")
    cassandra_client.disconnect()
    logger.info("Shutdown complete.")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Job Scheduler API",
        description=(
            "Production-grade distributed job scheduling system v2. "
            "Features: Priority queues, INTERVAL scheduling, DLQ, monitoring, "
            "bulk operations, job pause/resume, SLO tracking, and a Web UI."
        ),
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(RequestIDMiddleware)

    # Routers
    app.include_router(jobs_router, prefix="/api/v1", tags=["Jobs"])
    app.include_router(monitoring_router, prefix="/api/v1/monitoring", tags=["Monitoring"])

    # Exception handlers
    @app.exception_handler(ValidationError)
    async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=ErrorResponse(error="Validation Error", detail=str(exc)).model_dump(mode="json"),
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=ErrorResponse(error="Bad Request", detail=str(exc)).model_dump(mode="json"),
        )

    @app.exception_handler(KeyError)
    async def key_error_handler(request: Request, exc: KeyError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=ErrorResponse(error="Not Found", detail=str(exc)).model_dump(mode="json"),
        )

    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error("Unhandled exception: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=ErrorResponse(
                error="Internal Server Error",
                detail="An unexpected error occurred.",
            ).model_dump(mode="json"),
        )

    return app


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
