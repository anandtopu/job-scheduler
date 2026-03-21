# Multi-stage build for the Job Worker service
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libev-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt


FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libev4 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /root/.local /root/.local

COPY src/ ./src/
COPY scripts/ ./scripts/

ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Worker entry point: register all built-in tasks, then run the worker loop
ENTRYPOINT ["python", "-c", "\
import logging; \
logging.basicConfig(level='INFO'); \
from src.tasks.builtin import log_task, http_task, email_task; \
from src.db.cassandra import cassandra_client; \
from src.queue.redis_queue import RedisQueue; \
from src.worker.worker import Worker; \
cassandra_client.connect(); \
queue = RedisQueue(); \
worker = Worker(queue=queue, cassandra_client=cassandra_client); \
worker.run() \
"]
