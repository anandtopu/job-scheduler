# Job Scheduler

This is a production-grade distributed job scheduling system built with Python, FastAPI, Apache Cassandra, and Redis.

## Architecture Overview

```
                          ┌─────────────────────────────────────┐
                          │           CLIENT / USER              │
                          └──────────────────┬──────────────────┘
                                             │ REST API
                          ┌──────────────────▼──────────────────┐
                          │            FastAPI (API)              │
                          │   POST /jobs  GET /jobs  GET /health  │
                          └──────┬───────────────────────┬───────┘
                                 │                       │
                    ┌────────────▼──────┐       ┌───────▼──────────┐
                    │   Cassandra DB     │       │  Redis Queue      │
                    │                   │       │                   │
                    │  jobs table        │       │  queue:pending    │
                    │  executions table  │       │  (sorted set,     │
                    │  user_executions   │◄──────│   score=exec_ts)  │
                    └────────────┬──────┘       └───────┬──────────┘
                                 │ poll every 5min       │ consume
                    ┌────────────▼──────┐       ┌───────▼──────────┐
                    │    Scheduler       │       │   Worker (N)      │
                    │                   │       │                   │
                    │  APScheduler       │       │  ThreadPoolExec   │
                    │  croniter          │──────►│  Heartbeat thread │
                    │  Lookahead: 6 min  │       │  Retry w/ backoff │
                    └───────────────────┘       └──────────────────┘
```

### Two-Layer Scheduling Design

**Layer 1 - DB Polling (Durability):**
The Scheduler polls Cassandra every 5 minutes for jobs due within the next 6 minutes.
This ensures no jobs are missed even if the scheduler restarts.

**Layer 2 - Redis Queue (Precision):**
Jobs are pushed into a Redis sorted set with `score = execution_unix_timestamp`.
Workers dequeue only jobs whose score <= now, enabling millisecond-precision delivery.

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- (For K8s deployment) kubectl + a Kubernetes cluster

## Quick Start (Docker Compose)

```bash
# 1. Clone and configure
cp .env.example .env

# 2. Build and start all services
docker-compose up -d

# 3. Wait for Cassandra to be ready (~60s), then initialize the schema
docker-compose exec api python scripts/init_db.py

# 4. The API is now available at http://localhost:8000
# Swagger UI: http://localhost:8000/docs
```

## Configuration Reference

All settings can be set via environment variables or in the `.env` file.

| Variable | Default | Description |
|---|---|---|
| `CASSANDRA_HOSTS` | `localhost` | Comma-separated Cassandra contact points |
| `CASSANDRA_PORT` | `9042` | Cassandra CQL port |
| `CASSANDRA_KEYSPACE` | `job_scheduler` | Cassandra keyspace name |
| `CASSANDRA_REPLICATION_FACTOR` | `1` | Replication factor (use 3 in production) |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis database number |
| `QUEUE_VISIBILITY_TIMEOUT` | `30` | Seconds before unACK'd job is redelivered |
| `QUEUE_HEARTBEAT_INTERVAL` | `15` | Seconds between heartbeats for in-flight jobs |
| `SCHEDULER_POLL_INTERVAL` | `300` | DB poll interval in seconds (5 minutes) |
| `SCHEDULER_LOOKAHEAD_SECONDS` | `360` | How far ahead to enqueue jobs (6 minutes) |
| `WORKER_CONCURRENCY` | `10` | Concurrent job execution threads per worker |
| `WORKER_MAX_RETRIES` | `3` | Maximum retry attempts before marking FAILED |
| `WORKER_RETRY_BASE_DELAY` | `5` | Base delay for exponential backoff (seconds) |
| `API_HOST` | `0.0.0.0` | API bind address |
| `API_PORT` | `8000` | API port |
| `LOG_LEVEL` | `INFO` | Logging level |

## API Documentation

### Create a Job

```bash
POST /api/v1/jobs
X-User-Id: user-123
Content-Type: application/json

# Immediate execution
{
  "task_id": "log_message",
  "schedule_type": "IMMEDIATE",
  "parameters": {"message": "Hello!", "level": "INFO"}
}

# Specific datetime
{
  "task_id": "http_request",
  "schedule_type": "DATETIME",
  "schedule_expression": "2024-12-25T09:00:00",
  "parameters": {"url": "https://api.example.com/notify", "method": "POST"}
}

# CRON recurring
{
  "task_id": "send_email",
  "schedule_type": "CRON",
  "schedule_expression": "0 9 * * 1-5",
  "parameters": {"to": "admin@example.com", "subject": "Daily Report", "body": "..."}
}
```

### List Jobs

```bash
GET /api/v1/jobs?user_id=user-123&status=ACTIVE&limit=20&page=1
```

### Get Job Details

```bash
GET /api/v1/jobs/{job_id}
```

### Get Job Executions

```bash
GET /api/v1/jobs/{job_id}/executions?status=COMPLETED&limit=20
```

### Health Check

```bash
GET /api/v1/health
```

## Built-in Tasks

| Task ID | Description | Parameters |
|---|---|---|
| `log_message` | Logs a message | `message` (str), `level` (str: DEBUG/INFO/WARNING/ERROR) |
| `http_request` | Makes an HTTP request | `url` (str), `method` (str), `headers` (dict), `body` (dict) |
| `send_email` | Simulates email sending | `to` (str), `subject` (str), `body` (str) |

## Running Locally (Without Docker)

```bash
# 1. Install dependencies
make dev

# 2. Start Cassandra and Redis (via Docker)
docker run -d --name cassandra -p 9042:9042 cassandra:4.1
docker run -d --name redis -p 6379:6379 redis:7-alpine

# 3. Initialize schema
make init-db

# 4. Start services in separate terminals
make run-api        # Terminal 1: FastAPI
make run-scheduler  # Terminal 2: Scheduler
make run-worker     # Terminal 3: Worker
```

## Running Tests

```bash
# Run unit tests (no external services required)
make test

# Run with coverage report
make test-cov

# Run integration tests (requires Cassandra + Redis)
docker-compose up -d cassandra redis
make test-integration
```

## Cloud Deployment (Kubernetes)

```bash
# 1. Build and push Docker images
docker build -t your-registry/job-scheduler-api:latest -f docker/api.Dockerfile .
docker build -t your-registry/job-scheduler-scheduler:latest -f docker/scheduler.Dockerfile .
docker build -t your-registry/job-scheduler-worker:latest -f docker/worker.Dockerfile .
docker push your-registry/job-scheduler-api:latest
# ... push others

# 2. Update image names in k8s/*.yaml

# 3. Apply manifests
make k8s-apply

# 4. Initialize Cassandra schema (run once)
kubectl exec -it cassandra-0 -n job-scheduler -- python /app/scripts/init_db.py

# 5. Check pod status
kubectl get pods -n job-scheduler
```

## Architecture Deep Dive

### Two-Layer Scheduler

The system uses a two-layer approach to balance durability and precision:

```
DB (Cassandra) ──► Executions table (source of truth)
                        │
                        │  Scheduler polls every 5 min
                        ▼
Redis Sorted Set ──► queue:pending (score = execution_unix_ts)
                        │
                        │  Worker dequeues when score <= now
                        ▼
                   Execute task
```

**Why two layers?**
- If the scheduler crashes and restarts, it re-reads from Cassandra (no lost jobs)
- Redis provides sub-second precision for job delivery
- The lookahead window (6 min > poll interval 5 min) ensures continuity

### Redis Queue Design

```
queue:pending   (sorted set)
  member: {job_id, execution_key, ...}
  score:  execution_unix_timestamp

queue:processing (sorted set)
  member: {job_id, execution_key, ...}
  score:  visibility_deadline (now + 30s)
```

**Dequeue flow (atomic Lua script):**
1. `ZRANGEBYSCORE queue:pending -inf now LIMIT 0 N` - find ready jobs
2. `ZREM queue:pending` - remove from pending
3. `ZADD queue:processing score=now+30s` - add to processing

**ACK flow:** `ZREM queue:processing message`

**Stuck job recovery:** A background thread periodically calls `requeue_stuck_jobs()`, which moves jobs with expired visibility deadlines back to pending.

### Retry Logic

Failed jobs use exponential backoff:
- Attempt 1 → delay = 5^1 = 5 seconds
- Attempt 2 → delay = 5^2 = 25 seconds
- Attempt 3 → delay = 5^3 = 125 seconds
- After max_retries → marked as FAILED

### Cassandra Schema Design

**executions table:** Partitioned by `time_bucket` (hourly unix timestamp). This enables efficient range queries: "give me all executions between time A and time B" by querying only the relevant hourly buckets.

**user_executions table:** Denormalized copy partitioned by `user_id` for fast "show me all executions for user X" queries without ALLOW FILTERING.

## Edge Cases Handled

| Edge Case | Handling |
|---|---|
| Job scheduled in the past | Clamped to now, executed immediately |
| Invalid CRON expression | 422 error returned at API layer |
| Worker crash mid-execution | Visibility timeout expires, job redelivered |
| Duplicate enqueue | Redis dedup key (TTL=2h) prevents double execution |
| Cassandra failure | Retry with exponential backoff (up to 10 attempts) |
| Redis failure | retry_on_timeout=True, error logged, message preserved |
| Task not registered | 400 error at API layer |
| Parameters > 64KB | 422 validation error |
| CRON job never fires | Validation error at job creation |

## Scalability Considerations

- **API**: Stateless, horizontally scalable behind a load balancer (HPA: 2-10 replicas)
- **Scheduler**: Single instance to avoid double-scheduling (Recreate strategy)
- **Workers**: Horizontally scalable (HPA: 3-20 replicas); Redis queue handles fan-out
- **Cassandra**: 3-node cluster with RF=3; time-bucketed partitions prevent hotspots
- **Redis**: Single instance sufficient for most workloads; Redis Cluster for massive scale
- **Queue throughput**: Workers dequeue in batches and execute concurrently (10 threads/worker)
#   j o b - s c h e d u l e r 
 
 