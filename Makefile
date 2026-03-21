.PHONY: install dev test test-cov lint format docker-up docker-down k8s-apply k8s-delete init-db clean

PYTHON := python
PIP := pip
PYTEST := pytest
PROJECT := job-scheduler

# Install production dependencies
install:
	$(PIP) install -r requirements.txt

# Install development dependencies
dev:
	$(PIP) install -r requirements-dev.txt

# Run unit tests
test:
	$(PYTEST) tests/unit/ -v --tb=short

# Run all tests with coverage report
test-cov:
	$(PYTEST) tests/ -v --cov=src --cov-report=term-missing --cov-report=html --ignore=tests/integration/

# Run integration tests (requires Cassandra + Redis)
test-integration:
	$(PYTEST) tests/integration/ -v --tb=short

# Lint with ruff
lint:
	ruff check src/ tests/
	mypy src/ --ignore-missing-imports

# Format with black and ruff
format:
	black src/ tests/
	ruff check --fix src/ tests/

# Start all services with Docker Compose
docker-up:
	docker-compose up -d
	@echo "Waiting for Cassandra to be ready..."
	@sleep 30
	docker-compose exec api python scripts/init_db.py

# Stop all Docker Compose services
docker-down:
	docker-compose down -v

# Build Docker images without starting
docker-build:
	docker-compose build

# View logs
docker-logs:
	docker-compose logs -f

# Apply Kubernetes manifests
k8s-apply:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/cassandra/
	kubectl apply -f k8s/redis/
	@echo "Waiting for Cassandra to be ready..."
	kubectl wait --for=condition=ready pod -l app=cassandra -n job-scheduler --timeout=300s
	kubectl apply -f k8s/api/
	kubectl apply -f k8s/scheduler/
	kubectl apply -f k8s/worker/

# Delete Kubernetes resources
k8s-delete:
	kubectl delete -f k8s/ --recursive --ignore-not-found

# Initialize Cassandra schema
init-db:
	$(PYTHON) scripts/init_db.py

# Run the API server locally
run-api:
	$(PYTHON) -m uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload

# Run the scheduler locally
run-scheduler:
	$(PYTHON) -c "from src.db.cassandra import cassandra_client; from src.queue.redis_queue import RedisQueue; from src.scheduler.scheduler import Scheduler; cassandra_client.connect(); cassandra_client.initialize_schema(); q = RedisQueue(); s = Scheduler(q, cassandra_client); s.run()"

# Run a worker locally
run-worker:
	$(PYTHON) -c "from src.db.cassandra import cassandra_client; from src.queue.redis_queue import RedisQueue; from src.worker.worker import Worker; from src.tasks.builtin import log_task, http_task, email_task; cassandra_client.connect(); q = RedisQueue(); w = Worker(q, cassandra_client); w.run()"

# Clean build artifacts and caches
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov .mypy_cache dist build *.egg-info
