# ETA Prediction System - Makefile
#
# Quick Start:
#   make build     # Build Docker image
#   make test      # Run tests
#   make up        # Start full stack
#   make shell     # Interactive shell in container

.PHONY: help build up down test shell logs clean prefect dev

# Default target
help:
	@echo "ETA Prediction System"
	@echo ""
	@echo "Usage:"
	@echo "  make build      Build Docker image"
	@echo "  make up         Start Redis + run tests"
	@echo "  make down       Stop all services"
	@echo "  make test       Run test suite"
	@echo "  make shell      Interactive shell in container"
	@echo "  make logs       View service logs"
	@echo "  make prefect    Start Prefect flow"
	@echo "  make dev        Development mode with hot reload"
	@echo "  make clean      Remove containers and volumes"
	@echo ""
	@echo "Redis only:"
	@echo "  make redis      Start Redis only"
	@echo "  make redis-cli  Connect to Redis CLI"

# Build the Docker image
build:
	docker compose build

# Start Redis and run default (tests)
up: build
	docker compose up

# Start Redis only
redis:
	docker compose up -d redis

# Connect to Redis CLI
redis-cli:
	docker compose exec redis redis-cli

# Stop all services
down:
	docker compose down

# Run tests
test: build
	docker compose run --rm eta pytest core/tests/test_core.py -v

# Run all tests
test-all: build
	docker compose run --rm eta pytest -v

# Interactive shell
shell: build
	docker compose run --rm eta bash

# View logs
logs:
	docker compose logs -f

# Start Prefect flow
prefect: build
	docker compose --profile prefect up

# Development mode
dev: build
	docker compose --profile dev run --rm eta-dev

# Clean up everything
clean:
	docker compose down -v --rmi local
	docker system prune -f

# Quick verification that everything works
verify: build
	@echo "==> Starting Redis..."
	docker compose up -d redis
	@echo "==> Waiting for Redis..."
	sleep 2
	@echo "==> Running tests..."
	docker compose run --rm eta pytest core/tests/test_core.py -v
	@echo "==> Testing estimator import..."
	docker compose run --rm eta python -c "from eta_service.estimator import estimate_stop_times; print('Estimator OK')"
	@echo "==> Cleaning up..."
	docker compose down
	@echo ""
	@echo "All checks passed!"
