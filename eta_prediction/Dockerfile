# ETA Prediction System - Multi-purpose Docker Image
#
# Supports running:
#   - ETA estimator service
#   - Prefect flows
#   - Bytewax flows
#   - Model training
#   - Tests
#
# Build:
#   docker build -t eta-prediction .
#
# Run examples:
#   docker compose up                    # Full stack with Redis
#   docker compose run eta pytest        # Run tests
#   docker compose run eta python -c "from eta_service.estimator import estimate_stop_times; print('OK')"

FROM python:3.12-slim AS base

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    MODEL_REGISTRY_DIR=/app/models/trained \
    UV_SYSTEM_PYTHON=1 \
    ETA_TIMEZONE=America/Costa_Rica

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast Python package management
RUN pip install --upgrade pip \
    && pip install --no-cache-dir uv

WORKDIR /app

# Copy the application
COPY . .

# Install dependencies from pyproject.toml (not as editable - just deps)
RUN uv pip install --system \
    pandas>=2.3.3 \
    numpy>=1.26 \
    scikit-learn>=1.7.2 \
    xgboost>=3.1.2 \
    redis>=5.0 \
    requests>=2.32 \
    pytest \
    holidays

# Install prefect dependencies
RUN uv pip install --system \
    prefect>=3.0

# Create directories for runtime data
RUN mkdir -p /app/models/trained /app/datasets /app/profiling

# Default command - show help
CMD ["python", "-c", "print('ETA Prediction System\\n\\nUsage:\\n  pytest                              # Run tests\\n  python -m eta_service.estimator     # Test estimator\\n  python prefect/prefect_eta_flow.py  # Run Prefect flow\\n')"]
