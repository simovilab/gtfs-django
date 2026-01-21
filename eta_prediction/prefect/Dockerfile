FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PREFECT_HOME=/opt/prefect \
    PYTHONPATH=/app \
    MODEL_REGISTRY_DIR=/app/models/trained

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies directly (without full project install)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    prefect>=3.0 \
    redis>=5.0 \
    pandas>=2.3.3 \
    numpy>=1.26 \
    scikit-learn>=1.7.2 \
    xgboost>=3.1.2 \
    pydantic>=2.0 \
    fastparquet>=2024.11.0 \
    httpx>=0.27.0

# Copy only the necessary Python modules
COPY core /app/core
COPY eta_service /app/eta_service
COPY models /app/models
COPY prefect /app/prefect

# Create models/trained directory if it doesn't exist
RUN mkdir -p /app/models/trained

CMD ["bash"]
