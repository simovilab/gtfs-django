FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PREFECT_HOME=/opt/prefect \
    PYTHONPATH=/app:/app/prefect \
    MODEL_REGISTRY_DIR=/app/models/trained \
    UV_SYSTEM_PYTHON=1

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip \
    && pip install --no-cache-dir uv \
    && uv pip install --system ./prefect

CMD ["bash"]
