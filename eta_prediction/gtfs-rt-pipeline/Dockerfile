# Django + Celery for GTFS-RT Ingestion (self-contained)
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        ca-certificates \
        libpq-dev \
        gdal-bin \
        libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
    && pip install --no-cache-dir uv

WORKDIR /app

# Install project dependencies (editable install so migrations/celery see code)
COPY pyproject.toml uv.lock* /app/
RUN uv pip install --system -e .

# Copy project source
COPY . /app

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
