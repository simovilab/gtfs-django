# Django + Celery for GTFS-RT ingestion.
#
# IMPORTANT: build context is the gtfs-django repo ROOT (see docker-compose.yml),
# not this directory. The ingest project depends on the editable gtfs-django
# package at `../..` and imports the sibling `feature_engineering` module, so the
# repo layout must be preserved inside the image:
#   /app                                  -> gtfs-django repo root (gtfs/ + pyproject)
#   /app/eta_prediction/gtfs-rt-pipeline  -> this ingest project
#   /app/eta_prediction/feature_engineering
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
        binutils \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
    && pip install --no-cache-dir uv

# --- gtfs-django package (target of the editable path dep `gtfs-django @ ../..`) ---
WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY gtfs ./gtfs

# --- ingest project: install deps (resolves gtfs-django editable from /app) ---
WORKDIR /app/eta_prediction/gtfs-rt-pipeline
COPY eta_prediction/gtfs-rt-pipeline/pyproject.toml eta_prediction/gtfs-rt-pipeline/uv.lock* ./
RUN uv pip install --system -e .

# --- source: the feature builder (imported by build_eta_sample) + the ingest app ---
COPY eta_prediction/feature_engineering /app/eta_prediction/feature_engineering
COPY eta_prediction/gtfs-rt-pipeline/ ./

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
