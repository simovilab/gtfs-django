# ETA Prediction System

Real-time arrival time prediction for transit vehicles using GTFS-Realtime feeds and machine learning.

## Overview

This system ingests live vehicle positions from GTFS-RT feeds, enriches them with temporal/spatial features, runs inference through trained ML models, and publishes predictions to Redis for downstream consumption.

```
GTFS-RT Feed → Redis Cache → Stream Processor → ML Models → Predictions → Redis
                              (Bytewax/Prefect)   (XGBoost)
```

## Architecture

```
├── core/                    # Shared utilities (config, logging, validation, exceptions)
├── eta_service/             # Prediction orchestrator (estimate_stop_times)
├── feature_engineering/     # Temporal, spatial, weather feature extraction
├── models/                  # ML model families + registry + evaluation
│   ├── historical_mean/     # Baseline lookup tables
│   ├── polyreg_distance/    # Distance-only polynomial regression
│   ├── polyreg_time/        # Distance + temporal/spatial regression
│   ├── ewma/                # Exponential smoothing with online updates
│   ├── xgb/                 # Gradient boosted trees
│   └── trained/             # Model artifacts (.pkl + metadata)
├── bytewax/                 # Low-latency stream processing
├── prefect/                 # Orchestrated batch processing
├── gtfs-rt-pipeline/        # Django + Celery ingestion pipeline
└── datasets/                # Training data (parquet)
```

## Quick Start (Docker)

The fastest way to get started on any machine.

### Prerequisites

- Docker
- Docker Compose
- Git

### 1. Clone and Setup

```bash
git clone https://github.com/simovilab/gtfs-django.git
cd gtfs-django/eta_prediction

# Copy environment configuration
cp .env.example .env

# Build all images
docker compose -f docker-compose.full.yml build
```

### 2. Run the Full Pipeline

The full pipeline includes: PostgreSQL, Redis, Django, Celery, ETA Service, MQTT, and Bytewax.

```bash
# Start core services (database, cache, ingestion)
docker compose -f docker-compose.full.yml up -d

# Start with stream processing (adds MQTT broker + Bytewax)
docker compose -f docker-compose.full.yml --profile streaming up -d

# Start with scheduled ingestion (adds Celery Beat)
docker compose -f docker-compose.full.yml --profile ingestion up -d

# Start everything (ingestion + streaming + runtime inference)
docker compose -f docker-compose.full.yml \
  --profile ingestion \
  --profile streaming \
  --profile runtime up -d
```

### 3. Interactive CLI

Use the `eta-cli` service for feature engineering, training, and manual inference:

```bash
# Start interactive shell
docker compose -f docker-compose.full.yml run eta-cli bash

# Inside the container:
python -c "from eta_service.estimator import estimate_stop_times; print('OK')"
python models/train_all_models.py --dataset sample_dataset --models xgboost
```

### 4. Seed Test Data

For testing without live feeds:

```bash
# Seed mock stops and shapes into Redis
docker compose -f docker-compose.full.yml --profile seed run cache-seeder
```

### Docker Compose Profiles

| Profile | Services Added | Use Case |
|---------|---------------|----------|
| (default) | postgres, redis, django, celery-worker | Core ingestion |
| `ingestion` | + celery-beat | Scheduled feed polling |
| `streaming` | + mqtt, mqtt-subscriber, bytewax | Real-time stream processing |
| `runtime` | + prefect-flow | Prefect-orchestrated inference |
| `seed` | cache-seeder (one-shot) | Populate Redis with test data |

### Quick Commands

| Command | Description |
|---------|-------------|
| `make build` | Build Docker image (basic) |
| `make up` | Start Redis + run tests |
| `make test` | Run test suite |
| `make shell` | Interactive shell in container |
| `make prefect` | Start Prefect flow |
| `make dev` | Development mode with hot reload |
| `make clean` | Remove containers and volumes |

---

## Quick Start (Local)

For development without Docker.

### Installation

```bash
# Clone and install dependencies
git clone https://github.com/simovilab/gtfs-django.git
cd gtfs-django/eta_prediction
uv sync
```

### Run Predictions

```python
from eta_service.estimator import estimate_stop_times

result = estimate_stop_times(
    vehicle_position={
        "vehicle_id": "BUS-042",
        "lat": 9.9281,
        "lon": -84.0907,
        "timestamp": "2024-01-15T10:00:00Z",
        "route": "171"
    },
    upcoming_stops=[
        {"stop_id": "S001", "lat": 9.9291, "lon": -84.0897, "stop_sequence": 5},
        {"stop_id": "S002", "lat": 9.9301, "lon": -84.0887, "stop_sequence": 6},
    ],
    route_id="171",
    max_stops=3
)

for pred in result["predictions"]:
    print(f"{pred['stop_id']}: {pred['eta_formatted']}")
```

### Stream Processing

**Bytewax (low-latency):**
```bash
cd bytewax
uv sync
uv run python -m bytewax.run pred2redis
```

**Prefect (orchestrated):**
```bash
cd prefect
uv sync
uv run python prefect_eta_flow.py --iterations 1
```

## Components

### Core Utilities (`core/`)

Shared infrastructure for configuration, logging, validation, and error handling.

```python
from core.config import get_config
from core.logging import get_logger
from core.validation import validate_vehicle_position

config = get_config()
logger = get_logger("my-component")
logger.info("Processing", vehicle_id="V123", route="171")
```

See [CHANGELOG.md](CHANGELOG.md) for details.

### ETA Service (`eta_service/`)

Production interface for generating ETA predictions. Bridges real-time data with trained models.

- `estimate_stop_times()` - Main prediction function
- Smart model selection (route-specific → global → fallback)
- Temporal + spatial feature extraction

### Feature Engineering (`feature_engineering/`)

Transforms raw telemetry into model-ready features:

- **Temporal**: hour, day_of_week, is_peak_hour, is_holiday
- **Spatial**: distance_to_stop, progress_ratio, segment_progress
- **Weather**: temperature, precipitation, wind (via Open-Meteo)

### Models (`models/`)

Five model families with consistent train/predict interfaces:

| Model | Use Case |
|-------|----------|
| Historical Mean | Baseline, cold-start |
| Polynomial (Distance) | Simple distance-based regression |
| Polynomial (Time) | Distance + temporal/spatial features |
| EWMA | Online learning, non-stationary patterns |
| XGBoost | Production model, nonlinear interactions |

Training:
```bash
python models/train_all_models.py --dataset sample_dataset --models xgboost
```

### Stream Processing

**Bytewax** (`bytewax/`): Low-latency dataflow for real-time predictions.

**Prefect** (`prefect/`): Orchestrated polling loop with profiling, notifications, and artifact management.

### Data Ingestion (`gtfs-rt-pipeline/`)

Django + Celery pipeline for ingesting GTFS-RT feeds into PostgreSQL.

## Testing

**With Docker (recommended):**
```bash
make test           # Core tests
make test-all       # Full test suite
```

**Local:**
```bash
.venv/bin/python -m pytest core/tests/test_core.py -v
uv run pytest
```

## Environment Variables

Copy `.env.example` to `.env` and customize as needed.

**Core ETA Service:**
| Variable | Description | Default |
|----------|-------------|---------|
| `MODEL_REGISTRY_DIR` | Path to trained models | `models/trained` |
| `ETA_TIMEZONE` | Default timezone | `America/Costa_Rica` |
| `ETA_DEBUG` | Enable debug logging | `0` |
| `ETA_LOG_JSON` | JSON log output | `0` |

**GTFS-RT Ingestion:**
| Variable | Description | Default |
|----------|-------------|---------|
| `GTFSRT_VEHICLE_POSITIONS_URL` | Vehicle positions feed URL | MBTA feed |
| `GTFSRT_TRIP_UPDATES_URL` | Trip updates feed URL | MBTA feed |
| `POLL_SECONDS` | Feed polling interval | `30` |
| `DATABASE_URL` | PostgreSQL connection string | - |

**Redis & MQTT:**
| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_HOST` | Redis hostname | `redis` |
| `REDIS_PORT` | Redis port | `6379` |
| `MQTT_HOST` | MQTT broker hostname | `mqtt` |
| `MQTT_PORT` | MQTT broker port | `1883` |
| `MQTT_TOPIC` | MQTT topic pattern | `transit/vehicles/bus/#` |

## Dependencies

**Docker (recommended):**
- Docker
- Docker Compose
- Git

**Local development:**
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) for dependency management
- Redis (for caching and stream processing)
- PostgreSQL (optional, for GTFS schedule data)

## License

See LICENSE file.
