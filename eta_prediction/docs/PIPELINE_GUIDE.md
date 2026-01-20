# ETA Prediction Pipeline Guide

End-to-end guide from data ingestion to runtime inference using Docker.

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ETA PREDICTION PIPELINE                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. DATA INGESTION          2. FEATURE ENGINEERING      3. MODEL TRAINING   │
│  ┌─────────────────┐        ┌─────────────────────┐     ┌────────────────┐  │
│  │ GTFS-RT Feed    │        │ Build Dataset       │     │ Train Models   │  │
│  │ (Vehicle Pos)   │───────▶│ - Temporal features │────▶│ - XGBoost      │  │
│  │      ↓          │        │ - Spatial features  │     │ - PolyReg      │  │
│  │ Django + Celery │        │ - Weather features  │     │ - EWMA         │  │
│  │      ↓          │        └─────────────────────┘     │ - Hist. Mean   │  │
│  │ PostgreSQL      │                                    └────────────────┘  │
│  └─────────────────┘                                            │           │
│                                                                 ▼           │
│  4. RUNTIME INFERENCE                                   ┌────────────────┐  │
│  ┌─────────────────────────────────────────────────┐    │ Model Registry │  │
│  │                                                 │    │ (models/trained)│  │
│  │  MQTT ──▶ Redis ──▶ Prefect/Bytewax ──▶ Redis   │◀───└────────────────┘  │
│  │  (live)   (cache)   (inference)        (preds)  │                        │
│  │                                                 │                        │
│  └─────────────────────────────────────────────────┘                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- Docker & Docker Compose
- Git

---

## Quick Start (Use Existing Models)

If you just want to run inference with pre-trained models:

```bash
# Clone and enter the project
git clone https://github.com/simovilab/gtfs-django.git
cd gtfs-django/eta_prediction

# Build and verify
make build
make verify

# Start Redis and run a prediction
make redis
make shell
```

Inside the container:
```python
from eta_service.estimator import estimate_stop_times

result = estimate_stop_times(
    vehicle_position={
        "vehicle_id": "BUS-001",
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
    print(f"Stop {pred['stop_id']}: {pred['eta_formatted']} ({pred['distance_to_stop_m']:.0f}m)")
```

---

## Full Pipeline

### Phase 1: Data Ingestion (gtfs-rt-pipeline)

The Django + Celery pipeline ingests GTFS-RT feeds into PostgreSQL.

```bash
cd gtfs-rt-pipeline

# Copy environment template
cp .env.example .env

# Edit .env with your feeds/credentials

# Start infra
docker compose up -d postgres redis

# (Postgres container ships with PostGIS via the postgis/postgis:16-3.4 image)

# Run migrations + create a superuser
docker compose run --rm web python manage.py migrate
docker compose run --rm web python manage.py createsuperuser

# Launch web + Celery worker/beat
docker compose up -d web celery-worker celery-beat
```

Data flows:
- `fetch_vehicle_positions` task downloads `.pb` feed
- Deduplicates via SHA-256 hash
- Parses protobuf into `VehiclePosition` model
- Stores in PostgreSQL

`celery-beat` triggers the periodic fetch according to `POLL_SECONDS`.  
**Verify ingestion:**
```bash
docker compose exec web python manage.py shell
>>> from rt_pipeline.models import VehiclePosition
>>> VehiclePosition.objects.count()
```

---

### Phase 2: Feature Engineering

Build training datasets from ingested data.

```bash
cd eta_prediction
make shell
```

Inside container:
```python
from feature_engineering.dataset_builder import build_vp_training_dataset, save_dataset
from datetime import datetime, timezone

# Build dataset for specific routes and date range
df = build_vp_training_dataset(
    route_ids=["171", "172"],  # Or None for all routes
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 7, tzinfo=timezone.utc),
    max_stops_ahead=5,
    attach_weather=True,
    tz_for_temporal="America/Costa_Rica"
)

print(f"Dataset shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# Save dataset
save_dataset(df, "/app/datasets/my_dataset.parquet")
```

**Dataset columns:**
| Category | Columns |
|----------|---------|
| Identity | trip_id, route_id, vehicle_id, stop_id, stop_sequence |
| Telemetry | vp_ts, vp_lat, vp_lon, vp_bearing, vp_speed |
| Geometry | stop_lat, stop_lon, distance_to_stop, progress_ratio |
| Target | actual_arrival, time_to_arrival_seconds |
| Temporal | hour, day_of_week, is_weekend, is_holiday, is_peak_hour |
| Weather | temperature_c, precipitation_mm, wind_speed_kmh |

---

### Phase 3: Model Training

Train models on your dataset.

```bash
make shell
```

Inside container:
```bash
# Train all model types on a dataset
python models/train_all_models.py --dataset my_dataset --models all

# Train specific models
python models/train_all_models.py --dataset my_dataset --models xgboost polyreg_time

# Train route-specific models
python models/train_all_models.py --dataset my_dataset --by-route --models xgboost
```

**Or train programmatically:**
```python
from models.xgb.train import train_xgboost_model
from models.common.registry import get_registry

# Train XGBoost model
model_key = train_xgboost_model(
    dataset_name="my_dataset",
    use_temporal=True,
    use_spatial=True,
    max_depth=5,
    n_estimators=200,
    learning_rate=0.05
)

# Check registry
registry = get_registry()
print(registry.list_models())

# Get best model
best = registry.get_best_model(model_type="xgboost", metric="test_mae_seconds")
print(f"Best XGBoost: {best}")
```

**Model comparison:**
```python
from models.evaluation.leaderboard import quick_compare

# Compare models on test set
results = quick_compare(
    model_keys=["xgboost_...", "polyreg_time_...", "historical_mean_..."],
    dataset_name="my_dataset"
)
print(results)
```

---

### Phase 4: Runtime Inference

Two options: Prefect (orchestrated) or Bytewax (low-latency).

#### Option A: Prefect Flow

```bash
# Start the full stack
docker compose --profile prefect up

# Or run manually
make redis
docker compose run --rm eta python prefect/prefect_eta_flow.py \
    --redis-host redis \
    --redis-port 6379 \
    --iterations 0 \
    --poll-interval 1.0
```

The flow:
1. Polls Redis for `vehicle:*` keys
2. Fetches `route_stops:<route_id>` for each vehicle
3. Calls `estimate_stop_times()` for each vehicle
4. Writes predictions to `predictions:<vehicle_id>`

#### Option B: Bytewax Flow

```bash
cd bytewax
docker compose up -d redis

# Seed mock stops/shapes
docker compose run --rm bytewax python mock_stops_and_shapes.py

# Run the flow
docker compose run --rm bytewax python -m bytewax.run pred2redis
```

#### Seeding Test Data

To test without live MQTT data:

```bash
make shell
python -c "
import redis
import json
from datetime import datetime, timezone

r = redis.Redis(host='redis', port=6379, decode_responses=True)

# Seed a vehicle position
vehicle = {
    'vehicle_id': 'TEST-001',
    'lat': 9.9281,
    'lon': -84.0907,
    'speed': 10.5,
    'timestamp': datetime.now(timezone.utc).isoformat(),
    'route': '171',
    'trip_id': 'trip_001'
}
r.set('vehicle:TEST-001', json.dumps(vehicle))

# Seed route stops
stops = {
    'stops': [
        {'stop_id': 'S001', 'lat': 9.9291, 'lon': -84.0897, 'stop_sequence': 1},
        {'stop_id': 'S002', 'lat': 9.9301, 'lon': -84.0887, 'stop_sequence': 2},
        {'stop_id': 'S003', 'lat': 9.9311, 'lon': -84.0877, 'stop_sequence': 3},
    ]
}
r.set('route_stops:171', json.dumps(stops))

print('Test data seeded!')
print('Vehicle keys:', list(r.scan_iter('vehicle:*')))
print('Route keys:', list(r.scan_iter('route_stops:*')))
"
```

#### Reading Predictions

```bash
make shell
python -c "
import redis
import json

r = redis.Redis(host='redis', port=6379, decode_responses=True)

# List all predictions
for key in r.scan_iter('predictions:*'):
    pred = json.loads(r.get(key))
    print(f'\\n{key}:')
    print(f'  Model: {pred.get(\"model_type\")}')
    print(f'  Computed: {pred.get(\"computed_at\")}')
    for p in pred.get('predictions', []):
        print(f'  Stop {p[\"stop_id\"]}: {p[\"eta_formatted\"]} ({p[\"distance_to_stop_m\"]:.0f}m)')
"
```

---

## Docker Commands Reference

| Command | Description |
|---------|-------------|
| `make build` | Build Docker image |
| `make verify` | Full verification (tests + estimator) |
| `make up` | Start Redis + run tests |
| `make redis` | Start Redis only |
| `make shell` | Interactive shell in container |
| `make prefect` | Start Prefect flow |
| `make test` | Run core tests |
| `make clean` | Remove containers and volumes |

---

## Troubleshooting

**Models not loading:**
```bash
# Check model registry
make shell
python -c "
from models.common.registry import get_registry
r = get_registry()
print(f'Registry path: {r.base_dir}')
print(f'Models: {len(r.registry)}')
print(r.list_models())
"
```

**Redis connection issues:**
```bash
# Check Redis is running
docker compose ps
docker compose logs redis

# Test connection
docker compose run --rm eta python -c "
import redis
r = redis.Redis(host='redis', port=6379)
print('PING:', r.ping())
"
```

**Missing dependencies:**
```bash
# Rebuild image
docker compose build --no-cache eta
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_HOST` | Redis hostname | `redis` (in Docker) |
| `REDIS_PORT` | Redis port | `6379` |
| `MODEL_REGISTRY_DIR` | Model storage path | `/app/models/trained` |
| `ETA_TIMEZONE` | Default timezone | `America/Costa_Rica` |
| `ETA_DEBUG` | Enable debug logging | `0` |
| `ETA_LOG_JSON` | JSON log output | `0` |
