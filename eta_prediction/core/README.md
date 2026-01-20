# Core Utilities

Shared infrastructure for the ETA prediction system. Provides centralized configuration, structured logging, input validation, and a consistent exception hierarchy.

## Modules

### `config.py` - Centralized Configuration

Eliminates scattered `sys.path` manipulation and provides a single source of truth for all project paths.

```python
from core.config import get_config

config = get_config()
print(config.project_root)         # /path/to/eta_prediction
print(config.model_registry_dir)   # /path/to/eta_prediction/models/trained
print(config.default_timezone)     # America/Costa_Rica
```

**Environment Variables:**
- `MODEL_REGISTRY_DIR` - Override model storage path
- `ETA_TIMEZONE` - Override default timezone

### `logging.py` - Structured Logging

Consistent logging across all components with support for structured fields.

```python
from core.logging import get_logger

logger = get_logger("my-component")
logger.info("Processing vehicle", vehicle_id="V123", route="171")
logger.warning("Missing stops", route_id="172")
logger.error("Prediction failed", model_key="xgb_global", exc_info=True)
```

Output:
```
2026-01-20 06:52:12 [I] [my-component] Processing vehicle | vehicle_id=V123 route=171
```

**Environment Variables:**
- `ETA_DEBUG=1` - Enable debug logging
- `ETA_LOG_JSON=1` - Output logs as JSON

### `validation.py` - Input Validation

Validates data at system boundaries (Redis payloads, API inputs).

```python
from core.validation import validate_vehicle_position, validate_stops_list
from core.exceptions import InvalidVehiclePositionError

try:
    vp = validate_vehicle_position({
        "id": "V123",
        "lat": 9.93,
        "lon": -84.08,
        "timestamp": "2024-01-15T10:00:00Z",
        "route": "171"
    })
    print(f"Vehicle {vp.vehicle_id} at ({vp.lat}, {vp.lon})")
except InvalidVehiclePositionError as e:
    print(f"Invalid: {e.message}")

# Validate stops
stops = validate_stops_list([
    {"stop_id": "S1", "lat": 9.93, "lon": -84.08},
    {"stop_id": "S2", "lat": 9.94, "lon": -84.07},
])
```

**Features:**
- Handles multiple field name conventions (`vehicle_id`, `vehicleId`, `id`)
- Returns typed dataclasses with `.to_dict()` for compatibility
- Validates coordinate ranges, required fields, timestamps

### `exceptions.py` - Custom Exceptions

Consistent exception hierarchy with error codes and context fields.

```python
from core.exceptions import ModelNotFoundError, InvalidVehiclePositionError

# Exceptions include context
raise ModelNotFoundError(model_type="xgboost", route_id="171")
# Output: [MODEL_NOT_FOUND] No xgboost model found for route 171 (model_type=xgboost, route_id=171)

# Convert to dict for JSON responses
try:
    # ... operation
except InvalidVehiclePositionError as e:
    return {"error": e.to_dict()}
```

**Exception Hierarchy:**
```
ETAError (base)
├── ConfigurationError
│   └── MissingConfigError
├── ValidationError
│   ├── InvalidVehiclePositionError
│   ├── InvalidStopError
│   └── InvalidRouteError
├── ModelError
│   ├── ModelNotFoundError
│   ├── ModelLoadError
│   └── ModelMetadataError
├── PredictionError
│   └── FeatureExtractionError
├── DataError
│   ├── DatasetError
│   └── MissingDataError
└── RedisError
    ├── RedisConnectionError
    └── RedisKeyError
```

## Testing

```bash
.venv/bin/python -m pytest core/tests/test_core.py -v
```

## Directory Structure

```
core/
├── __init__.py      # Central exports
├── config.py        # Path management & configuration
├── logging.py       # Structured logging
├── validation.py    # Input validation schemas
├── exceptions.py    # Custom exception hierarchy
├── README.md        # This file
└── tests/
    ├── __init__.py
    └── test_core.py # 25 test cases
```
