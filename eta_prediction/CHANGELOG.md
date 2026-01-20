# Changelog

## [Unreleased] - 2026-01-20

### Added

#### New `core/` Module
A centralized utilities module providing foundational infrastructure for the ETA prediction system.

**`core/config.py`** - Centralized Configuration
- Single source of truth for all project paths (eliminates scattered `sys.path` manipulation)
- Environment variable support (`MODEL_REGISTRY_DIR`, `ETA_TIMEZONE`, `ETA_DEBUG`)
- Sensible defaults for magic numbers (TTLs, cache sizes, weather placeholders)
- Auto-discovery of project root via `pyproject.toml` or `.git`

**`core/logging.py`** - Structured Logging
- Consistent log formatting across all components
- Support for structured fields: `logger.info("msg", vehicle_id="V123", route="171")`
- Log levels: DEBUG, INFO, WARNING, ERROR
- JSON output mode for production (`ETA_LOG_JSON=1`)
- Automatic integration with Prefect's logger when running in Prefect context

**`core/validation.py`** - Input Validation
- `validate_vehicle_position(data)` - Validates vehicle payloads from Redis/MQTT
- `validate_stop(data)` - Validates stop definitions
- `validate_stops_list(data)` - Validates lists of stops
- Returns typed dataclasses (`VehiclePosition`, `Stop`) with `.to_dict()` for compatibility
- Handles multiple field name conventions (e.g., `vehicle_id`, `vehicleId`, `id`)

**`core/exceptions.py`** - Custom Exception Hierarchy
- `ETAError` - Base exception with error codes and context fields
- `ConfigurationError`, `MissingConfigError`
- `ValidationError`, `InvalidVehiclePositionError`, `InvalidStopError`
- `ModelError`, `ModelNotFoundError`, `ModelLoadError`
- `PredictionError`, `FeatureExtractionError`
- `RedisError`, `RedisConnectionError`, `RedisKeyError`
- All exceptions include `.to_dict()` for JSON serialization

**`core/tests/test_core.py`** - Test Suite
- 25 test cases covering all core modules
- Config, logging, validation, exceptions, and integration tests

---

### Changed

#### `eta_service/estimator.py`
- Replaced `sys.path` manipulation with `core.config` imports
- Replaced `print()` statements with structured logging via `core.logging`
- Timezone and weather defaults now read from config instead of hardcoded values

#### `models/common/registry.py`
- Replaced `print()` statements with structured logging
- Log messages now include structured fields for easier parsing

---

## Usage

### Configuration
```python
from core.config import get_config

config = get_config()
print(config.project_root)        # /path/to/eta_prediction
print(config.model_registry_dir)  # /path/to/eta_prediction/models/trained
print(config.default_timezone)    # America/Costa_Rica
```

### Logging
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

Enable debug logging:
```bash
ETA_DEBUG=1 python your_script.py
```

Enable JSON output:
```bash
ETA_LOG_JSON=1 python your_script.py
```

### Validation
```python
from core.validation import validate_vehicle_position, validate_stops_list
from core.exceptions import InvalidVehiclePositionError

# Validate vehicle position
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
    print(f"Invalid: {e.message} (code: {e.code})")

# Validate stops
stops = validate_stops_list([
    {"stop_id": "S1", "lat": 9.93, "lon": -84.08},
    {"stop_id": "S2", "lat": 9.94, "lon": -84.07},
])
for stop in stops:
    print(f"{stop.stop_id}: sequence {stop.stop_sequence}")
```

### Exceptions
```python
from core.exceptions import ModelNotFoundError, InvalidVehiclePositionError

# Raise with context
raise ModelNotFoundError(model_type="xgboost", route_id="171")
# Output: [MODEL_NOT_FOUND] No xgboost model found for route 171 (model_type=xgboost, route_id=171)

# Convert to dict for JSON responses
try:
    # ... some operation
except InvalidVehiclePositionError as e:
    return {"error": e.to_dict()}
```

---

## Testing

### Run the core test suite
```bash
.venv/bin/python -m pytest core/tests/test_core.py -v
```

### Verify estimator imports work
```bash
.venv/bin/python -c "from eta_service.estimator import estimate_stop_times; print('OK')"
```

### Quick validation test
```bash
.venv/bin/python -c "
from core.validation import validate_vehicle_position
vp = validate_vehicle_position({
    'vehicle_id': 'V123',
    'lat': 9.93,
    'lon': -84.08,
    'timestamp': '2024-01-15T10:00:00Z',
    'route': '171'
})
print(f'Validated: {vp.vehicle_id} on route {vp.route_id}')
"
```

### Quick logging test
```bash
.venv/bin/python -c "
from core.logging import get_logger
logger = get_logger('test')
logger.info('Hello', key='value')
"
```

---

## Migration Guide

### Updating existing code to use core modules

**Before:**
```python
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print(f"[MyComponent] Processing {vehicle_id}")
```

**After:**
```python
from core.config import get_config
from core.logging import get_logger

config = get_config()  # Handles sys.path automatically
logger = get_logger("my-component")

logger.info("Processing", vehicle_id=vehicle_id)
```

### Adding validation to data ingestion

**Before:**
```python
vehicle_id = data.get("vehicle_id") or data.get("id")
lat = float(data.get("lat"))
# ... manual extraction and validation
```

**After:**
```python
from core.validation import validate_vehicle_position
from core.exceptions import InvalidVehiclePositionError

try:
    vp = validate_vehicle_position(data)
    # Use vp.vehicle_id, vp.lat, vp.lon, etc.
except InvalidVehiclePositionError as e:
    logger.error("Invalid vehicle data", error=str(e))
```
