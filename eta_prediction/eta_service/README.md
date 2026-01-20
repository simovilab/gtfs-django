# ETA Service

Real-time arrival time estimation service for transit vehicles.

## Overview

The `eta_service` provides a production-ready interface for generating ETA predictions from live vehicle positions. It bridges the real-time data stream (MQTT/Redis) with trained ML models.

## Features

- `estimate_stop_times()` - Main prediction function
- Smart model selection (route-specific → global → fallback)
- Temporal + spatial feature extraction
- Structured logging via `core.logging`
- Input validation via `core.validation`
- Configurable defaults via `core.config`

---

## Quick Start

```python
from eta_service import estimate_stop_times
from datetime import datetime, timezone

# Vehicle position from MQTT/Redis (matches your MQTT format)
vehicle_position = {
    'vehicle_id': 'vehicle_42',
    'route': '1',
    'lat': 9.9281,
    'lon': -84.0907,
    'speed': 10.5,  # m/s (will be converted to km/h internally)
    'heading': 90,
    'timestamp': datetime.now(timezone.utc).isoformat()
}

# Upcoming stops (from cached route data)
upcoming_stops = [
    {'stop_id': 'stop_001', 'stop_sequence': 5, 'total_stop_sequence': 20, 'lat': 9.9291, 'lon': -84.0897},
    {'stop_id': 'stop_002', 'stop_sequence': 6, 'total_stop_sequence': 20, 'lat': 9.9301, 'lon': -84.0887},
    {'stop_id': 'stop_003', 'stop_sequence': 7, 'total_stop_sequence': 20, 'lat': 9.9311, 'lon': -84.0877},
]

# Get predictions
result = estimate_stop_times(
    vehicle_position=vehicle_position,
    upcoming_stops=upcoming_stops,
    route_id='1',
    trip_id='trip_001',
    max_stops=3
)

# Access predictions
print(f"Vehicle: {result['vehicle_id']}")
print(f"Model: {result['model_type']}")
for pred in result['predictions']:
    if not pred.get('error'):
        print(f"Stop {pred['stop_id']}: {pred['eta_formatted']} ({pred['eta_minutes']:.1f} min)")
```

---

## Function Reference

### `estimate_stop_times()`

```python
estimate_stop_times(
    vehicle_position: dict,
    upcoming_stops: list[dict],
    route_id: str = None,
    trip_id: str = None,
    model_key: str = None,
    max_stops: int = 3,
) -> dict
```

**Parameters:**
- `vehicle_position`: Vehicle data with:
  - `vehicle_id` (str)
  - `lat`, `lon` (float)
  - `speed` (float, in m/s) - converted to km/h internally
  - `heading` (int, degrees) - optional
  - `timestamp` (str, ISO format)
- `upcoming_stops`: List of stops with `stop_id`, `stop_sequence`, `lat`, `lon`
- `route_id`: Route identifier (optional, passed to models)
- `trip_id`: Trip identifier (optional, for context)
- `model_key`: Specific model to use (if `None`, uses best from registry)
- `max_stops`: Maximum stops to predict (default: 3)

**Returns:**
```python
{
    'vehicle_id': str,
    'route_id': str,
    'trip_id': str,
    'computed_at': str,  # ISO timestamp
    'model_key': str,
    'model_type': str,  # 'historical_mean', 'ewma', 'polyreg_distance', 'polyreg_time'
    'predictions': [
        {
            'stop_id': str,
            'stop_sequence': int,
            'distance_to_stop_m': float,
            'eta_seconds': float,
            'eta_minutes': float,
            'eta_formatted': str,  # e.g., "5m 23s"
            'eta_timestamp': str,  # ISO timestamp
        },
        ...
    ]
}
```

**Important Notes:**
- Speed must be in **m/s** (matches your MQTT format)
- Each model type has different feature requirements, handled internally
- Models use appropriate features based on their training configuration
- Missing features are filled with sensible defaults

---

## Testing

Run the test suite:

```bash
cd eta_prediction/
python eta_service/test_estimator.py
```

Test individual scenarios:

```python
from eta_service.test_estimator import test_basic_prediction
test_basic_prediction()
```

---

## Integration with Data Pipeline

### Expected Flow (When Complete):

```
MQTT Broker (raw vehicle data)
    ↓
Redis Subscriber
    ↓
Bytewax Stream Processor
    ↓
estimate_stop_times() ← YOU ARE HERE
    ↓
Redis (predictions cache)
    ↓
API / Dashboard
```

### Current MVP Flow:

```
Mock/Real Vehicle Position
    ↓
estimate_stop_times()
    ↓
Return predictions dict
```

---

## Directory Structure

```
eta_service/
├── __init__.py          # Module exports
├── estimator.py         # Main estimation logic
├── test_estimator.py    # Test suite
└── README.md            # This file
```

Configuration, logging, and validation are now provided by the `core/` module at the project root.

---

## Dependencies

- `core` module (config, logging, validation, exceptions)
- `feature_engineering` module (temporal, spatial)
- `models` module (registry, trained models)
- Python 3.10+

---

## Notes

- All timestamps are UTC
- Distances in meters, speeds in km/h
- Models must be trained and registered before use
- Function is designed to be called from stream processors (Bytewax, Flink, etc.)
