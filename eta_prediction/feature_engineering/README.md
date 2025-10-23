
# Feature Engineering Module

## Overview
This `feature_engineering` module provides some functions for deriving features used in the ETA prediction pipeline. 
It transforms raw GTFS Schedule and Realtime data into structured model-ready inputs covering **temporal**, **spatial**, **operational**, and **weather** dimensions. 
Each submodule can run independently or as part of the dataset builder process.

---

## 1. Temporal Features (`temporal.py`)
Extracts consistent time-based signals from timestamps.

### Main Function
`extract_temporal_features(ts, tz='America/New_York', region='US_MA') -> dict`

**Features returned**
- `hour` — hour of day (0–23)  
- `day_of_week` — Monday=0  
- `is_weekend` — weekend flag  
- `is_holiday` — calendar-aware (via `holidays` package)  
- `time_of_day_bin` — {morning, midday, afternoon, evening}  
- `is_peak_hour` — weekdays 07–09 & 16–19

**Notes**
- Naive datetimes assumed UTC and converted to local timezone.  
- Holidays are optional and region-aware (`US_MA`, `CR`, etc.).  
- Designed to remain lightweight and dependency-tolerant.

---

## 2. Spatial Features (`spatial.py`)
Provides route- and vehicle-level geometric computations for positioning and segment tracking.

### Functions

**`calculate_distance_features(vehicle_position, stop, next_stop)`**  
Computes per-vehicle distances, bearings, and progress ratios.

Returns:
- `distance_to_stop` — meters  
- `distance_to_next_stop` — meters  
- `bearing_to_stop` — degrees (0–360)  
- `is_approaching` — whether heading aligns with route direction  
- `segment_id` — hash of (stop_id, next_stop_id)  
- `progress_on_segment` — 0–1 ratio (1 - d_next/d_segment)

**`get_route_features(route_id, conn=None, stops_in_order=None)`**  
Computes route geometry metrics either via DB query or stop list.

Returns:
- `total_stops` — int  
- `route_length_km` — float  
- `avg_stop_spacing` — meters

**Assumptions**
- Input points contain `lat`, `lon`, and optional `bearing`.  
- Bearing tolerance for approach detection = 35°.  
- All distance computations use the haversine formula.

---

## 3. Operational Features (`operational.py`)
Provides route-level temporal dynamics for congestion and spacing estimation.

### Functions

**`calculate_headway(conn, route_id, timestamp, vehicle_id)`**  
Time since the most recent **other** vehicle passed on the same route.  
Returns `float('inf')` if no prior vehicle found.

**`detect_congestion_proxy(conn, route_id, stop_sequence, timestamp, ...)`**  
Heuristic congestion indicators based on speed and delay patterns.

Returns:
- `avg_speed_last_10min` — mean km/h from recent VehiclePositions  
- `vehicles_on_route` — count of distinct active vehicles  
- `delay_trend` — `'increasing' | 'stable' | 'decreasing'` from slope of delay evolution

**Data Sources**
- `rt_pipeline_vehicleposition` and `rt_pipeline_tripupdate` tables.  
- Speed converted from m/s to km/h.  
- SQL-optimized with recommended indexes for efficient lookup.

---

## 4. Weather Features (`weather.py`)
Integrates real-time meteorological conditions via the [Open‑Meteo API](https://open-meteo.com/).

### Function
`fetch_weather(lat, lon, timestamp)`

**Returns:**
- `temperature_c` (°C)  
- `precipitation_mm` (mm)  
- `wind_speed_kmh` (km/h)  
- `weather_code` (WMO)  
- `visibility_m` (m)
- `None` values on API or network failure.  

---
## Dependencies Summary
- `pandas`, `numpy`, `requests`, `datetime`, `math`
- Optional: `holidays`, `django.core.cache`, `psycopg2`
- Geospatial computations assume WGS84 (EPSG:4326).

---

## File Structure
```
feature_engineering/
├── temporal.py
├── spatial.py
├── operational.py
├── weather.py
├── tests/
│   ├── test_temporal.py
│   ├── test_spatial.py
│   ├── test_operational.py
│   └── test_weather.py
└── README.md  ← this document
```
