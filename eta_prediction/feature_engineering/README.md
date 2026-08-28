
# Feature Engineering Module

Transforms GTFS Schedule + Realtime telemetry into model-ready ETA features. The package is structured so every feature family (temporal, spatial, weather) can be invoked on its own or orchestrated via the dataset builder.

---

## 1. Architecture & Data Flow

1. **Ingest** – Pull raw `VehiclePosition`, `Trip`, `StopTime`, and `Stop` records from the Django ORM that fronts the GTFS replicas (`dataset_builder.py`).
2. **Trip Stitching** – Join telemetry rows with trip metadata (route, direction, headsign) and ordered stop sequences so every position knows which stops remain.
3. **Target Construction** – For each VehiclePosition, locate up to `max_stops_ahead` downstream stops, estimate distance with a haversine helper, and mine subsequent VehiclePositions to timestamp the actual arrival that forms the regression target.
4. **Label Validation** – Compute `time_to_arrival_seconds`, drop negative or >2h horizons, and standardize timestamps.
5. **Feature Enrichment** – Attach temporal, spatial, and weather tensors. Each step is isolated so failures yield NaNs without aborting the run.
6. **Finalize** – Select the canonical column set, fill missing headers with NaNs, and return a tidy DataFrame ready for persistence. `save_dataset` writes Snappy-compressed Parquet when needed.

```
VehiclePosition ➜ Trip metadata ➜ Stops in sequence ➜ Distance/target labeling
          ➜ {Temporal | Spatial | Weather} features ➜ Training frame
```

---

## 2. Dataset Builder (`dataset_builder.py`)

**Signature:**  
`build_vp_training_dataset(route_ids=None, start_date=None, end_date=None, distance_threshold=50.0, max_stops_ahead=5, attach_weather=True, tz_for_temporal="America/Costa_Rica", pg_conn=None) -> pd.DataFrame`

**Inputs**

- `route_ids` – subset of GTFS routes; omit for all.
- `start_date` / `end_date` – UTC datetimes bounding telemetry ingestion.
- `distance_threshold` – meters for “arrived” when mining actual arrival.
- `max_stops_ahead` – number of downstream stops to label per VehiclePosition.
- `attach_weather` – fetch Open‑Meteo hourly observations per VP position.
- `tz_for_temporal` – timezone used by temporal bins/peaks.
- `pg_conn` – optional psycopg connection used for shape loading; defaults to the Django connection when omitted.

**Outputs**

Each row represents a `(VehiclePosition, downstream stop)` pair with:

| Category | Columns |
| --- | --- |
| Identity | `trip_id`, `route_id`, `vehicle_id`, `stop_id`, `stop_sequence` |
| Telemetry | `vp_ts`, `vp_lat`, `vp_lon`, `vp_bearing`, `vp_speed` |
| Geometry | `stop_lat`, `stop_lon`, `distance_to_stop`, `progress_on_segment`, `progress_ratio` |
| Targets | `actual_arrival`, `time_to_arrival_seconds` |
| Temporal | `hour`, `day_of_week`, `is_weekend`, `is_holiday`, `is_peak_hour` |
| Weather | `temperature_c`, `precipitation_mm`, `wind_speed_kmh` |

`progress_ratio` approximates how far along the route the vehicle is by combining the ordinal of the closest stop with its progress within the current stop-to-stop segment.

**Helper routines**

- `haversine_distance` computes great-circle distances using a 6371 km Earth radius.
- `find_actual_arrival_time` scans future VehiclePositions for the first observation within `distance_threshold` meters (falls back to closest approach within 200 m).

**Usage Example**

```python
from datetime import datetime, timezone
from feature_engineering.dataset_builder import build_vp_training_dataset, save_dataset

df = build_vp_training_dataset(
    route_ids=["Green-B"],
    start_date=datetime(2023, 10, 1, tzinfo=timezone.utc),
    end_date=datetime(2023, 10, 2, tzinfo=timezone.utc),
    max_stops_ahead=3,
)
save_dataset(df, "datasets/green_line_oct01.parquet")
```

Run inside the Django project context so ORM models resolve and DB settings are loaded.

---

## 3. Temporal Features (`temporal.py`)

`extract_temporal_features(timestamp, tz="America/New_York", region="US_MA") -> Dict[str, object>`

- Naive timestamps are assumed UTC, converted to the requested tz via `zoneinfo`.
- Peak-hour logic (weekdays 07–09 & 16–19) complements coarse bins (`_tod_bin`) defined as morning/midday/afternoon/evening.
- Holiday detection relies on `holidays` with regional fallbacks; absence of the package silently downgrades to `False`.

Unit tests validate timezone handling, binning, weekend overrides, and holiday stubs (`tests/test_temporal.py`).

---

## 4. Spatial Utilities (`spatial.py`)

- `calculate_distance_features(vehicle_position, stop, next_stop)` returns distances from vehicle to current/next stop, bearing alignment, and a normalized `progress_on_segment` proxy.
- `get_route_features(route_id, conn=None, stops_in_order=None)` aggregates route length and average stop spacing either via Postgres (`sch_pipeline_routestop`/`sch_pipeline_stop`) or an in-memory stop list.
- `distance_postgis_m` exposes `ST_DistanceSphere` for higher-precision metrics when PostGIS is available.

Use these helpers when deriving spatial covariates outside the dataset builder.

---

## 5. Weather Adapter (`weather.py`)

`fetch_weather(lat, lon, timestamp) -> dict` pulls hourly Open‑Meteo aggregates aligned to the VehiclePosition timestamp.

- Timestamps are normalized to the hour in UTC, forming the cache key `weather:{lat}:{lon}:{iso_hour}` stored in the configured Django cache for 1 hour.
- Only the relevant hour is requested via `start`/`end` query params to minimize payloads; API timeouts default to 8 seconds.
- Missing API data or coverage gaps yield a dict of `None`s, keeping the dataset builder resilient.

---

## 6. Dependencies & Environment

- **Python stack:** pandas, numpy, psycopg, requests, django, holidays (optional).
- **Database:** Postgres schemas `rt_pipeline_*` and `sch_pipeline_*` accessed either through the Django ORM or raw psycopg connections. Ensure indexes on `VehiclePosition(ts, route_id)` and `TripUpdate(route_id, stop_sequence, ts)` for performant scans.
- **Caching:** Uses the configured Django cache backend for weather responses.
- **Timezone & locale:** Default tz is `America/Costa_Rica` in the dataset builder (override via `tz_for_temporal`), but temporal helpers accept any IANA tz string.

---

## 7. Testing & Validation

Automated tests cover:
- Temporal calculations (`tests/test_temporal.py`)
- Spatial distances/progress (`tests/test_spatial.py`)
- Weather fetching/memoization (`tests/test_weather.py`)

To perform tests, from the `eta_prediction` directory, run:
```bash
$ uv run pytest feature_engineering
```
---

