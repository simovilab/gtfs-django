from __future__ import annotations

from datetime import datetime, date, time, timedelta
from typing import Optional, List, Any, Dict
import numpy as np
import pandas as pd
from math import radians, degrees, cos, sin, asin, atan2, sqrt
from pathlib import Path

from django.db import connection as django_connection
from sch_pipeline.models import StopTime, Stop, Trip
from feature_engineering.rt_source import fetch_vehicle_positions
from feature_engineering.temporal import extract_temporal_features
from feature_engineering.spatial import calculate_distance_features_with_shape, load_shape_for_trip
from feature_engineering.weather import fetch_weather
from feature_engineering import progress as ui


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance in meters between two points 
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000  # Radius of earth in meters
    return c * r


def find_actual_arrival_time(vp_df_for_trip, stop_lat, stop_lon, distance_threshold=50):
    """
    Find the ts when a vehicle arrived at a stop.
    
    Strategy:
    1. Calculate distance from each VP to the stop
    2. Find the first VP within distance_threshold meters
    3. Return its ts as arrival time
    
    Args:
        vp_df_for_trip: DataFrame of VehiclePositions for a specific trip, sorted by ts
        stop_lat: Stop lat
        stop_lon: Stop lon
        distance_threshold: Distance in meters to consider "arrived" (default 50m)
    
    Returns:
        datetime or None if vehicle never arrived
    """
    if vp_df_for_trip.empty:
        return None
    
    # Calculate distances to stop
    distances = vp_df_for_trip.apply(
        lambda row: haversine_distance(row['lat'], row['lon'], stop_lat, stop_lon),
        axis=1
    )
    
    # Find first position within threshold
    arrived_mask = distances <= distance_threshold
    if arrived_mask.any():
        first_arrival_idx = arrived_mask.idxmax()  # First True index
        return vp_df_for_trip.loc[first_arrival_idx, 'ts']
    
    # Alternative: If never within threshold, find closest approach
    closest_idx = distances.idxmin()
    min_distance = distances.min()
    
    # Only use closest approach if reasonably close (e.g., within 200m)
    if min_distance <= 200:
        return vp_df_for_trip.loc[closest_idx, 'ts']

    return None


def find_stopped_at_arrival(vp_df_for_trip, target_stop_id, target_stop_seq):
    """Arrival ts from GTFS-RT status: first VP reporting STOPPED_AT at the stop.

    Matches on ``stop_id`` when available, else falls back to
    ``current_stop_sequence``. Returns None when no qualifying VP exists (e.g.
    pre-capture data where ``current_status`` is null).
    """
    if vp_df_for_trip.empty or "current_status" not in vp_df_for_trip.columns:
        return None

    stopped = vp_df_for_trip[vp_df_for_trip["current_status"] == "STOPPED_AT"]
    if stopped.empty:
        return None

    by_id = None
    if target_stop_id is not None and "stop_id" in stopped.columns:
        by_id = stopped[stopped["stop_id"].astype("string") == str(target_stop_id)]
    if by_id is not None and not by_id.empty:
        return by_id["ts"].min()

    if target_stop_seq is not None and "current_stop_sequence" in stopped.columns:
        by_seq = stopped[stopped["current_stop_sequence"] == target_stop_seq]
        if not by_seq.empty:
            return by_seq["ts"].min()

    return None


def _initial_bearing(lat1, lon1, lat2, lon2):
    """Initial great-circle bearing from point 1 to point 2, in degrees [0, 360)."""
    φ1, φ2 = radians(lat1), radians(lat2)
    dλ = radians(lon2 - lon1)
    x = sin(dλ) * cos(φ2)
    y = cos(φ1) * sin(φ2) - sin(φ1) * cos(φ2) * cos(dλ)
    return (degrees(atan2(x, y)) + 360.0) % 360.0


def _angle_diff(a, b):
    """Smallest absolute difference between two bearings, in degrees [0, 180]."""
    if a is None or b is None:
        return None
    d = abs(float(a) - float(b)) % 360.0
    return d if d <= 180.0 else 360.0 - d


def _seconds_of_day(t):
    """Seconds since midnight for a datetime.time / GTFS arrival_time, else None."""
    if t is None or pd.isna(t):
        return None
    if hasattr(t, "hour"):
        return t.hour * 3600 + t.minute * 60 + getattr(t, "second", 0)
    return None


def build_vp_training_dataset(
    route_ids: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    distance_threshold: float = 50.0,
    max_stops_ahead: int = 5,
    attach_weather: bool = True,
    use_shapes: bool = True,
    tz_for_temporal: str = "America/New_York",
    pg_conn: Optional[Any] = None,
    vp_source_uri: Optional[str] = None,
    arrival_source: str = "computed",
) -> pd.DataFrame:
    """
    Build training dataset from VehiclePosition data only.
    
    For each VehiclePosition:
    - Identify current trip and route
    - Find remaining stops in trip
    - Calculate distance to each remaining stop (up to max_stops_ahead)
    - Find actual arrival times from subsequent VehiclePositions
    - Extract temporal, operational, and weather features
    
    Args:
        route_ids: List of route IDs to include (None = all routes)
        start_date: Start of date range
        end_date: End of date range
        distance_threshold: Distance in meters to consider "arrived at stop"
        max_stops_ahead: Maximum number of future stops to include per VP
        attach_weather: Whether to fetch weather data
        use_shapes: Whether to use GTFS shapes for accurate progress calculation
        tz_for_temporal: Timezone for temporal features
        pg_conn: PostgreSQL connection for shape loading (defaults to Django connection)
        vp_source_uri: Override S3 base URI for the VP store
        arrival_source: Which arrival drives the canonical target —
            'computed' (first VP within distance_threshold of the stop) or
            'stopped_at' (first VP reporting GTFS-RT current_status==STOPPED_AT
            at the stop). Both columns are always emitted when derivable.

    Returns:
        DataFrame with columns:
        - trip_id, route_id, vehicle_id, stop_id, stop_sequence, stop_lat/lon
        - vp_ts, vp_lat, vp_lon, vp_bearing
        - spatial: distance_to_stop, distance_to_next_stop, shape_distance_to_stop,
          shape_progress, cross_track_error, progress_ratio, stops_ahead
        - kinematic: current_speed_kmh, bearing_to_stop, bearing_diff
        - status: is_at_stop
        - target: actual_arrival_computed, actual_arrival_stopped_at, actual_arrival,
          time_to_arrival_seconds_computed, time_to_arrival_seconds_stopped_at,
          time_to_arrival_seconds
        - temporal: hour, day_of_week, is_weekend, is_holiday, is_peak_hour,
          hour_sin, hour_cos, dow_sin, dow_cos
    """
    if arrival_source not in ("computed", "stopped_at"):
        raise ValueError(
            f"arrival_source must be 'computed' or 'stopped_at', got {arrival_source!r}"
        )
    
    _banner_lines = []
    if route_ids:
        _banner_lines.append(f"routes   {', '.join(route_ids)}")
    if start_date and end_date:
        _banner_lines.append(f"window   {start_date.date()} → {end_date.date()}")
    _banner_lines.append(f"shapes   {'enabled' if use_shapes else 'disabled'}")
    ui.banner("ETA dataset builder · VehiclePositions", _banner_lines)

    # Setup database connection for shape loading
    if use_shapes and pg_conn is None:
        try:
            django_connection.ensure_connection()
            pg_conn = django_connection.connection
        except Exception as exc:
            ui.note(f"DB connection unavailable ({exc}) — shape features disabled")
            use_shapes = False
    
    # ============================================================
    # STEP 1: Fetch VehiclePositions
    # ============================================================
    # RT now comes from the S3 store (route + date partition pushdown), not the
    # live Postgres ORM. Returns the same columns the rest of this builder uses.
    with ui.step("Fetch VehiclePositions from S3") as s:
        vp_df = fetch_vehicle_positions(
            route_ids=route_ids,
            start=start_date,
            end=end_date,
            base_uri=vp_source_uri,
        )
        s.detail(f"{len(vp_df):,} records")

    if vp_df.empty:
        ui.note("No VehiclePosition data found for this filter — nothing to build.")
        return pd.DataFrame()
    
    # ============================================================
    # STEP 2: Get trip metadata
    # ============================================================
    with ui.step("Join trip metadata") as s:
        trip_ids = vp_df['trip_id'].unique()
        trips_data = Trip.objects.filter(trip_id__in=trip_ids).values(
            'trip_id', 'route_id', 'direction_id', 'trip_headsign', 'service_id'
        )
        trips_df = pd.DataFrame.from_records(trips_data)

        vp_df = vp_df.merge(trips_df, on='trip_id', how='left')
        # Drop VPs without route info
        initial_count = len(vp_df)
        vp_df = vp_df.dropna(subset=['route_id'])
        s.detail(f"{len(vp_df):,} VPs matched · {initial_count - len(vp_df):,} dropped")
    
    # ============================================================
    # STEP 2b: Load shapes for trips (if enabled)
    # ============================================================
    shape_cache: Dict[str, Any] = {}
    
    if use_shapes:
        unique_trip_ids = vp_df['trip_id'].unique()
        loaded_count = 0
        failed_count = 0

        with ui.bar(len(unique_trip_ids), "Load GTFS shapes") as b:
            for trip_id in unique_trip_ids:
                try:
                    shape = load_shape_for_trip(trip_id, pg_conn)
                    if shape is not None:
                        shape_cache[trip_id] = shape
                        loaded_count += 1
                except Exception:
                    # Silently skip trips without shapes
                    failed_count += 1
                finally:
                    b.advance(detail=f"{loaded_count:,} loaded")
        if failed_count > 0:
            ui.note(f"{failed_count:,} trips had no shape (skipped)")
    
    # ============================================================
    # STEP 3: Get stop sequences for each trip
    # ============================================================
    with ui.step("Load stop sequences + coordinates") as s:
        stoptimes_data = StopTime.objects.filter(
            trip_id__in=trip_ids
        ).values(
            'trip_id',
            'stop_sequence',
            'stop_id',
            'arrival_time',
        ).order_by('trip_id', 'stop_sequence')

        st_df = pd.DataFrame.from_records(stoptimes_data)

        if st_df.empty:
            ui.note("No StopTime data found — is the schedule imported?")
            return pd.DataFrame()

        # Get stop coordinates
        stop_ids = st_df['stop_id'].unique()
        # remove any NaN / None values and coerce to STRING
        stop_ids = [str(s) for s in stop_ids if pd.notna(s)]

        stops_data = Stop.objects.filter(stop_id__in=stop_ids).values(
            'stop_id', 'stop_lat', 'stop_lon', 'stop_name'
        )
        stops_df = pd.DataFrame.from_records(stops_data)

        st_df = st_df.merge(stops_df, on='stop_id', how='left')
        s.detail(
            f"{len(st_df):,} stop-times · {st_df['stop_lat'].notna().sum():,} geocoded"
        )
    
    # ============================================================
    # STEP 4: For each VP, find remaining stops and distances
    # ============================================================
    training_rows = []

    # Group VPs by trip for efficient processing
    vp_grouped = vp_df.groupby('trip_id')
    st_grouped = st_df.groupby('trip_id')

    for trip_id, trip_vps in ui.track(
        vp_grouped,
        vp_grouped.ngroups,
        f"Match VPs → upcoming stops (≤{max_stops_ahead})",
        detail=lambda: f"{len(training_rows):,} samples",
    ):
        # Get stop sequence for this trip
        if trip_id not in st_grouped.groups:
            continue
        
        trip_stops = (
            st_grouped.get_group(trip_id)
            .sort_values('stop_sequence')
            .reset_index(drop=True)
        )
        trip_stops['stop_order'] = trip_stops.index
        trip_stops['next_stop_id'] = trip_stops['stop_id'].shift(-1)
        trip_stops['next_stop_lat'] = trip_stops['stop_lat'].shift(-1)
        trip_stops['next_stop_lon'] = trip_stops['stop_lon'].shift(-1)
        trip_total_segments = max(len(trip_stops) - 1, 1)
        
        # Get shape for this trip (if available)
        trip_shape = shape_cache.get(trip_id)
        
        # For each VP in this trip
        for vp_idx, vp_row in trip_vps.iterrows():
            vp_lat = vp_row['lat']
            vp_lon = vp_row['lon']
            vp_ts = vp_row['ts']
            vp_position = {
                'lat': float(vp_lat),
                'lon': float(vp_lon),
                'bearing': vp_row.get('bearing'),
            }
            
            # Find which stops are ahead of this VP
            # Strategy: Calculate distance to all stops, take the closest N
            distances_to_stops = trip_stops.apply(
                lambda stop: haversine_distance(vp_lat, vp_lon, stop['stop_lat'], stop['stop_lon']),
                axis=1
            )
            
            trip_stops_with_dist = trip_stops.copy()
            trip_stops_with_dist['distance_to_stop'] = distances_to_stops
            
            # Sort by stop_sequence and take upcoming stops
            # A stop is "upcoming" if it hasn't been passed yet
            # Simple heuristic: stops that are ahead in sequence from the closest stop
            closest_stop_idx = distances_to_stops.idxmin()
            closest_stop_seq = trip_stops.loc[closest_stop_idx, 'stop_sequence']
            closest_stop_order = trip_stops.loc[closest_stop_idx, 'stop_order']
            
            # Get stops with sequence >= closest (upcoming stops)
            upcoming_stops = trip_stops_with_dist[
                trip_stops_with_dist['stop_sequence'] >= closest_stop_seq
            ].head(max_stops_ahead)
            
            # For each upcoming stop, find actual arrival time
            for stop_idx, stop_row in upcoming_stops.iterrows():
                # Future VPs of this trip (used to detect arrival both ways)
                future_vps = trip_vps[trip_vps['ts'] > vp_ts]

                stop_payload = {
                    'stop_id': stop_row['stop_id'],
                    'lat': stop_row['stop_lat'],
                    'lon': stop_row['stop_lon'],
                    'stop_order': stop_row.get('stop_order'),
                    'total_segments': trip_total_segments,
                }
                next_stop_payload = None
                if pd.notna(stop_row['next_stop_id']):
                    next_stop_payload = {
                        'stop_id': stop_row['next_stop_id'],
                        'lat': stop_row['next_stop_lat'],
                        'lon': stop_row['next_stop_lon'],
                    }

                # Spatial features (shape-aware when a shape is available).
                spatial_feats = calculate_distance_features_with_shape(
                    vp_position,
                    stop_payload,
                    next_stop_payload,
                    shape=trip_shape if use_shapes else None,
                    vehicle_stop_order=int(closest_stop_order) if pd.notna(closest_stop_order) else None,
                    total_segments=trip_total_segments
                )

                # --- Dual arrival target -------------------------------------
                actual_arrival_computed = find_actual_arrival_time(
                    future_vps,
                    stop_row['stop_lat'],
                    stop_row['stop_lon'],
                    distance_threshold=distance_threshold,
                )
                actual_arrival_stopped_at = find_stopped_at_arrival(
                    future_vps,
                    stop_row['stop_id'],
                    stop_row['stop_sequence'],
                )
                primary_arrival = (
                    actual_arrival_computed if arrival_source == "computed"
                    else actual_arrival_stopped_at
                )
                # Keep the row only when the chosen primary arrival exists.
                if primary_arrival is None:
                    continue

                # --- Derived features ----------------------------------------
                bearing_to_stop = _initial_bearing(
                    float(vp_lat), float(vp_lon),
                    float(stop_row['stop_lat']), float(stop_row['stop_lon']),
                )
                vp_bearing = vp_row.get('bearing')
                stops_ahead = int(stop_row['stop_order'] - closest_stop_order)

                training_rows.append({
                    'trip_id': trip_id,
                    'route_id': vp_row['route_id'],
                    'vehicle_id': vp_row['vehicle_id'],
                    'vp_ts': vp_ts,
                    'vp_lat': vp_lat,
                    'vp_lon': vp_lon,
                    'vp_bearing': vp_bearing,
                    'vp_speed': vp_row.get('speed'),
                    'stop_id': stop_row['stop_id'],
                    'stop_sequence': stop_row['stop_sequence'],
                    'stop_lat': stop_row['stop_lat'],
                    'stop_lon': stop_row['stop_lon'],
                    # spatial
                    'distance_to_stop': spatial_feats.get('distance_to_stop'),
                    'distance_to_next_stop': spatial_feats.get('distance_to_next_stop'),
                    'shape_distance_to_stop': spatial_feats.get('shape_distance_to_stop'),
                    'shape_progress': spatial_feats.get('shape_progress'),
                    'cross_track_error': spatial_feats.get('cross_track_error'),
                    'progress_ratio': spatial_feats.get('progress_ratio'),
                    'stops_ahead': stops_ahead,
                    # kinematic
                    'bearing_to_stop': bearing_to_stop,
                    'bearing_diff': _angle_diff(vp_bearing, bearing_to_stop),
                    # status
                    'is_at_stop': bool(vp_row.get('current_status') == "STOPPED_AT"),
                    # target (both sources + canonical primary)
                    'actual_arrival_computed': actual_arrival_computed,
                    'actual_arrival_stopped_at': actual_arrival_stopped_at,
                    'actual_arrival': primary_arrival,
                })
    
    if not training_rows:
        ui.note("No training rows generated (no detected arrivals) — nothing to build.")
        return pd.DataFrame()

    df = pd.DataFrame(training_rows)

    # ============================================================
    # STEP 5: Compute time_to_arrival_seconds
    # ============================================================
    with ui.step(f"Compute time-to-arrival target (primary={arrival_source})") as s:
        # Normalise all arrival timestamps to UTC datetimes.
        df['vp_ts'] = pd.to_datetime(df['vp_ts'], utc=True)
        for col in ('actual_arrival', 'actual_arrival_computed', 'actual_arrival_stopped_at'):
            df[col] = pd.to_datetime(df[col], utc=True)

        # Both sources + the canonical target (driven by arrival_source).
        df['time_to_arrival_seconds_computed'] = (
            df['actual_arrival_computed'] - df['vp_ts']
        ).dt.total_seconds()
        df['time_to_arrival_seconds_stopped_at'] = (
            df['actual_arrival_stopped_at'] - df['vp_ts']
        ).dt.total_seconds()
        df['time_to_arrival_seconds'] = (
            df['actual_arrival'] - df['vp_ts']
        ).dt.total_seconds()

        # Filter on the canonical target only (the trained target).
        initial_count = len(df)
        df = df[
            (df['time_to_arrival_seconds'] >= 0) &
            (df['time_to_arrival_seconds'] <= 7200)  # Max 2 hours ahead
        ]
        s.detail(f"{len(df):,} valid · {initial_count - len(df):,} out-of-range dropped")
    
    # ============================================================
    # STEP 6: Temporal features
    # ============================================================
    temporal_data = []
    for ts in ui.track(df['vp_ts'], len(df), "Extract temporal features"):
        try:
            feats = extract_temporal_features(ts, tz=tz_for_temporal, region="US_MA")
            temporal_data.append(feats)
        except Exception:
            temporal_data.append({})

    tf = pd.DataFrame(temporal_data)
    keep_temporal = ["hour", "day_of_week", "is_weekend", "is_holiday", "is_peak_hour"]
    for k in keep_temporal:
        if k in tf.columns:
            df[k] = tf[k].values
        else:
            df[k] = np.nan

    # Cyclical encodings (continuous, no artificial 23->0 / Sun->Mon jump) —
    # helpful for linear/poly models and LSTMs alike.
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24.0)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24.0)
    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7.0)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7.0)
    
    # ============================================================
    # STEP 7: Operational features
    # ============================================================
    # Commented out for now - uncomment if you want operational features
    # print("\nStep 7: Computing operational features...")
    # ... (operational feature code)
    
    # Use VP speed directly if available
    if 'vp_speed' in df.columns:
        df['current_speed_kmh'] = df['vp_speed'] * 3.6  # m/s to km/h
    else:
        df['current_speed_kmh'] = np.nan
    
    # ============================================================
    # STEP 8: Weather features — STUB (intentionally disabled)
    # ============================================================
    # Weather needs rethinking (source, caching, leakage) and is out of scope.
    # Re-add temperature_c / precipitation_mm / wind_speed_kmh here when ready,
    # then register them in models.common.data.ETADataset.FEATURE_GROUPS.
    # (See feature_engineering/weather.py.)

    # ============================================================
    # STEP 9: Final column selection
    # ============================================================
    wanted_cols = [
        # identifiers
        "trip_id", "route_id", "vehicle_id", "stop_id", "stop_sequence",
        # position
        "vp_ts", "vp_lat", "vp_lon", "vp_bearing", "stop_lat", "stop_lon",
        # spatial
        "distance_to_stop", "distance_to_next_stop", "shape_distance_to_stop",
        "shape_progress", "cross_track_error", "progress_ratio", "stops_ahead",
        # kinematic
        "current_speed_kmh", "bearing_to_stop", "bearing_diff",
        # status
        "is_at_stop",
        # target (dual source + canonical)
        "actual_arrival_computed", "actual_arrival_stopped_at", "actual_arrival",
        "time_to_arrival_seconds_computed", "time_to_arrival_seconds_stopped_at",
        "time_to_arrival_seconds",
        # temporal
        "hour", "day_of_week", "is_weekend", "is_holiday", "is_peak_hour",
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    ]

    for c in wanted_cols:
        if c not in df.columns:
            df[c] = np.nan
    
    result = df[wanted_cols].reset_index(drop=True)

    _avg_tta = result['time_to_arrival_seconds'].mean()
    _stopped_cov = (
        100.0 * result['actual_arrival_stopped_at'].notna().mean()
        if len(result) else 0.0
    )
    ui.summary(
        "Dataset ready",
        [
            ("rows", f"{len(result):,}"),
            ("columns", f"{len(wanted_cols)}"),
            ("trips", f"{result['trip_id'].nunique():,}"),
            ("routes", f"{result['route_id'].nunique():,}"),
            ("stops", f"{result['stop_id'].nunique():,}"),
            ("target", f"{arrival_source}"),
            ("avg ETA", f"{_avg_tta:.0f}s ({_avg_tta / 60:.1f} min)"),
            ("stopped_at cov", f"{_stopped_cov:.1f}%"),
        ],
    )

    return result


def save_dataset(df: pd.DataFrame, output_path: str):
    """Save to parquet with compression, creating directories if needed."""
    output_path = Path(output_path)

    # Create parent directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with ui.step(f"Save → {output_path}") as s:
        df.to_parquet(output_path, compression="snappy", index=False)
        s.detail(f"{len(df):,} rows")
