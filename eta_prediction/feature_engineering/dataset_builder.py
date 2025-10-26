from __future__ import annotations

from datetime import datetime, date, time, timedelta
from typing import Optional, List
import numpy as np
import pandas as pd
from math import radians, cos, sin, asin, sqrt

from django.db.models import F, Q
from sch_pipeline.models import StopTime, Stop, Route, Trip
from rt_pipeline.models import VehiclePosition
from feature_engineering.temporal import extract_temporal_features
from feature_engineering.operational import calculate_headway, detect_congestion_proxy
from feature_engineering.weather import fetch_weather


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


def build_vp_training_dataset(
    route_ids: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    distance_threshold: float = 50.0,
    max_stops_ahead: int = 5,
    attach_weather: bool = True,
    tz_for_temporal: str = "America/Costa_Rica",
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
        tz_for_temporal: Timezone for temporal features
    
    Returns:
        DataFrame with columns:
        - trip_id, route_id, vehicle_id
        - vp_ts, vp_lat, vp_lon
        - stop_id, stop_sequence, stop_lat, stop_lon
        - distance_to_stop (meters)
        - scheduled_arrival, actual_arrival, delay_seconds
        - temporal features (hour, day_of_week, etc.)
        - operational features (headway, avg_speed)
        - weather features (temperature, precipitation, wind_speed)
    """
    
    print("=" * 70)
    print("VP-BASED DATASET BUILDER")
    print("=" * 70)
    
    # ============================================================
    # STEP 1: Fetch VehiclePositions
    # ============================================================
    print("\nStep 1: Fetching VehiclePositions...")
    
    vp_qs = VehiclePosition.objects.exclude(
        Q(trip_id__isnull=True) |
        Q(lat__isnull=True) |
        Q(lon__isnull=True)
    )
    
    if start_date:
        print(f"  Filtering start_date >= {start_date}")
        vp_qs = vp_qs.filter(ts__gte=start_date)
    
    if end_date:
        print(f"  Filtering end_date < {end_date}")
        vp_qs = vp_qs.filter(ts__lt=end_date)
    
    # Filter by route if specified
    if route_ids:
        print(f"  Filtering for routes: {route_ids}")
        trip_ids_for_routes = list(
            Trip.objects.filter(route_id__in=route_ids)
            .values_list("trip_id", flat=True)
        )
        vp_qs = vp_qs.filter(trip_id__in=trip_ids_for_routes)
    
    print(f"  Fetching VehiclePosition records...")
    vp_data = vp_qs.values(
        'trip_id',
        'vehicle_id',
        'ts',
        'lat',
        'lon',
        'bearing',
        'speed',
    ).order_by('trip_id', 'ts')
    
    vp_df = pd.DataFrame.from_records(vp_data)
    print(f"  Retrieved {len(vp_df):,} VehiclePosition records")
    
    if vp_df.empty:
        print("\nWARNING: No VehiclePosition data found!")
        return pd.DataFrame()
    
    # ============================================================
    # STEP 2: Get trip metadata
    # ============================================================
    print("\nStep 2: Getting trip metadata...")
    
    trip_ids = vp_df['trip_id'].unique()
    trips_data = Trip.objects.filter(trip_id__in=trip_ids).values(
        'trip_id', 'route_id', 'direction_id', 'trip_headsign', 'service_id'
    )
    trips_df = pd.DataFrame.from_records(trips_data)
    
    vp_df = vp_df.merge(trips_df, on='trip_id', how='left')
    print(f"  Added route info for {len(vp_df):,} records")
    
    # Drop VPs without route info
    initial_count = len(vp_df)
    vp_df = vp_df.dropna(subset=['route_id'])
    print(f"  Dropped {initial_count - len(vp_df):,} VPs without route info")
    
    # ============================================================
    # STEP 3: Get stop sequences for each trip
    # ============================================================
    print("\nStep 3: Loading stop sequences for trips...")
    
    stoptimes_data = StopTime.objects.filter(
        trip_id__in=trip_ids
    ).values(
        'trip_id',
        'stop_sequence',
        'stop_id',
        'arrival_time',
    ).order_by('trip_id', 'stop_sequence')
    
    st_df = pd.DataFrame.from_records(stoptimes_data)
    print(f"  Retrieved {len(st_df):,} StopTime records")
    
    if st_df.empty:
        print("\nWARNING: No StopTime data found!")
        return pd.DataFrame()
    
    # # Get stop coordinates
    # stop_ids = st_df['stop_id'].unique()
    # stops_data = Stop.objects.filter(id__in=stop_ids).values(
    #     'id', 'stop_lat', 'stop_lon', 'stop_name'
    # )
    # stops_df = pd.DataFrame.from_records(stops_data).rename(
    #     columns={'id': 'stop_id'}
    # )
    
    # st_df = st_df.merge(stops_df, on='stop_id', how='left')
    # print(f"  Added coordinates for {st_df['stop_lat'].notna().sum():,} stops")
    
    # Get stop coordinates
    stop_ids = st_df['stop_id'].unique()
    # remove any NaN / None values and coerce to list for ORM filter
    stop_ids = [s for s in stop_ids if pd.notna(s)]
    stops_data = Stop.objects.filter(id__in=stop_ids).values(
        'id', 'stop_lat', 'stop_lon', 'stop_name'
    )
    stops_df = pd.DataFrame.from_records(stops_data).rename(
        columns={'id': 'stop_id'}
    )
    # use the model column that stores GTFS stop id (stop_id) so values() returns 'stop_id'
    stops_data = Stop.objects.filter(stop_id__in=stop_ids).values(
        'stop_id', 'stop_lat', 'stop_lon', 'stop_name'
    )
    stops_df = pd.DataFrame.from_records(stops_data)

    st_df = st_df.merge(stops_df, on='stop_id', how='left')
    print(f"  Added coordinates for {st_df['stop_lat'].notna().sum():,} stops")
    # st_df = st_df.merge(stops_df, on='stop_id', how='left')
    # print(f"  Added coordinates for {st_df['stop_lat'].notna().sum():,} stops")
# ...existing code...


    # ============================================================
    # STEP 4: For each VP, find remaining stops and distances
    # ============================================================
    print(f"\nStep 4: Calculating distances to remaining stops (max {max_stops_ahead})...")
    
    training_rows = []
    total_vps = len(vp_df)
    
    # Group VPs by trip for efficient processing
    vp_grouped = vp_df.groupby('trip_id')
    st_grouped = st_df.groupby('trip_id')
    
    processed_trips = 0
    for trip_id, trip_vps in vp_grouped:
        processed_trips += 1
        if processed_trips % 100 == 0:
            print(f"  Processing trip {processed_trips}/{len(vp_grouped)} ({len(training_rows):,} rows generated)")
        
        # Get stop sequence for this trip
        if trip_id not in st_grouped.groups:
            continue
        
        trip_stops = st_grouped.get_group(trip_id).sort_values('stop_sequence')
        
        # For each VP in this trip
        for vp_idx, vp_row in trip_vps.iterrows():
            vp_lat = vp_row['lat']
            vp_lon = vp_row['lon']
            vp_ts = vp_row['ts']
            
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
            
            # Get stops with sequence >= closest (upcoming stops)
            upcoming_stops = trip_stops_with_dist[
                trip_stops_with_dist['stop_sequence'] >= closest_stop_seq
            ].head(max_stops_ahead)
            
            # For each upcoming stop, find actual arrival time
            for stop_idx, stop_row in upcoming_stops.iterrows():
                # Find actual arrival from future VPs
                future_vps = trip_vps[trip_vps['ts'] > vp_ts]
                
                actual_arrival = find_actual_arrival_time(
                    future_vps,
                    stop_row['stop_lat'],
                    stop_row['stop_lon'],
                    distance_threshold=distance_threshold
                )
                
                # Only include if we found an actual arrival
                if actual_arrival is not None:
                    training_rows.append({
                        'trip_id': trip_id,
                        'route_id': vp_row['route_id'],
                        'vehicle_id': vp_row['vehicle_id'],
                        'vp_ts': vp_ts,
                        'vp_lat': vp_lat,
                        'vp_lon': vp_lon,
                        'vp_bearing': vp_row.get('bearing'),
                        'vp_speed': vp_row.get('speed'),
                        'stop_id': stop_row['stop_id'],
                        'stop_sequence': stop_row['stop_sequence'],
                        'stop_lat': stop_row['stop_lat'],
                        'stop_lon': stop_row['stop_lon'],
                        'distance_to_stop': stop_row['distance_to_stop'],
                        'scheduled_arrival': stop_row['arrival_time'],
                        'actual_arrival': actual_arrival,
                    })
    
    print(f"  Generated {len(training_rows):,} training samples")
    
    if not training_rows:
        print("\nWARNING: No training rows generated!")
        return pd.DataFrame()
    
    df = pd.DataFrame(training_rows)
    
    # ============================================================
    # STEP 5: Compute delay_seconds
    # ============================================================
    print("\nStep 5: Computing delay_seconds...")
    
    # Convert tss to datetime
    df['vp_ts'] = pd.to_datetime(df['vp_ts'], utc=True)
    df['actual_arrival'] = pd.to_datetime(df['actual_arrival'], utc=True)
    
    # Parse scheduled arrival time (may need date context)
    # For now, compute time_to_arrival (prediction target)
    df['time_to_arrival_seconds'] = (
        df['actual_arrival'] - df['vp_ts']
    ).dt.total_seconds()
    
    # Filter out negative or unrealistic values
    initial_count = len(df)
    df = df[
        (df['time_to_arrival_seconds'] >= 0) &
        (df['time_to_arrival_seconds'] <= 7200)  # Max 2 hours ahead
    ]
    print(f"  Dropped {initial_count - len(df):,} rows with invalid time_to_arrival")
    print(f"  Remaining: {len(df):,} rows")
    
    # ============================================================
    # STEP 6: Temporal features
    # ============================================================
    print("\nStep 6: Extracting temporal features...")
    
    temporal_data = []
    for idx, ts in enumerate(df['vp_ts']):
        if idx % 10000 == 0 and idx > 0:
            print(f"  Processed {idx:,}/{len(df):,} temporal features")
        
        try:
            feats = extract_temporal_features(ts, tz=tz_for_temporal, region="CR")
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
    
    # ============================================================
    # STEP 7: Operational features
    # ============================================================
    print("\nStep 7: Computing operational features...")
    
    def _safe_headway(route_id, ts, vehicle_id):
        try:
            return calculate_headway(
                route_id=route_id,
                ts=ts,
                vehicle_id=vehicle_id
            )
        except Exception:
            return np.nan
    
    headways = []
    for idx, row in df.iterrows():
        if idx % 1000 == 0:
            print(f"  Processing headway: {idx:,}/{len(df):,}")
        headways.append(_safe_headway(
            row['route_id'],
            row['vp_ts'],
            row['vehicle_id']
        ))
    
    df['headway_seconds'] = headways
    
    # Use VP speed directly if available
    df['current_speed_kmh'] = df['vp_speed'] * 3.6  # m/s to km/h
    
    # ============================================================
    # STEP 8: Weather features
    # ============================================================
    if attach_weather:
        print("\nStep 8: Fetching weather data...")
        
        weather_data = []
        for idx, row in df.iterrows():
            if idx % 5000 == 0:
                print(f"  Fetching weather: {idx:,}/{len(df):,}")
            
            try:
                w = fetch_weather(
                    float(row['vp_lat']),
                    float(row['vp_lon']),
                    row['vp_ts'].to_pydatetime()
                )
                weather_data.append(w or {})
            except Exception:
                weather_data.append({})
        
        wx_df = pd.DataFrame(weather_data)
        for k in ["temperature_c", "precipitation_mm", "wind_speed_kmh"]:
            if k in wx_df.columns:
                df[k] = wx_df[k].values
            else:
                df[k] = np.nan
    else:
        df["temperature_c"] = np.nan
        df["precipitation_mm"] = np.nan
        df["wind_speed_kmh"] = np.nan
    
    # ============================================================
    # STEP 9: Final column selection
    # ============================================================
    print("\nStep 9: Preparing final dataset...")
    
    wanted_cols = [
        "trip_id", "route_id", "vehicle_id", "stop_id", "stop_sequence",
        "vp_ts", "vp_lat", "vp_lon", "vp_bearing",
        "stop_lat", "stop_lon", "distance_to_stop",
        "actual_arrival", "time_to_arrival_seconds",
        "hour", "day_of_week", "is_weekend", "is_holiday", "is_peak_hour",
        "headway_seconds", "current_speed_kmh",
        "temperature_c", "precipitation_mm", "wind_speed_kmh",
    ]
    
    for c in wanted_cols:
        if c not in df.columns:
            df[c] = np.nan
    
    result = df[wanted_cols].reset_index(drop=True)
    
    print("\n" + "=" * 70)
    print(f"✓ Final dataset: {len(result):,} rows × {len(wanted_cols)} columns")
    print(f"  Unique trips: {result['trip_id'].nunique():,}")
    print(f"  Unique routes: {result['route_id'].nunique():,}")
    print(f"  Unique stops: {result['stop_id'].nunique():,}")
    print(f"  Avg time_to_arrival: {result['time_to_arrival_seconds'].mean():.1f}s ({result['time_to_arrival_seconds'].mean()/60:.1f} min)")
    print("=" * 70)
    
    return result


def save_dataset(df: pd.DataFrame, output_path: str):
    """Save to parquet with compression."""
    print(f"\nSaving to {output_path}...")
    df.to_parquet(output_path, compression="snappy", index=False)
    print(f"✓ Saved {len(df):,} rows")