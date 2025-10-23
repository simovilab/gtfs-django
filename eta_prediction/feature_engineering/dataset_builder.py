from __future__ import annotations

from datetime import datetime, date, time, timedelta
from typing import Optional, List

import numpy as np
import pandas as pd

from django.db.models import F, Q, OuterRef, Subquery, Exists, Prefetch
from django.db.models.functions import Coalesce

from sch_pipeline.models import StopTime, Stop, Route, Trip
from rt_pipeline.models import VehiclePosition, TripUpdate
from sch_pipeline.utils import top_routes_by_scheduled_trips

from feature_engineering.temporal import extract_temporal_features
from feature_engineering.operational import calculate_headway, detect_congestion_proxy
from feature_engineering.weather import fetch_weather


def _parse_sched_time(val) -> Optional[int]:
    """
    Normalize schedule arrival_time to seconds since midnight.
    Accepts 'HH:MM:SS', integer seconds, or time object.
    """
    if val is None or pd.isna(val):
        return None
    
    # If it's already an integer (seconds), return it
    if isinstance(val, int):
        return val
    
    # If it's a time object
    if isinstance(val, time):
        return val.hour * 3600 + val.minute * 60 + val.second
    
    # If it's a timedelta (from GTFS sometimes)
    if isinstance(val, timedelta):
        return int(val.total_seconds())
    
    # If it's a string
    if isinstance(val, str):
        try:
            parts = val.split(":")
            hh, mm, ss = int(parts[0]), int(parts[1]), int(parts[2])
            return hh * 3600 + mm * 60 + ss
        except Exception:
            return None
    
    return None


def _yyyymmdd_to_date(s: Optional[str]) -> Optional[date]:
    """Convert YYYYMMDD string or date object to date object."""
    if not s or pd.isna(s):
        return None
    
    # If it's already a date object, return it
    if isinstance(s, date):
        return s
    
    # If it's a datetime, extract the date
    if isinstance(s, datetime):
        return s.date()
    
    try:
        s = str(s)
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    except Exception:
        return None


def _mk_dt(d: Optional[date], seconds_since_midnight: Optional[int]) -> Optional[datetime]:
    """Combine date and seconds to create datetime."""
    if d is None or seconds_since_midnight is None:
        return None
    return datetime.combine(d, time(0, 0)) + timedelta(seconds=int(seconds_since_midnight))


def build_training_dataset(
    provider_id: Optional[int] = None,
    route_ids: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    min_observations_per_stop: int = 30,
    attach_weather: bool = True,
    tz_for_temporal: str = "America/Costa_Rica",
) -> pd.DataFrame:
    """
    Optimized pipeline using direct database join via raw SQL or simplified ORM.
    
    Strategy:
    1. Filter TripUpdates by date range FIRST (reduce dataset)
    2. Get matching trip_ids for routes
    3. Use a single query with proper joins instead of subqueries
    4. Process in pandas for complex computations
    """

    print("=" * 70)
    print("OPTIMIZED DATASET BUILDER")
    print("=" * 70)
    
    # ============================================================
    # STEP 1: Filter TripUpdates first (reduce data volume)
    # ============================================================
    print("\nStep 1: Building TripUpdate filter...")
    
    tu_qs = TripUpdate.objects.all()
    
    # Date range filter
    if start_date:
        print(f"  Filtering start_date >= {start_date}")
        tu_qs = tu_qs.filter(ts__gte=start_date)
    
    if end_date:
        print(f"  Filtering end_date < {end_date}")
        tu_qs = tu_qs.filter(ts__lt=end_date)
    
    # Must have essential fields
    tu_qs = tu_qs.exclude(
        Q(trip_id__isnull=True) | 
        Q(stop_sequence__isnull=True) |
        Q(start_date__isnull=True)
    )
    
    print(f"  TripUpdates matching date range: {tu_qs.count():,}")
    
    # ============================================================
    # STEP 2: Get trip_ids for specified routes
    # ============================================================
    if route_ids:
        print(f"\nStep 2: Filtering for routes: {route_ids}")
        trip_ids_for_routes = list(
            Trip.objects.filter(route_id__in=route_ids)
            .values_list("trip_id", flat=True)
        )
        print(f"  Found {len(trip_ids_for_routes):,} trips for these routes")
        
        # Filter TripUpdates to only these trips
        tu_qs = tu_qs.filter(trip_id__in=trip_ids_for_routes)
        print(f"  TripUpdates for these routes: {tu_qs.count():,}")
    
    # ============================================================
    # STEP 3: Get TripUpdate data (deduplicated)
    # ============================================================
    print("\nStep 3: Fetching TripUpdate data...")
    
    # Get latest update per (trip_id, stop_sequence)
    # We'll deduplicate in pandas since Django ORM doesn't have good window functions
    tu_data = tu_qs.values(
        'trip_id',
        'stop_sequence',
        'arrival_time',
        'departure_time',
        'ts',
        'start_date',
        'stop_id',
    ).order_by('trip_id', 'stop_sequence', '-ts')
    
    print(f"  Fetching TripUpdate records...")
    tu_df = pd.DataFrame.from_records(tu_data)
    print(f"  Retrieved {len(tu_df):,} TripUpdate records")
    
    if tu_df.empty:
        print("\nWARNING: No TripUpdate data found!")
        return pd.DataFrame()
    
    # Deduplicate: keep latest update per (trip_id, stop_sequence)
    print("  Deduplicating TripUpdates (keeping latest per trip/stop)...")
    tu_df = tu_df.drop_duplicates(subset=['trip_id', 'stop_sequence'], keep='first')
    print(f"  After deduplication: {len(tu_df):,} records")
    
    # Rename for clarity
    tu_df = tu_df.rename(columns={
        'arrival_time': 'tu_arrival',
        'departure_time': 'tu_departure',
        'ts': 'tu_ts',
        'start_date': 'tu_start_date',
        'stop_id': 'tu_stop_id',
    })
    
    # ============================================================
    # STEP 4: Get StopTime data for matching trips
    # ============================================================
    print("\nStep 4: Fetching StopTime schedule data...")
    
    # Get unique trip_ids from TripUpdates
    trip_ids = tu_df['trip_id'].unique()
    print(f"  Unique trips to fetch: {len(trip_ids):,}")
    
    # Fetch StopTimes for these trips
    st_qs = StopTime.objects.filter(trip_id__in=trip_ids)
    
    st_data = st_qs.values(
        'trip_id',
        'stop_sequence',
        'stop_id',
        'arrival_time',
        'departure_time',
    )
    
    print(f"  Fetching StopTime records...")
    st_df = pd.DataFrame.from_records(st_data)
    print(f"  Retrieved {len(st_df):,} StopTime records")
    
    if st_df.empty:
        print("\nWARNING: No StopTime data found!")
        return pd.DataFrame()
    
    # ============================================================
    # STEP 5: Join StopTime + TripUpdate
    # ============================================================
    print("\nStep 5: Joining schedule and realtime data...")
    
    df = st_df.merge(
        tu_df,
        on=['trip_id', 'stop_sequence'],
        how='inner',  # Only keep records with both schedule and realtime data
        suffixes=('_sched', '_tu')
    )
    
    print(f"  After join: {len(df):,} records")
    
    if df.empty:
        print("\nWARNING: No matching records after join!")
        return pd.DataFrame()
    
    # ============================================================
    # STEP 6: Get Trip/Route metadata
    # ============================================================
    print("\nStep 6: Adding route information...")
    
    trip_ids = df['trip_id'].unique()
    trips_data = Trip.objects.filter(trip_id__in=trip_ids).values(
        'trip_id', 'route_id', 'direction_id', 'trip_headsign'
    )
    trips_df = pd.DataFrame.from_records(trips_data)
    
    df = df.merge(trips_df, on='trip_id', how='left')
    print(f"  Added route_id for {len(df):,} records")
    
    # ============================================================
    # STEP 7: Compute scheduled and actual arrival times
    # ============================================================
    print("\nStep 7: Computing scheduled_arrival...")
    
    # Debug: Check what we have
    print(f"  Sample arrival_time values: {df['arrival_time'].head().tolist()}")
    print(f"  Sample arrival_time types: {[type(x) for x in df['arrival_time'].head().tolist()]}")
    print(f"  Sample tu_start_date values: {df['tu_start_date'].head().tolist()}")
    print(f"  Sample tu_start_date types: {[type(x) for x in df['tu_start_date'].head().tolist()]}")
    
    df["sched_secs"] = df["arrival_time"].apply(_parse_sched_time)
    df["start_date_obj"] = df["tu_start_date"].apply(_yyyymmdd_to_date)
    
    print(f"  Parsed sched_secs (non-null): {df['sched_secs'].notna().sum()}/{len(df)}")
    print(f"  Parsed start_date_obj (non-null): {df['start_date_obj'].notna().sum()}/{len(df)}")
    print(f"  Sample sched_secs: {df['sched_secs'].head().tolist()}")
    print(f"  Sample start_date_obj: {df['start_date_obj'].head().tolist()}")
    
    # Check if both are present before combining
    both_valid = (df['sched_secs'].notna() & df['start_date_obj'].notna()).sum()
    print(f"  Rows with BOTH sched_secs AND start_date_obj: {both_valid}/{len(df)}")
    
    df["scheduled_arrival"] = df.apply(
        lambda row: _mk_dt(row["start_date_obj"], row["sched_secs"]),
        axis=1
    )
    
    print(f"  Scheduled arrivals (non-null): {df['scheduled_arrival'].notna().sum()}/{len(df)}")
    print(f"  Sample scheduled_arrival: {df['scheduled_arrival'].head().tolist()}")
    
    print("\nStep 8: Computing actual_arrival...")
    print(f"  Sample tu_arrival: {df['tu_arrival'].head().tolist()}")
    print(f"  Sample tu_departure: {df['tu_departure'].head().tolist()}")
    print(f"  Sample tu_ts: {df['tu_ts'].head().tolist()}")
    print(f"  tu_arrival (non-null): {df['tu_arrival'].notna().sum()}/{len(df)}")
    print(f"  tu_departure (non-null): {df['tu_departure'].notna().sum()}/{len(df)}")
    print(f"  tu_ts (non-null): {df['tu_ts'].notna().sum()}/{len(df)}")
    
    df["actual_arrival"] = df["tu_arrival"].combine_first(
        df["tu_departure"]
    ).combine_first(df["tu_ts"])
    
    print(f"  Actual arrivals (non-null): {df['actual_arrival'].notna().sum()}/{len(df)}")
    print(f"  Sample actual_arrival: {df['actual_arrival'].head().tolist()}")
    
    # Ensure datetime dtype
    for col in ["scheduled_arrival", "actual_arrival"]:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    
    print(f"\n  After pd.to_datetime conversion:")
    print(f"  Scheduled arrivals (non-null): {df['scheduled_arrival'].notna().sum()}/{len(df)}")
    print(f"  Actual arrivals (non-null): {df['actual_arrival'].notna().sum()}/{len(df)}")
    
    # ============================================================
    # STEP 9: Compute delay
    # ============================================================
    print("\nStep 9: Computing delay_seconds...")
    df["delay_seconds"] = (
        df["actual_arrival"] - df["scheduled_arrival"]
    ).dt.total_seconds()
    
    # Drop rows with missing critical data
    initial_count = len(df)
    df = df.dropna(subset=["scheduled_arrival", "actual_arrival", "delay_seconds"])
    print(f"  Dropped {initial_count - len(df):,} rows with missing critical data")
    print(f"  Remaining: {len(df):,} rows")
    
    if df.empty:
        print("\nWARNING: All rows dropped due to missing data")
        return df
    
    # ============================================================
    # STEP 10: Temporal features
    # ============================================================
    print("\nStep 10: Extracting temporal features...")
    temporal_data = []
    for idx, ts in enumerate(df["actual_arrival"]):
        if idx % 10000 == 0 and idx > 0:
            print(f"  Processed {idx:,}/{len(df):,} temporal features")
        
        if pd.notna(ts):
            try:
                feats = extract_temporal_features(ts, tz=tz_for_temporal, region="CR")
                temporal_data.append(feats)
            except Exception as e:
                temporal_data.append({})
        else:
            temporal_data.append({})
    
    tf = pd.DataFrame(temporal_data)
    keep_temporal = ["hour", "day_of_week", "is_weekend", "is_holiday", "is_peak_hour"]
    for k in keep_temporal:
        if k in tf.columns:
            df[k] = tf[k].values
        else:
            df[k] = np.nan
    
    # ============================================================
    # STEP 11: Operational features
    # ============================================================
    print("\nStep 11: Computing operational features...")
    
    def _safe_headway(route_id, timestamp):
        try:
            return calculate_headway(
                route_id=route_id,
                timestamp=timestamp,
                vehicle_id=None
            )
        except Exception:
            return np.nan
    
    def _safe_congestion(route_id, stop_sequence, timestamp):
        try:
            result = detect_congestion_proxy(
                route_id=route_id,
                stop_sequence=stop_sequence,
                timestamp=timestamp,
            )
            return result.get("avg_speed_last_10min", np.nan) if result else np.nan
        except Exception:
            return np.nan
    
    headways = []
    speeds = []
    for idx, row in df.iterrows():
        if idx % 50 == 0:
            print(f"  Processing operational features: {idx:,}/{len(df):,}")
        headways.append(_safe_headway(row["route_id"], row["actual_arrival"]))
        speeds.append(_safe_congestion(row["route_id"], row["stop_sequence"], row["actual_arrival"]))
    
    df["headway_seconds"] = headways
    df["avg_speed_last_10min"] = speeds
    
    
    # ============================================================
    # STEP 12: Weather features
    # ============================================================
    if attach_weather:
        print("\nStep 12: Fetching weather data...")
        
        # Use tu_stop_id (from realtime) or stop_id (from schedule)
        df['final_stop_id'] = df['tu_stop_id'].combine_first(df['stop_id'])
        
        stop_ids = df["final_stop_id"].unique()
        stop_meta = Stop.objects.filter(
            id__in=stop_ids
        ).values("id", "stop_lat", "stop_lon")
        stop_df = pd.DataFrame.from_records(stop_meta).rename(
            columns={"id": "final_stop_id"}
        )
        
        df = df.merge(stop_df, on="final_stop_id", how="left")
        
        weather_data = []
        for idx, row in df.iterrows():
            if idx % 5000 == 0:
                print(f"  Fetching weather: {idx:,}/{len(df):,}")
            
            if pd.notna(row.get("stop_lat")) and pd.notna(row.get("stop_lon")) and pd.notna(row.get("actual_arrival")):
                try:
                    w = fetch_weather(
                        float(row["stop_lat"]),
                        float(row["stop_lon"]),
                        pd.Timestamp(row["actual_arrival"]).to_pydatetime()
                    )
                    weather_data.append(w or {})
                except Exception:
                    weather_data.append({})
            else:
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
    # STEP 13: Outlier filtering
    # ============================================================
    # print("\nStep 13: Filtering outliers...")
    # initial_count = len(df)
    
    # # Clip extreme delays (±2 hours)
    # df = df[(df["delay_seconds"] >= -7200) & (df["delay_seconds"] <= 7200)]
    # print(f"  Dropped {initial_count - len(df):,} outlier rows")
    
    # ============================================================
    # STEP 14: Minimum observations per stop
    # ============================================================
    print(f"\nStep 14: Filtering stops with < {min_observations_per_stop} observations...")
    
    df['final_stop_id'] = df['tu_stop_id'].combine_first(df['stop_id'])
    counts = df.groupby("final_stop_id").size()
    keep_stops = set(counts[counts >= min_observations_per_stop].index)
    
    initial_count = len(df)
    df = df[df["final_stop_id"].isin(keep_stops)]
    print(f"  Kept {len(keep_stops)} stops, dropped {initial_count - len(df):,} rows")
    
    # ============================================================
    # STEP 15: Final column selection
    # ============================================================
    print("\nStep 15: Preparing final dataset...")
    
    # Use tu_stop_id preferentially, fallback to schedule stop_id
    df['stop_id'] = df['tu_stop_id'].combine_first(df['stop_id'])
    
    wanted_cols = [
        "trip_id", "route_id", "stop_id", "stop_sequence",
        "scheduled_arrival", "actual_arrival", "delay_seconds",
        "hour", "day_of_week", "is_weekend", "is_holiday", "is_peak_hour",
        "headway_seconds", "avg_speed_last_10min",
        "temperature_c", "precipitation_mm", "wind_speed_kmh",
    ]
    
    for c in wanted_cols:
        if c not in df.columns:
            df[c] = np.nan
    
    result = df[wanted_cols].reset_index(drop=True)
    
    print("\n" + "=" * 70)
    print(f"✓ Final dataset: {len(result):,} rows × {len(wanted_cols)} columns")
    print("=" * 70)
    
    return result


def save_dataset(df: pd.DataFrame, output_path: str):
    """Save to parquet with compression."""
    print(f"\nSaving to {output_path}...")
    df.to_parquet(output_path, compression="snappy", index=False)
    print(f"✓ Saved {len(df):,} rows")