"""
Debug helper to diagnose data availability issues.
Run with: python manage.py shell < debug_data.py
"""

from django.db.models import Min, Max, Count, Q
from datetime import timedelta
from django.utils import timezone


import os
# init Django before importing models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ingestproj.settings")
import django
django.setup()



from sch_pipeline.models import StopTime, Stop, Route, Trip
from rt_pipeline.models import VehiclePosition, TripUpdate

print("="*70)
print("GTFS-RT PIPELINE DATA DIAGNOSTICS")
print("="*70)

# 1. Check Trip/Route data
print("\n1. SCHEDULE DATA (GTFS)")
print("-" * 70)
trip_count = Trip.objects.count()
route_count = Route.objects.count()
stop_count = Stop.objects.count()
stoptime_count = StopTime.objects.count()

print(f"  Trips: {trip_count:,}")
print(f"  Routes: {route_count:,}")
print(f"  Stops: {stop_count:,}")
print(f"  StopTimes: {stoptime_count:,}")

if route_count > 0:
    top_routes = (
        Trip.objects
        .values("route_id")
        .annotate(trip_count=Count("id"))
        .order_by("-trip_count")[:5]
    )
    print(f"\n  Top 5 routes by trip count:")
    for r in top_routes:
        print(f"    {r['route_id']}: {r['trip_count']:,} trips")

# 2. Check realtime data
print("\n2. REALTIME DATA (GTFS-RT)")
print("-" * 70)
vp_count = VehiclePosition.objects.count()
tu_count = TripUpdate.objects.count()

print(f"  VehiclePositions: {vp_count:,}")
print(f"  TripUpdates: {tu_count:,}")

if tu_count > 0:
    tu_stats = TripUpdate.objects.aggregate(
        min_ts=Min("ts"),
        max_ts=Max("ts"),
        unique_trips=Count("trip_id", distinct=True),
        unique_stops=Count("stop_id", distinct=True),
    )
    print(f"\n  TripUpdate time range:")
    print(f"    Earliest: {tu_stats['min_ts']}")
    print(f"    Latest: {tu_stats['max_ts']}")
    if tu_stats['min_ts'] and tu_stats['max_ts']:
        span = tu_stats['max_ts'] - tu_stats['min_ts']
        print(f"    Span: {span.days} days")
    print(f"  Unique trip_ids: {tu_stats['unique_trips']:,}")
    print(f"  Unique stop_ids: {tu_stats['unique_stops']:,}")

    # Check for start_date field
    with_start_date = TripUpdate.objects.exclude(start_date__isnull=True).count()
    print(f"  TripUpdates with start_date: {with_start_date:,} ({100*with_start_date/tu_count:.1f}%)")
    
    # Check for arrival_time
    with_arrival = TripUpdate.objects.exclude(arrival_time__isnull=True).count()
    print(f"  TripUpdates with arrival_time: {with_arrival:,} ({100*with_arrival/tu_count:.1f}%)")

# 3. Check join potential
print("\n3. JOIN DIAGNOSTICS")
print("-" * 70)

if trip_count > 0 and tu_count > 0:
    # Check trip_id overlap
    sample_trip_ids = set(Trip.objects.values_list("trip_id", flat=True)[:])
    tu_trip_ids = set(TripUpdate.objects.values_list("trip_id", flat=True).distinct()[:])
    overlap = sample_trip_ids & tu_trip_ids
    
    print(f"  Sample trip_id overlap:")
    print(f"    Trip table (sample): {len(sample_trip_ids)}")
    print(f"    TripUpdate table (sample): {len(tu_trip_ids)}")
    print(f"    Overlap: {len(overlap)}")
    
    if overlap:
        print(f"    Example matching trip_ids: {list(overlap)[:3]}")
    else:
        print(f"    ⚠️  NO OVERLAP - trip_ids don't match between tables!")
        print(f"    Trip examples: {list(sample_trip_ids)[:3]}")
        print(f"    TripUpdate examples: {list(tu_trip_ids)[:3]}")

if stoptime_count > 0 and tu_count > 0:
    # Check stop_id overlap
    st_stop_ids = set(StopTime.objects.values_list("stop_id", flat=True).distinct()[:1000])
    tu_stop_ids = set(TripUpdate.objects.exclude(stop_id__isnull=True).values_list("stop_id", flat=True).distinct()[:1000])
    overlap = st_stop_ids & tu_stop_ids
    
    print(f"\n  Stop_id overlap:")
    print(f"    StopTime table (sample): {len(st_stop_ids)}")
    print(f"    TripUpdate table (sample): {len(tu_stop_ids)}")
    print(f"    Overlap: {len(overlap)}")
    
    if not overlap and st_stop_ids and tu_stop_ids:
        print(f"    ⚠️  NO OVERLAP - stop_ids don't match!")
        print(f"    StopTime examples: {list(st_stop_ids)[:3]}")
        print(f"    TripUpdate examples: {list(tu_stop_ids)[:3]}")

# 4. Sample query test
print("\n4. SAMPLE JOIN TEST")
print("-" * 70)

if trip_count > 0 and tu_count > 0:
    # Try to find one matching record
    from django.db.models import OuterRef, Subquery, Exists
    
    # Get a trip_id that exists in both tables
    tu_trip_ids = set(TripUpdate.objects.values_list("trip_id", flat=True).distinct()[:100])
    matching_trips = Trip.objects.filter(trip_id__in=tu_trip_ids)[:1]
    
    if matching_trips:
        test_trip = matching_trips[0]
        print(f"  Test trip_id: {test_trip.trip_id}")
        print(f"  Route: {test_trip.route_id}")