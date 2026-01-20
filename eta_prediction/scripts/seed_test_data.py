#!/usr/bin/env python
"""
Seed test data into PostgreSQL for end-to-end pipeline testing.

This script creates:
- Sample routes, trips, stops, stop_times (schedule data)
- Sample vehicle positions (realtime data)

Run from eta-cli container:
    python scripts/seed_test_data.py
"""

import os
import sys
import django
from datetime import datetime, timedelta, timezone, time
from decimal import Decimal
import random

# Setup Django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ingestproj.settings")
sys.path.insert(0, "/app/gtfs-rt-pipeline")
django.setup()

from django.db import connection
from rt_pipeline.models import VehiclePosition
from sch_pipeline.models import Route, Trip, Stop, StopTime


def create_test_routes():
    """Create test routes."""
    routes = []
    for route_id in ["171", "172", "173"]:
        route, created = Route.objects.get_or_create(
            route_id=route_id,
            defaults={
                "route_short_name": route_id,
                "route_long_name": f"Test Route {route_id}",
                "route_type": 3,  # Bus
            }
        )
        routes.append(route)
        status = "Created" if created else "Exists"
        print(f"  Route {route_id}: {status}")
    return routes


def create_test_stops():
    """Create test stops along a path."""
    stops = []
    # Costa Rica coordinates (San Jose area)
    base_lat = 9.9281
    base_lon = -84.0907

    for i in range(1, 11):
        stop_id = f"STOP-{i:03d}"
        stop, created = Stop.objects.get_or_create(
            stop_id=stop_id,
            defaults={
                "stop_name": f"Test Stop {i}",
                "stop_lat": Decimal(str(base_lat + (i * 0.001))),
                "stop_lon": Decimal(str(base_lon + (i * 0.001))),
            }
        )
        stops.append(stop)
        status = "Created" if created else "Exists"
        print(f"  Stop {stop_id}: {status}")
    return stops


def create_test_trips(routes):
    """Create test trips for each route."""
    trips = []
    for route in routes:
        for trip_num in range(1, 4):
            trip_id = f"{route.route_id}-TRIP-{trip_num:03d}"
            trip, created = Trip.objects.get_or_create(
                trip_id=trip_id,
                defaults={
                    "route": route,
                    "service_id": "WEEKDAY",
                    "trip_headsign": f"To Terminal via Route {route.route_id}",
                    "direction_id": 0,
                }
            )
            trips.append(trip)
            status = "Created" if created else "Exists"
            print(f"  Trip {trip_id}: {status}")
    return trips


def create_test_stop_times(trips, stops):
    """Create stop times linking trips to stops."""
    count = 0
    for trip in trips:
        base_time = time(hour=random.randint(6, 18), minute=0)
        for seq, stop in enumerate(stops, start=1):
            # Calculate arrival/departure times
            minutes_offset = seq * 3  # 3 minutes between stops
            arrival = time(
                hour=(base_time.hour + minutes_offset // 60) % 24,
                minute=(base_time.minute + minutes_offset) % 60
            )
            departure = time(
                hour=arrival.hour,
                minute=(arrival.minute + 1) % 60
            )

            stop_time, created = StopTime.objects.get_or_create(
                trip=trip,
                stop=stop,
                stop_sequence=seq,
                defaults={
                    "arrival_time": arrival,
                    "departure_time": departure,
                }
            )
            if created:
                count += 1
    print(f"  StopTimes: {count} created")


def create_test_vehicle_positions(trips, stops):
    """Create test vehicle positions simulating vehicles moving along routes."""
    count = 0
    now = datetime.now(timezone.utc)

    for trip in trips:
        vehicle_id = f"BUS-{trip.route.route_id}-{random.randint(100, 999)}"

        # Simulate vehicle moving through stops
        for i, stop in enumerate(stops[:-1]):  # All but last stop
            # Time offset: each stop is ~3 minutes apart
            ts = now - timedelta(minutes=(len(stops) - i) * 3)

            # Position between current and next stop
            next_stop = stops[i + 1]
            progress = random.uniform(0.2, 0.8)
            lat = float(stop.stop_lat) + progress * (float(next_stop.stop_lat) - float(stop.stop_lat))
            lon = float(stop.stop_lon) + progress * (float(next_stop.stop_lon) - float(stop.stop_lon))

            vp = VehiclePosition.objects.create(
                vehicle_id=vehicle_id,
                trip_id=trip.trip_id,
                route_id=trip.route.route_id,
                lat=lat,
                lon=lon,
                bearing=random.randint(0, 360),
                speed=random.uniform(5, 15),
                ts=ts,
                feed_name="test",
            )
            count += 1

    print(f"  VehiclePositions: {count} created")


def seed_redis_data():
    """Seed Redis with route stops for runtime inference."""
    import redis
    import json

    redis_host = os.environ.get("REDIS_HOST", "redis")
    redis_port = int(os.environ.get("REDIS_PORT", 6379))

    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    # Get stops from database
    stops = Stop.objects.all().order_by("stop_id")

    # Create route_stops entries for each route
    for route_id in ["171", "172", "173"]:
        stops_data = {
            "stops": [
                {
                    "stop_id": stop.stop_id,
                    "stop_name": stop.stop_name,
                    "lat": float(stop.stop_lat),
                    "lon": float(stop.stop_lon),
                    "stop_sequence": i + 1,
                }
                for i, stop in enumerate(stops)
            ]
        }
        r.set(f"route_stops:{route_id}", json.dumps(stops_data))
        print(f"  Redis route_stops:{route_id}: seeded")

    # Create a current vehicle position
    vehicle = {
        "vehicle_id": "TEST-LIVE-001",
        "lat": 9.9291,
        "lon": -84.0897,
        "speed": 10.5,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "route": "171",
        "trip_id": "171-TRIP-001",
    }
    r.set("vehicle:TEST-LIVE-001", json.dumps(vehicle))
    print(f"  Redis vehicle:TEST-LIVE-001: seeded")


def main():
    print("\n" + "=" * 60)
    print("SEEDING TEST DATA")
    print("=" * 60)

    print("\n[1/6] Creating routes...")
    routes = create_test_routes()

    print("\n[2/6] Creating stops...")
    stops = create_test_stops()

    print("\n[3/6] Creating trips...")
    trips = create_test_trips(routes)

    print("\n[4/6] Creating stop times...")
    create_test_stop_times(trips, stops)

    print("\n[5/6] Creating vehicle positions...")
    create_test_vehicle_positions(trips, stops)

    print("\n[6/6] Seeding Redis...")
    seed_redis_data()

    print("\n" + "=" * 60)
    print("TEST DATA SEEDED SUCCESSFULLY")
    print("=" * 60)

    # Summary
    print(f"\nDatabase summary:")
    print(f"  Routes: {Route.objects.count()}")
    print(f"  Stops: {Stop.objects.count()}")
    print(f"  Trips: {Trip.objects.count()}")
    print(f"  StopTimes: {StopTime.objects.count()}")
    print(f"  VehiclePositions: {VehiclePosition.objects.count()}")


if __name__ == "__main__":
    main()
