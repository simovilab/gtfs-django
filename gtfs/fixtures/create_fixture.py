"""
Generate minimal deterministic fixtures for GTFS Schedule
---------------------------------------------------------
Usage:
    python gtfs/fixtures/create_fixture.py
or with options:
    python gtfs/fixtures/create_fixture.py --seed 42 --output fixtures/schedule_fixture.json
"""

import os
import sys
import json
import random
import django
from datetime import date, time
from pathlib import Path

# ─────────────────────────────
# Django setup
# ─────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")
django.setup()

from gtfs.models import (
    FeedInfoSchedule,
    AgencySchedule,
    RouteSchedule,
    CalendarSchedule,
    TripSchedule,
    StopSchedule,
    StopTimeSchedule,
    ShapeSchedule,
    CalendarDateSchedule,
)

# ─────────────────────────────
# Fixture generator
# ─────────────────────────────
def create_fixtures(seed: int = 42, output_path: str = "gtfs/fixtures/schedule_fixture.json"):
    """Generate minimal deterministic GTFS Schedule dataset."""
    random.seed(seed)
    print(f"🎯 Generating GTFS Schedule fixtures (seed={seed})")

    # Clean previous data
    FeedInfoSchedule.objects.all().delete()

    # Feed
    feed = FeedInfoSchedule.objects.create(
        feed_publisher_name="UCR Feed Demo",
        feed_publisher_url="https://ucr.ac.cr",
        feed_lang="es",
        feed_version="v1.0"
    )

    # Agency
    agency = AgencySchedule.objects.create(
        feed=feed,
        agency_id="UCR",
        agency_name="Universidad de Costa Rica",
        agency_url="https://ucr.ac.cr",
        agency_timezone="America/Costa_Rica"
    )

    # Route
    route = RouteSchedule.objects.create(
        feed=feed,
        route_id="R1",
        agency=agency,
        route_short_name="1",
        route_long_name="Campus a San Pedro",
        route_type=3
    )

    # Calendar
    calendar = CalendarSchedule.objects.create(
        feed=feed,
        service_id="S2025",
        monday=1, tuesday=1, wednesday=1, thursday=1, friday=1,
        saturday=0, sunday=0,
        start_date=date(2025, 3, 1),
        end_date=date(2025, 12, 31)
    )

    # Shape
    shape = ShapeSchedule.objects.create(
        feed=feed,
        shape_id="Shape1",
        shape_pt_lat=9.936,
        shape_pt_lon=-84.054,
        shape_pt_sequence=1
    )

    # Stop
    stop = StopSchedule.objects.create(
        feed=feed,
        stop_id="SP01",
        stop_name="Parada San Pedro",
        stop_lat=9.936,
        stop_lon=-84.054
    )

    # Trip
    trip = TripSchedule.objects.create(
        feed=feed,
        trip_id="T100",
        route=route,
        service=calendar,
        trip_headsign="San Pedro",
        shape=shape
    )

    # StopTime
    stoptime = StopTimeSchedule.objects.create(
        feed=feed,
        trip=trip,
        stop=stop,
        arrival_time=time(7, 30),
        departure_time=time(7, 31),
        stop_sequence=1
    )

    # CalendarDate
    caldate = CalendarDateSchedule.objects.create(
        feed=feed,
        service=calendar,
        date=date(2025, 4, 1),
        exception_type=1
    )

    # Output summary
    data = {
        "feed": feed.feed_publisher_name,
        "agency": agency.agency_name,
        "route": route.route_long_name,
        "trip": trip.trip_headsign,
        "stop": stop.stop_name,
        "calendar_date": str(caldate.date)
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"✅ Fixture successfully written → {output_path}")


# ─────────────────────────────
# CLI entrypoint
# ─────────────────────────────
if __name__ == "__main__":
    seed = 42
    output = "gtfs/fixtures/schedule_fixture.json"

    if "--seed" in sys.argv:
        idx = sys.argv.index("--seed") + 1
        if idx < len(sys.argv):
            seed = int(sys.argv[idx])

    if "--output" in sys.argv:
        idx = sys.argv.index("--output") + 1
        if idx < len(sys.argv):
            output = sys.argv[idx]

    create_fixtures(seed=seed, output_path=output)
