import json
import os
import random
from pathlib import Path

from django.core.management.base import BaseCommand
from django.conf import settings

from gtfs.models_schedule import (
    AgencySchedule,
    RouteSchedule,
    CalendarSchedule,
    CalendarDateSchedule,
    ShapeSchedule,
    StopSchedule,
    TripSchedule,
    StopTimeSchedule,
    FeedInfoSchedule,
)


def obj(app_label, model_cls, pk, fields):
    return {
        "model": f"{app_label}.{model_cls.__name__.lower()}",
        "pk": pk,
        "fields": fields,
    }


class Command(BaseCommand):
    help = "Generate a minimal, deterministic GTFS Schedule fixture."

    def add_arguments(self, parser):
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Random seed for deterministic output (default: 42).",
        )
        parser.add_argument(
            "--output",
            type=str,
            default=str(Path("gtfs/fixtures/schedule_min.json")),
            help="Output path for the generated fixture (default: gtfs/fixtures/schedule_min.json).",
        )

    def handle(self, *args, **options):
        seed = options["seed"]
        output = Path(options["output"])
        output.parent.mkdir(parents=True, exist_ok=True)

        rnd = random.Random(seed)
        app_label = "gtfs"

        data = []

        # Agency
        data.append(obj(app_label, AgencySchedule, "A1", {
            "agency_name": "Demo Transit",
            "agency_url": "https://demo.example",
            "agency_timezone": "UTC",
            "agency_phone": "000-000",
            "agency_email": "info@demo.example",
        }))

        # Calendar
        data.append(obj(app_label, CalendarSchedule, "WKDY", {
            "monday": 1, "tuesday": 1, "wednesday": 1,
            "thursday": 1, "friday": 1, "saturday": 0, "sunday": 0,
            "start_date": "2025-01-01", "end_date": "2025-12-31",
        }))
        data.append(obj(app_label, CalendarDateSchedule, None, {
            "service": "WKDY",
            "date": "2025-05-01",
            "exception_type": 1,
        }))

        # Route
        data.append(obj(app_label, RouteSchedule, "R10", {
            "agency": "A1",
            "route_short_name": "10",
            "route_long_name": "Central Line",
            "route_desc": "Main corridor",
            "route_type": 3,
            "route_color": "0044AA",
            "route_text_color": "FFFFFF",
        }))

        # Shape (3 points)
        for seq, (lat, lon) in enumerate([(9.93, -84.08), (9.94, -84.07), (9.95, -84.06)], start=1):
            data.append(obj(app_label, ShapeSchedule, seq, {  
                "shape_id": "S1",
                "shape_pt_lat": lat,
                "shape_pt_lon": lon,
                "shape_pt_sequence": seq,
                "shape_dist_traveled": float(seq - 1),
    }))


        # Stops (2)
        stops = [
            ("ST1", "Central Station", 9.93, -84.08),
            ("ST2", "North Park", 9.95, -84.06),
        ]
        for sid, name, lat, lon in stops:
            data.append(obj(app_label, StopSchedule, sid, {
                "stop_code": sid,
                "stop_name": name,
                "stop_desc": "",
                "stop_lat": lat,
                "stop_lon": lon,
                "zone_id": "",
                "location_type": 0,
                "parent_station": None,
                "stop_timezone": "",
                "wheelchair_boarding": 0,
            }))

        # Trip
        data.append(obj(app_label, TripSchedule, "T100", {
            "route": "R10",
            "service": "WKDY",
            "trip_headsign": "Northbound",
            "trip_short_name": "NB-10",
            "direction_id": 0,
            "block_id": "",
            "shape": None,  # shape FK opcional, podemos dejarlo None
            "wheelchair_accessible": 1,
        }))

        # StopTimes (seq 1..2)
        times = [("08:00:00", "08:00:00"), ("08:10:00", "08:10:00")]
        for seq, (arr, dep) in enumerate(times, start=1):
            data.append(obj(app_label, StopTimeSchedule, None, {
                "trip": "T100",
                "stop": stops[seq-1][0],
                "stop_sequence": seq,
                "arrival_time": arr,
                "departure_time": dep,
                "stop_headsign": "",
                "pickup_type": 0,
                "drop_off_type": 0,
                "shape_dist_traveled": float(seq - 1),
                "timepoint": 1,
            }))

        # FeedInfo
        data.append(obj(app_label, FeedInfoSchedule, None, {
            "feed_publisher_name": "SIMOVILab",
            "feed_publisher_url": "https://simovilab.org",
            "feed_lang": "en",
            "feed_version": "0.1.0",
            "feed_start_date": "2025-01-01",
            "feed_end_date": "2025-12-31",
            "feed_contact_email": "admin@simovilab.org",
            "feed_contact_url": "https://simovilab.org/contact",
        }))

        with output.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        self.stdout.write(self.style.SUCCESS(f"Fixture written to: {output}"))
