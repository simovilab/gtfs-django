#!/usr/bin/env python3
"""
regenerate_fixtures.py
----------------------------------------
Generates small reproducible GTFS-Realtime fixture datasets.

These fixtures are used for unit tests and documentation.
They follow the GTFS-Realtime v2.0 specification.

TripUpdates are generated deterministically using ETAModule and
the Bytewax builder implemented in gtfs/utils/realtime.py.
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# ==============================================================
# Environment setup to import gtfs without requiring full Django
# ==============================================================

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

# Attempt minimal Django setup (used in tests too)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")
try:
    import django
    django.setup()
except Exception:
    pass  # Safe fallback for standalone execution

# Safe import of realtime module
try:
    from gtfs.utils import realtime
except Exception as e:
    print(f"Warning: could not import gtfs.utils.realtime normally ({e}).")
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "realtime", str(BASE_DIR / "gtfs" / "utils" / "realtime.py")
    )
    realtime = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(realtime)

# ==============================================================
# Paths and utility functions
# ==============================================================

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures")
os.makedirs(FIXTURE_DIR, exist_ok=True)


def generate_trip_updates():
    """
    Generates and saves a deterministic TripUpdates fixture.

    Uses the build_trip_updates_bytewax() function from realtime.py
    to produce a reproducible synthetic feed message.
    """
    realtime.build_trip_updates_bytewax()
    src = "feed/files/trip_updates_bytewax.json"
    dst = os.path.join(FIXTURE_DIR, "sample_trip_updates.json")

    if os.path.exists(src):
        with open(src, "r", encoding="utf-8") as f:
            data = json.load(f)
        with open(dst, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"TripUpdates fixture updated → {dst}")
    else:
        print("TripUpdates source file not found; run Bytewax builder first.")


def generate_vehicle_positions():
    """
    Creates a minimal synthetic VehiclePositions fixture.

    This fixture contains static values and is not generated
    from a live source, ensuring full reproducibility.
    """
    vehicles = [{
        "vehicle_id": "V001",
        "trip_id": "FAKE_TRIP_001",
        "latitude": 9.93,
        "longitude": -84.08,
        "speed": 15,
        "timestamp": int(datetime.now().timestamp())
    }]
    dst = os.path.join(FIXTURE_DIR, "sample_vehicle_positions.json")
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(vehicles, f, indent=2)
    print(f"VehiclePositions fixture updated → {dst}")


def generate_alerts():
    """
    Creates a minimal deterministic Alerts fixture.

    Includes required and recommended GTFS-Realtime fields:
    header_text, description_text, informed_entity,
    active_period, cause, effect, and severity_level.
    """
    alerts = [{
        "alert_id": "A001",
        "route_id": "R01",
        "header_text": "Service interruption on R01",
        "description_text": "Maintenance from 14:00 to 18:00",
        "severity": "moderate",
        "timestamp": int(datetime.now().timestamp())
    }]
    dst = os.path.join(FIXTURE_DIR, "sample_alerts.json")
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(alerts, f, indent=2)
    print(f"Alerts fixture updated → {dst}")


if __name__ == "__main__":
    print("Regenerating deterministic fixtures...")
    generate_trip_updates()
    generate_vehicle_positions()
    generate_alerts()
    print("Fixtures generated successfully.")
