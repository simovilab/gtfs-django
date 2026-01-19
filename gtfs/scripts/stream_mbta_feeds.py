"""
stream_mbta_feeds.py
----------------------------------------
Fetches GTFS-Realtime feeds from MBTA every 15 seconds
and stores them in the local Django database.

Uses the GTFSProvider, FeedMessage, TripUpdate,
VehiclePosition, and Alert models from gtfs.models.

Configured for use within "tests.settings".
"""
import os
import sys
import time
import requests
from datetime import datetime
from pathlib import Path
from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format
from django.utils import timezone

# ====================================================
# Django environment setup
# ====================================================
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")

import django
django.setup()

# ====================================================
# Django models import
# ====================================================
from gtfs.models import (
    GTFSProvider,
    Feed,
    FeedMessage,
    TripUpdate,
    VehiclePosition,
    Alert,
)

# ====================================================
# MBTA Realtime feed URLs
# ====================================================
MBTA_URLS = {
    "trip_update": "https://cdn.mbta.com/realtime/TripUpdates.pb",
    "vehicle": "https://cdn.mbta.com/realtime/VehiclePositions.pb",
    "alert": "https://cdn.mbta.com/realtime/Alerts.pb",
}

# ====================================================
# Helper functions
# ====================================================

def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    """Fetch a GTFS-Realtime feed from a URL and parse it using gtfs-realtime-bindings."""
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        return feed
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
        return None


def store_feed(feed: gtfs_realtime_pb2.FeedMessage, entity_type: str, provider: GTFSProvider):
    """Store GTFS-Realtime FeedMessage contents into the database."""
    if feed is None:
        print(f"[WARN] Skipping {entity_type}: empty feed")
        return

    # Create FeedMessage record
    feed_msg = FeedMessage.objects.create(
        feed_message_id=f"{provider.code}_{entity_type}_{int(timezone.now().timestamp())}",
        provider=provider,
        entity_type=entity_type,
        gtfs_realtime_version=getattr(feed.header, "gtfs_realtime_version", "2.0"),
        incrementality=str(getattr(feed.header, "incrementality", "FULL_DATASET")),
    )

    # -------------------
    # Trip Updates
    # -------------------
    if entity_type == "trip_update":
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            trip_update = entity.trip_update
            trip = trip_update.trip
            TripUpdate.objects.create(
                entity_id=entity.id,
                feed_message=feed_msg,
                trip_trip_id=trip.trip_id,
                trip_route_id=trip.route_id,
                trip_direction_id=trip.direction_id if trip.HasField("direction_id") else None,
                timestamp=timezone.make_aware(datetime.fromtimestamp(feed.header.timestamp)),
                delay=0,
            )

    # -------------------
    # Vehicle Positions
    # -------------------
    elif entity_type == "vehicle":
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            v = entity.vehicle
            VehiclePosition.objects.create(
                entity_id=entity.id,
                feed_message=feed_msg,
                vehicle_trip_trip_id=v.trip.trip_id if v.HasField("trip") else None,
                vehicle_position_latitude=v.position.latitude if v.HasField("position") else None,
                vehicle_position_longitude=v.position.longitude if v.HasField("position") else None,
                vehicle_timestamp=(
                    timezone.make_aware(datetime.fromtimestamp(v.timestamp))
                    if v.HasField("timestamp") else None
                ),
                vehicle_current_status=str(v.current_status),
            )

    # -------------------
    # Alerts
    # -------------------
    elif entity_type == "alert":
        # Ensure a corresponding Feed object exists for the provider
        feed_obj, _ = Feed.objects.get_or_create(
            feed_id=f"{provider.code}_alerts",
            defaults={"gtfs_provider": provider, "is_current": True},
        )

        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue

            alert = entity.alert
            header = alert.header_text.translation[0].text if alert.header_text.translation else ""
            desc = alert.description_text.translation[0].text if alert.description_text.translation else ""

            # Convert informed entities
            informed_entities = [json_format.MessageToDict(e) for e in alert.informed_entity]

            # Upsert (create or update if exists)
            Alert.objects.update_or_create(
                alert_id=entity.id,
                feed=feed_obj,
                defaults={
                    "alert_header": header,
                    "alert_description": desc,
                    "informed_entity": informed_entities,
                    "service_date": timezone.now().date(),
                    "service_start_time": timezone.now().time(),
                    "service_end_time": timezone.now().time(),
                    "cause": getattr(alert, "cause", 1),
                    "effect": getattr(alert, "effect", 1),
                    "severity": getattr(alert, "severity_level", 1),
                    "published": timezone.now(),
                    "updated": timezone.now(),
                },
            )


def stream_mbta(interval=15):
    """Periodically fetch and store MBTA GTFS-Realtime feeds every <interval> seconds."""
    print(f"[{timezone.now()}] Starting MBTA realtime streamer (every {interval}s)...")

    # Ensure provider record exists
    provider, _ = GTFSProvider.objects.get_or_create(
        code="MBTA",
        defaults={
            "name": "Massachusetts Bay Transportation Authority",
            "description": "Public transport authority for Greater Boston",
            "website": "https://www.mbta.com/",
            "timezone": "America/New_York",
            "is_active": True,
            "trip_updates_url": MBTA_URLS["trip_update"],
            "vehicle_positions_url": MBTA_URLS["vehicle"],
            "service_alerts_url": MBTA_URLS["alert"],
        },
    )

    # Main loop
    while True:
        try:
            for entity_type, url in MBTA_URLS.items():
                print(f"[{timezone.now()}] Fetching {entity_type}...")
                feed = fetch_feed(url)
                store_feed(feed, entity_type, provider)
            print(f"[{timezone.now()}] Cycle completed successfully.\n")
        except Exception as e:
            print(f"[{timezone.now()}] Error in cycle: {e}")
        time.sleep(interval)


def main():
    """Main entry point for module execution."""
    stream_mbta(interval=15)


if __name__ == "__main__":
    main()
