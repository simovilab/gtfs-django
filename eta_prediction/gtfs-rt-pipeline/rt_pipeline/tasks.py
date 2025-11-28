from celery import shared_task
from django.conf import settings
import requests
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone
from django.db import transaction, IntegrityError
from .models import RawMessage, VehiclePosition, TripUpdate
import hashlib

def _sha256_hex(b: bytes) -> str: return hashlib.sha256(b).hexdigest()
def _to_ts(sec): return datetime.fromtimestamp(sec, tz=timezone.utc) if sec else None
def _now(): return datetime.now(timezone.utc)

# Vehicle Positions tasks
@shared_task(bind=True, autoretry_for=(requests.RequestException,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def fetch_vehicle_positions(self):
    url = settings.GTFSRT_VEHICLE_POSITIONS_URL
    r = requests.get(url, timeout=(settings.HTTP_CONNECT_TIMEOUT, settings.HTTP_READ_TIMEOUT))
    r.raise_for_status()
    if not r.content:
        return {"skipped": True}

    h = _sha256_hex(r.content)

    # Parse header early
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    header_ts = _to_ts(feed.header.timestamp) if feed.HasField("header") else None
    inc = None
    if feed.HasField("header"):
        inc_val = feed.header.incrementality  # this is an int
        try:
            # Map int -> enum name (e.g., 0 -> "FULL_DATASET", 1 -> "DIFFERENTIAL")
            inc = gtfs_realtime_pb2.FeedHeader.Incrementality.Name(inc_val)
        except Exception:
            # Fallback: store the raw int as string if mapping not available
            inc = str(inc_val)    

    try:
        with transaction.atomic():
            obj = RawMessage.objects.create(
                feed_name=settings.FEED_NAME,
                message_type=RawMessage.MESSAGE_TYPE_VEHICLE_POSITIONS,
                header_timestamp=header_ts,
                incrementality=inc,
                content=r.content,
                content_hash=h,
            )
    except IntegrityError:
        # duplicate blob
        existing = RawMessage.objects.filter(
            feed_name=settings.FEED_NAME,
            message_type=RawMessage.MESSAGE_TYPE_VEHICLE_POSITIONS,
            content_hash=h
        ).first()
        return {"created": False, "raw_message_id": str(existing.id) if existing else None}

    parse_and_upsert_vehicle_positions.delay(str(obj.id))
    return {"created": True, "raw_message_id": str(obj.id)}

@shared_task(bind=True)
def parse_and_upsert_vehicle_positions(self, raw_message_id: str):
    raw = RawMessage.objects.filter(id=raw_message_id).only("content").first()
    if not raw: return {"error": "raw_not_found"}

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(bytes(raw.content))

    rows = []
    for ent in feed.entity:
        if not ent.HasField("vehicle"):
            continue
        v = ent.vehicle
        vh_id = (v.vehicle.id or v.vehicle.label or ent.id or "unknown").strip()
        ts = _to_ts(v.timestamp) or _now()
        lat = v.position.latitude if v.HasField("position") else None
        lon = v.position.longitude if v.HasField("position") else None
        bearing = v.position.bearing if v.HasField("position") else None
        speed = v.position.speed if v.HasField("position") else None
        route_id = v.trip.route_id if v.HasField("trip") else None
        trip_id = v.trip.trip_id if v.HasField("trip") else None
        css = v.current_stop_sequence if v.HasField("current_stop_sequence") else None

        rows.append(VehiclePosition(
            feed_name=settings.FEED_NAME, vehicle_id=vh_id, ts=ts, lat=lat, lon=lon,
            bearing=bearing, speed=speed, route_id=route_id, trip_id=trip_id,
            current_stop_sequence=css, raw_message_id=raw_message_id
        ))

    if not rows:
        return {"inserted": 0}

    # Bulk insert with conflict handling (Django 5+)
    # We'll try bulk_create(ignore_conflicts=True) then optional updates
    inserted = 0
    try:
        VehiclePosition.objects.bulk_create(rows, ignore_conflicts=True, batch_size=2000)
        inserted = len(rows)  # approximate; conflicts ignored
    except Exception as e:
        return {"error": str(e)}

    return {"inserted": inserted}

# Trip Updates tasks
@shared_task(bind=True, autoretry_for=(requests.RequestException,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def fetch_trip_updates(self):
    """Download TU .pb and store a RawMessage. Enqueue parse task only for NEW content."""
    url = getattr(settings, "GTFSRT_TRIP_UPDATES_URL", None)
    if not url:
        return {"error": "GTFSRT_TRIP_UPDATES_URL not configured"}

    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    raw_bytes = resp.content
    content_hash = _sha256_hex(raw_bytes)

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_bytes)

    header_ts = _to_ts(getattr(feed.header, "timestamp", None))
    incr = str(getattr(feed.header, "incrementality", "")) if hasattr(feed.header, "incrementality") else None

    try:
        with transaction.atomic():
            raw = RawMessage.objects.create(
                feed_name=settings.FEED_NAME,
                message_type=RawMessage.MESSAGE_TYPE_TRIP_UPDATES,
                content_hash=content_hash,
                header_timestamp=header_ts,
                incrementality=incr,
                content=raw_bytes
            )
    except IntegrityError:
        # duplicate blob
        existing = RawMessage.objects.filter(
            feed_name=settings.FEED_NAME,
            message_type=RawMessage.MESSAGE_TYPE_TRIP_UPDATES,
            content_hash=content_hash
        ).first()
        return {"created": False, "raw_message_id": str(existing.id) if existing else None}

    # only parse if this payload is new
    parse_and_upsert_trip_updates.delay(str(raw.id))
    return {"created": True, "raw_message_id": str(raw.id), "hash": content_hash}

@shared_task(bind=True)
def parse_and_upsert_trip_updates(self, raw_id: str):
    """Parse a stored RawMessage (TU) into TripUpdate rows (one per StopTimeUpdate)."""
    raw = RawMessage.objects.get(id=raw_id)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(bytes(raw.content))

    rows = []
    for ent in feed.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        tu_ts = _to_ts(getattr(tu, "timestamp", None))

        trip = getattr(tu, "trip", None)
        veh = getattr(tu, "vehicle", None)

        trip_id = getattr(trip, "trip_id", None) or None
        route_id = getattr(trip, "route_id", None) or None
        start_time = getattr(trip, "start_time", None) or None
        start_date = getattr(trip, "start_date", None) or None
        header_sr = str(getattr(trip, "schedule_relationship", "")) if trip else None
        vehicle_id = getattr(veh, "id", None) or None

        for stu in getattr(tu, "stop_time_update", []):
            stop_sequence = getattr(stu, "stop_sequence", None)
            stop_id = getattr(stu, "stop_id", None) or None

            arr = getattr(stu, "arrival", None)
            dep = getattr(stu, "departure", None)

            arrival_delay = getattr(arr, "delay", None) if arr else None
            arrival_time = _to_ts(getattr(arr, "time", None)) if arr and getattr(arr, "time", None) else None
            departure_delay = getattr(dep, "delay", None) if dep else None
            departure_time = _to_ts(getattr(dep, "time", None)) if dep and getattr(dep, "time", None) else None
            stu_sr = str(getattr(stu, "schedule_relationship", "")) if stu else None

            rows.append(TripUpdate(
                feed_name=settings.FEED_NAME,
                ts=tu_ts,
                trip_id=trip_id,
                route_id=route_id,
                start_time=start_time,
                start_date=start_date,
                schedule_relationship=header_sr,
                vehicle_id=vehicle_id,
                stop_sequence=stop_sequence,
                stop_id=stop_id,
                arrival_delay=arrival_delay,
                arrival_time=arrival_time,
                departure_delay=departure_delay,
                departure_time=departure_time,
                stu_schedule_relationship=stu_sr,
                raw_message=raw,
            ))

    if rows:
        TripUpdate.objects.bulk_create(rows, ignore_conflicts=True)
    return {"parsed_rows": len(rows)}

# ---- Celery Beat schedule ----
from celery.schedules import schedule
from ingestproj.celery import app as celery_app

celery_app.conf.beat_schedule = {
    "poll-vehicle-positions": {
        "task": "rt_pipeline.tasks.fetch_vehicle_positions",
        "schedule": schedule(run_every=settings.POLL_SECONDS),
    },
    "poll-trip-updates": {
        "task": "rt_pipeline.tasks.fetch_trip_updates",
        "schedule": schedule(run_every=settings.POLL_SECONDS),
    },
}