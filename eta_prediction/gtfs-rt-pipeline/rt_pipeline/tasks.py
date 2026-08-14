"""Celery tasks for the MBTA GTFS-RT collector.

VehiclePositions polling writes to a local DuckDB spool
(``rt_pipeline.storage.spool.Spool``) rather than S3 directly -- S3 is only
touched by the hourly ``flush_vp_spool_s3`` task. This decouples "durably
record a poll" from "touch S3", which fixes the incident where a per-poll
``COPY ... PARTITION_BY (..., route_id)`` fanned out into ~160 S3 PUTs (one
per active MBTA route) every 5 seconds -- see ``docs/S3_LAYOUT.md`` and the
project plan for the full writeup.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from celery import shared_task
from django.conf import settings
from django.db import IntegrityError, transaction
import requests
from google.transit import gtfs_realtime_pb2

from . import status
from .models import RawMessage, TripUpdate, VehiclePosition
from .storage.spool import Spool

logger = logging.getLogger(__name__)

# One Spool instance per worker process. The poll queue runs at
# --concurrency=1 (DuckDB is single-writer), so this is never shared across
# concurrent writers; each Spool method still opens/closes its own
# connection (see spool.py) so no lock is held across a task boundary.
_spool: Spool | None = None


def _get_spool(*, must_exist: bool = False) -> Spool:
    """Process-wide spool handle.

    ``must_exist=True`` is used by the flush path: an absent database file
    there means the task is running on a worker that does not mount the spool
    volume, where DuckDB would silently create an empty one and the flush
    would report success having written nothing.
    """
    global _spool
    if _spool is None:
        _spool = Spool(getattr(settings, "SPOOL_PATH", None), must_exist=must_exist)
    return _spool


_STATUS_NAME = "mbta"


def _status_dir() -> str | None:
    return getattr(settings, "STATUS_DIR", None)

def _sha256_hex(b: bytes) -> str: return hashlib.sha256(b).hexdigest()
def _to_ts(sec): return datetime.fromtimestamp(sec, tz=timezone.utc) if sec else None
def _now(): return datetime.now(timezone.utc)


def _write_vp_records_to_s3(records):
    """Write VP records to the S3 Hive-Parquet store. Returns rows written."""
    if not records:
        return 0
    import pandas as pd
    from rt_pipeline.storage import write_vehicle_positions

    base_uri = getattr(settings, "S3_VP_BASE_URI", "") or None
    return write_vehicle_positions(pd.DataFrame.from_records(records), base_uri)


def _maybe_sink_vp_to_s3(records):
    """Best-effort dual-write of parsed VP records to S3 (used by the legacy
    Postgres path). Env-gated (S3_VP_SINK_ENABLED); swallows errors so it can
    never break Postgres ingestion."""
    if not records or not getattr(settings, "S3_VP_SINK_ENABLED", False):
        return 0
    try:
        return _write_vp_records_to_s3(records)
    except Exception as exc:  # never break ingestion on a sink error
        logger.warning("S3 VP sink failed: %s", exc)
        return 0


def _vp_entity_to_record(ent, ingested):
    """Map a GTFS-RT vehicle entity to the flat storage record dict."""
    v = ent.vehicle
    return {
        "feed_name": settings.FEED_NAME,
        "vehicle_id": (v.vehicle.id or v.vehicle.label or ent.id or "unknown").strip(),
        "ts": _to_ts(v.timestamp) or _now(),
        "lat": v.position.latitude if v.HasField("position") else None,
        "lon": v.position.longitude if v.HasField("position") else None,
        "bearing": v.position.bearing if v.HasField("position") else None,
        "speed": v.position.speed if v.HasField("position") else None,
        "route_id": v.trip.route_id if v.HasField("trip") else None,
        "trip_id": v.trip.trip_id if v.HasField("trip") else None,
        "current_stop_sequence": v.current_stop_sequence
        if v.HasField("current_stop_sequence") else None,
        # VehicleStopStatus enum name (INCOMING_AT/STOPPED_AT/IN_TRANSIT_TO) and
        # the stop it refers to — needed for STOPPED_AT-based arrival + is_at_stop.
        "current_status": (
            gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(v.current_status)
            if v.HasField("current_status") else None
        ),
        "stop_id": v.stop_id if v.HasField("stop_id") else None,
        "ingested_at": ingested,
    }


# Vehicle Positions -> local spool only (no Postgres, no S3). This is the
# active scheduled task.
@shared_task(bind=True, queue="fetch",
             autoretry_for=(requests.RequestException,), retry_backoff=True,
             retry_kwargs={"max_retries": 5})
def poll_vehicle_positions_s3(self):
    """Fetch the VehiclePositions feed and append it to the local DuckDB spool.

    Does NOT touch Postgres (no RawMessage, no VehiclePosition rows) and does
    NOT touch S3 -- that used to happen here directly via a per-poll
    ``COPY ... PARTITION_BY (..., route_id)``, which fanned out into ~160 S3
    PUTs per poll (one per active MBTA route) and drove poll latency past
    300s against a 5s beat schedule. S3 is now only written by the hourly
    ``flush_vp_spool_s3`` task. This remains the only task scheduled by beat
    for MBTA VP polling; the legacy Postgres fetch/parse tasks below are kept
    for reference but are not scheduled.
    """
    fetch_ok = False
    try:
        url = settings.GTFSRT_VEHICLE_POSITIONS_URL
        r = requests.get(
            url, timeout=(settings.HTTP_CONNECT_TIMEOUT, settings.HTTP_READ_TIMEOUT)
        )
        r.raise_for_status()
        fetch_ok = True
        ingested = _now()

        if not r.content:
            status.update(
                _STATUS_NAME, _status_dir(),
                last_poll_utc=ingested.isoformat(),
                fetch_ok=True, fetch_fail=False,
                rows_fetched=0, rows_new=0,
            )
            return {"skipped": True}

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(r.content)
        records = [
            _vp_entity_to_record(ent, ingested)
            for ent in feed.entity if ent.HasField("vehicle")
        ]
        df = pd.DataFrame.from_records(records) if records else pd.DataFrame()
        sp = _get_spool()
        spooled = sp.append(df)
        spool_stats = sp.stats()

        status.update(
            _STATUS_NAME, _status_dir(),
            last_poll_utc=ingested.isoformat(),
            fetch_ok=True, fetch_fail=False,
            rows_fetched=len(records), rows_new=spooled,
            spool_rows=spool_stats["rows"], spool_bytes=spool_stats["bytes"],
        )
        return {"fetched": len(records), "spooled": spooled}
    except Exception as exc:
        status.update(
            _STATUS_NAME, _status_dir(), event=True,
            fetch_ok=fetch_ok, fetch_fail=not fetch_ok,
            last_error=str(exc), last_error_utc=_now().isoformat(),
        )
        raise

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
    s3_records = []
    ingested = _now()
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
        s3_records.append({
            "feed_name": settings.FEED_NAME, "vehicle_id": vh_id, "ts": ts,
            "lat": lat, "lon": lon, "bearing": bearing, "speed": speed,
            "route_id": route_id, "trip_id": trip_id,
            "current_stop_sequence": css, "ingested_at": ingested,
        })

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

    sunk = _maybe_sink_vp_to_s3(s3_records)
    return {"inserted": inserted, "s3_rows": sunk}

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


# ---- Spool -> S3 staging flush ----

# queue="fetch" is load-bearing and must match the routing note in
# ingestproj/celery.py. A `queue=` on the decorator OVERRIDES task_routes, so
# setting the route there alone is not enough. Two reasons this must be the
# poll worker: DuckDB permits one read-write process per file, and only the
# poll worker mounts the spool volume — on the maint worker this task opened a
# brand-new empty database inside that container and reported a cheerful
# {'flushed': 0, 'days': []} while the real spool grew unbounded.
@shared_task(bind=True, queue="fetch")
def flush_vp_spool_s3(self) -> dict[str, Any]:
    """Hourly: drain spooled rows older than the current UTC hour to S3 staging.

    The write-then-verify-then-delete logic (and the reason ``route_id`` is
    NOT part of the partitioning here) lives in
    ``rt_pipeline.storage.spool.flush_to_staging``, which is Django-free and
    unit-tested directly against a local temp directory. This task is a thin
    wrapper: compute the UTC-hour cutoff, call it, and report status.
    """
    from .storage.spool import flush_to_staging

    now = _now()
    cutoff = now.replace(minute=0, second=0, microsecond=0)
    sp = _get_spool(must_exist=True)
    base_uri = settings.S3_VP_STAGING_BASE_URI

    try:
        result = flush_to_staging(sp, base_uri, cutoff)
    except Exception as exc:
        logger.exception("flush_vp_spool_s3: write/verify failed; spool left intact")
        status.update(
            _STATUS_NAME, _status_dir(), event=True,
            last_error=str(exc), last_error_utc=_now().isoformat(),
        )
        return {"flushed": 0, "error": str(exc)}

    spool_stats = sp.stats()
    status.update(
        _STATUS_NAME, _status_dir(), event=True,
        last_flush_utc=now.isoformat(),
        last_flush_key=",".join(result["days"]),
        last_flush_rows=result["flushed"],
        spool_rows=spool_stats["rows"], spool_bytes=spool_stats["bytes"],
    )
    return result


# ---- Daily curated compaction ----

@shared_task(bind=True, queue="maint")
def compact_vp_day(self) -> dict[str, Any]:
    """Daily: compact closed-day staging objects into the curated dataset.

    Delegates to ``rt_pipeline.compaction`` (a separate work item owned by
    another agent). Imported lazily inside the task body so beat/worker
    startup never fails if that module isn't present yet or is mid-change --
    a missing compaction module should degrade to "compaction skipped today",
    never "worker won't boot".
    """
    try:
        # rt_pipeline.compaction.run_compaction(feeds=None, dry_run=False,
        # since=None, until=None, ...) -> dict. Signature confirmed against
        # rt_pipeline/compaction/__init__.py, which documents this task as
        # its intended caller. Imported lazily anyway so a future change to
        # that module can never prevent beat/worker startup.
        from rt_pipeline.compaction import run_compaction
    except ImportError as exc:
        logger.error("compact_vp_day: rt_pipeline.compaction unavailable: %s", exc)
        return {"error": "compaction module unavailable"}

    try:
        result = run_compaction(feeds=None, dry_run=False)
    except Exception as exc:
        logger.exception("compact_vp_day: compaction run failed")
        status.update(
            _STATUS_NAME, _status_dir(), event=True,
            last_error=f"compaction failed: {exc}",
            last_error_utc=_now().isoformat(),
        )
        return {"error": "compaction run failed"}

    status.update(
        _STATUS_NAME, _status_dir(), event=True,
        last_compaction_utc=_now().isoformat(),
        last_compaction_day=result.get("today_utc"),
        last_compaction_files=result.get("compacted"),
        last_compaction_rows_in=result.get("rows_in"),
        last_compaction_rows_out=result.get("rows_out"),
        last_compaction_dupes_removed=result.get("dupes_removed"),
        last_compaction_errors=result.get("errors"),
    )
    return result


# ---- Celery Beat schedule ----
from celery.schedules import crontab, schedule
from ingestproj.celery import app as celery_app

# VehiclePositions polling, the hourly spool->staging flush, and the daily
# staging->curated compaction are the only scheduled tasks. The Postgres VP
# fetch/parse and the TripUpdate tasks above remain defined but are
# intentionally NOT scheduled. Re-add entries here to revive.
celery_app.conf.beat_schedule = {
    "poll-vehicle-positions-s3": {
        "task": "rt_pipeline.tasks.poll_vehicle_positions_s3",
        "schedule": schedule(run_every=settings.POLL_SECONDS),
        # The single most important line in this schedule: a poll that
        # missed its slot is DROPPED rather than queued, so a backlog can
        # never accumulate again (this is what let the queue grow without
        # bound during the OOM incident).
        "options": {"expires": settings.POLL_SECONDS},
    },
    "flush-vp-spool": {
        "task": "rt_pipeline.tasks.flush_vp_spool_s3",
        "schedule": crontab(minute=settings.SPOOL_FLUSH_MINUTE),
    },
    "compact-vp-day": {
        "task": "rt_pipeline.tasks.compact_vp_day",
        "schedule": crontab(hour=settings.COMPACT_HOUR_UTC, minute=settings.COMPACT_MINUTE),
    },
}