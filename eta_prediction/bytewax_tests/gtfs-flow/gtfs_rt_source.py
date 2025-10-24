from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from google.transit import gtfs_realtime_pb2 as gtfs

UTC = timezone.utc

@dataclass
class PollState:
    next_awake: datetime
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    cursor_ts: Optional[int] = None  # unix seconds of last processed entity batch

class GTFSRTPartition(StatefulSourcePartition):
    """
    A resumable, single-partition GTFS-RT poller.
    Stores HTTP cache headers and wake schedule in snapshot() for recovery.
    """

    def __init__(
        self,
        url: str,
        poll_every: timedelta,
        state: PollState,
        timeout_s: float = 10.0,
        max_batches: Optional[int] = None,  # for demos/tests
        headers: Optional[Dict[str, str]] = None,
    ):
        self.url = url
        self.poll_every = poll_every
        self._state = state
        self.timeout_s = timeout_s
        self.max_batches = max_batches
        self.headers = headers or {}
        self._emitted = 0

    # ---- Bytewax-required methods ----

    def next_awake(self) -> datetime:
        return self._state.next_awake

    def snapshot(self) -> Dict[str, Any]:
        return {
            "next_awake": self._state.next_awake.isoformat(),
            "etag": self._state.etag,
            "last_modified": self._state.last_modified,
            "cursor_ts": self._state.cursor_ts,
        }

    def next_batch(self) -> List[Dict[str, Any]]:
        if self.max_batches is not None and self._emitted >= self.max_batches:
            raise StopIteration()

        # Poll now, using cache headers if present.
        req_headers = dict(self.headers)
        if self._state.etag:
            req_headers["If-None-Match"] = self._state.etag
        if self._state.last_modified:
            req_headers["If-Modified-Since"] = self._state.last_modified

        try:
            resp = requests.get(self.url, headers=req_headers, timeout=self.timeout_s)
        except requests.RequestException as e:
            # On transient error, schedule next poll and return empty batch.
            self._schedule_next_awake()
            return [{"type": "log", "level": "warning", "msg": f"poll error: {e}"}]

        # Update next awake (drift-corrected to target cadence)
        self._schedule_next_awake()

        # 304 = unchanged → return empty batch (keeps runtime sleepy & cheap)
        if resp.status_code == 304:
            return []

        if resp.status_code != 200:
            return [{"type": "log", "level": "error", "msg": f"http {resp.status_code}"}]

        # Capture cache headers for next time
        self._state.etag = resp.headers.get("ETag") or self._state.etag
        self._state.last_modified = resp.headers.get("Last-Modified") or self._state.last_modified

        # Parse protobuf
        feed = gtfs.FeedMessage()
        feed.ParseFromString(resp.content)

        # Normalize to dicts (one per entity); tag with feed timestamp if present
        feed_ts = feed.header.timestamp if feed.header.HasField("timestamp") else None
        out: List[Dict[str, Any]] = []
        count = 0
        for ent in feed.entity:
            count += 1
            if count >= 3:
                break
            if ent.HasField("trip_update"):
                out.append(normalize_trip_update(ent.id, ent.trip_update, feed_ts))
            if ent.HasField("vehicle"):
                out.append(normalize_vehicle(ent.id, ent.vehicle, feed_ts))
            # (if you later handle alerts, add normalize_alert(...))

        # Optionally filter by cursor_ts to avoid re-processing older snapshots
        if self._state.cursor_ts is not None and feed_ts is not None:
            if feed_ts <= self._state.cursor_ts:
                # stale payload; skip
                return []

        if feed_ts is not None:
            self._state.cursor_ts = feed_ts

        self._emitted += 1
        return out

    # ---- helpers ----
    def _schedule_next_awake(self) -> None:
        # Aim for fixed-rate cadence by snapping to last target + poll_every;
        # if we fell behind, schedule ASAP (now).
        target = self._state.next_awake + self.poll_every
        now = datetime.now(UTC)
        self._state.next_awake = target if target > now else now

class GTFSRTSource(FixedPartitionedSource):
    """
    Single, stable partition so Bytewax can checkpoint & resume precisely.
    """
    def __init__(
        self,
        url: str,
        poll_every: timedelta = timedelta(seconds=5),
        timeout_s: float = 10.0,
        headers: Optional[Dict[str, str]] = None,
        max_batches: Optional[int] = None,
    ):
        self.url = url
        self.poll_every = poll_every
        self.timeout_s = timeout_s
        self.headers = headers
        self.max_batches = max_batches

    def list_parts(self) -> Iterable[str]:
        return ["singleton"]

    def build_part(self, step_id: str, for_part: str, resume_state: Optional[Dict[str, Any]]):
        assert for_part == "singleton"
        rs = resume_state or {}
        now = datetime.now(UTC)
        next_awake = datetime.fromisoformat(rs.get("next_awake", now.isoformat()))
        state = PollState(
            next_awake=next_awake,
            etag=rs.get("etag"),
            last_modified=rs.get("last_modified"),
            cursor_ts=rs.get("cursor_ts"),
        )
        return GTFSRTPartition(
            url=self.url,
            poll_every=self.poll_every,
            state=state,
            timeout_s=self.timeout_s,
            headers=self.headers,
            max_batches=self.max_batches,
        )

# --------- Normalization helpers ---------

def normalize_trip_update(entity_id: str, tu: gtfs.TripUpdate, feed_ts: Optional[int]) -> Dict[str, Any]:
    base = {
        "record_type": "trip_update",
        "entity_id": entity_id,
        "feed_ts": feed_ts,
        "trip_id": tu.trip.trip_id if tu.trip.HasField("trip_id") else None,
        "route_id": tu.trip.route_id if tu.trip.HasField("route_id") else None,
        "start_time": tu.trip.start_time if tu.trip.HasField("start_time") else None,
        "start_date": tu.trip.start_date if tu.trip.HasField("start_date") else None,
        "vehicle_id": tu.vehicle.id if tu.HasField("vehicle") and tu.vehicle.HasField("id") else None,
        "schedule_relationship": tu.trip.schedule_relationship if tu.trip.HasField("schedule_relationship") else None,
    }
    out = []
    for stu in tu.stop_time_update:
        out.append({
            **base,
            "stop_sequence": stu.stop_sequence if stu.HasField("stop_sequence") else None,
            "stop_id": stu.stop_id if stu.HasField("stop_id") else None,
            "arrival_delay": stu.arrival.delay if stu.HasField("arrival") and stu.arrival.HasField("delay") else None,
            "arrival_time": stu.arrival.time if stu.HasField("arrival") and stu.arrival.HasField("time") else None,
            "departure_delay": stu.departure.delay if stu.HasField("departure") and stu.departure.HasField("delay") else None,
            "departure_time": stu.departure.time if stu.HasField("departure") and stu.departure.HasField("time") else None,
        })
    # If no stop_time_updates, still emit a row (some feeds do that)
    return out[0] if out else base

def normalize_vehicle(entity_id: str, vp: gtfs.VehiclePosition, feed_ts: Optional[int]) -> Dict[str, Any]:
    return {
        "record_type": "vehicle_position",
        "entity_id": entity_id,
        "feed_ts": feed_ts,
        "trip_id": vp.trip.trip_id if vp.HasField("trip") and vp.trip.HasField("trip_id") else None,
        "route_id": vp.trip.route_id if vp.HasField("trip") and vp.trip.HasField("route_id") else None,
        "start_time": vp.trip.start_time if vp.HasField("trip") and vp.trip.HasField("start_time") else None,
        "start_date": vp.trip.start_date if vp.HasField("trip") and vp.trip.HasField("start_date") else None,
        "vehicle_id": vp.vehicle.id if vp.HasField("vehicle") and vp.vehicle.HasField("id") else None,
        "stop_id": vp.stop_id if vp.HasField("stop_id") else None,
        "current_stop_sequence": vp.current_stop_sequence if vp.HasField("current_stop_sequence") else None,
        "current_status": vp.current_status if vp.HasField("current_status") else None,
        "timestamp": vp.timestamp if vp.HasField("timestamp") else None,
        "lat": vp.position.latitude if vp.HasField("position") and vp.position.HasField("latitude") else None,
        "lon": vp.position.longitude if vp.HasField("position") and vp.position.HasField("longitude") else None,
        "bearing": vp.position.bearing if vp.HasField("position") and vp.position.HasField("bearing") else None,
        "speed": vp.position.speed if vp.HasField("position") and vp.position.HasField("speed") else None,
        "congestion_level": vp.congestion_level if vp.HasField("congestion_level") else None,
        "occupancy_status": vp.occupancy_status if vp.HasField("occupancy_status") else None,
    }
