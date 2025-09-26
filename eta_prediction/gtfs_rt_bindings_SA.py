from google.transit import gtfs_realtime_pb2
import requests
from datetime import datetime, timezone

"""
This script takes a GTFS-RT Service Alerts feed and prints out the first N alerts.
"""
N = 10
URL = "https://cdn.mbta.com/realtime/Alerts.pb" # MBTA Service Alerts feed

def ts_to_iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None

def get_text(translated_string):
    """Return the first non-empty translation (common case)."""
    if not translated_string or not translated_string.translation:
        return None
    for t in translated_string.translation:
        if t.text:
            return t.text
    return None

def fmt_period(active_period):
    start = ts_to_iso(active_period.start) if active_period.HasField("start") else None
    end = ts_to_iso(active_period.end) if active_period.HasField("end") else None
    return f"{start or '—'} → {end or '—'}"

def describe_informed_entity(ie):
    parts = []
    if ie.route_id:
        parts.append(f"route={ie.route_id}")
    if ie.stop_id:
        parts.append(f"stop={ie.stop_id}")
    if ie.trip and ie.trip.trip_id:
        parts.append(f"trip={ie.trip.trip_id}")
    if not parts:
        parts.append("system-wide")
    return ", ".join(parts)

feed = gtfs_realtime_pb2.FeedMessage()
resp = requests.get(URL, timeout=15)
resp.raise_for_status()
feed.ParseFromString(resp.content)

shown = 0
for entity in feed.entity:
    if not entity.HasField("alert"):
        continue
    alert = entity.alert

    header = get_text(alert.header_text) or "(no header)"
    desc = get_text(alert.description_text) or ""
    cause = gtfs_realtime_pb2.Alert.Cause.Name(alert.cause) if alert.HasField("cause") else "CAUSE_UNSPECIFIED"
    effect = gtfs_realtime_pb2.Alert.Effect.Name(alert.effect) if alert.HasField("effect") else "EFFECT_UNSPECIFIED"

    periods = [fmt_period(p) for p in alert.active_period]
    applies = [describe_informed_entity(ie) for ie in alert.informed_entity]

    print(f"ALERT: {header}")
    if desc:
        print(f"  desc: {desc}")
    print(f"  cause: {cause}  |  effect: {effect}")
    if periods:
        for i, p in enumerate(periods, 1):
            print(f"  active[{i}]: {p}")
    if applies:
        for i, a in enumerate(applies, 1):
            print(f"  applies_to[{i}]: {a}")
    print("-" * 80)

    count += 1
    if count >= N:  # show first N alerts
        break
