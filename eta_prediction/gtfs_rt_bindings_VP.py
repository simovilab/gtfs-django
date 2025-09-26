from google.transit import gtfs_realtime_pb2
import requests
from datetime import datetime, timezone

"""
This script takes a GTFS-RT Vehicle Positions feed and prints out the first N vehicle positions.
"""
N = 10
# URL = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
URL = "https://databus.bucr.digital/feed/realtime/vehicle_positions.pb" # bUCR Realtime Vehicle Positions feed

feed = gtfs_realtime_pb2.FeedMessage()
resp = requests.get(URL, timeout=15)
resp.raise_for_status()
feed.ParseFromString(resp.content)

def ts_to_iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None

count = 0
for entity in feed.entity:
    if entity.HasField("vehicle"):
        v = entity.vehicle
        pos = v.position  # lat, lon, speed (m/s), bearing (deg)
        trip = v.trip     # trip_id, route_id, start_time/date
        veh = v.vehicle   # id, label

        print(
            f"veh_id={veh.id or '—'}  route={trip.route_id or '—'}  trip={trip.trip_id or '—'}\n"
            f"  lat={pos.latitude:.6f}  lon={pos.longitude:.6f}  "
            f"speed_mps={pos.speed if pos.HasField('speed') else '—'}  "
            f"bearing={pos.bearing if pos.HasField('bearing') else '—'}\n"
            f"  current_stop_seq={v.current_stop_sequence if v.HasField('current_stop_sequence') else '—'}  "
            f"status={v.current_status if v.HasField('current_status') else '—'}  "
            f"timestamp={ts_to_iso(v.timestamp)}"
        )
        print("-" * 80)
        count += 1
        if count >= 10:  # show first N vehicle positions
            break
