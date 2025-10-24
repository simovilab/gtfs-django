# run_gtfs_flow.py
from datetime import timedelta
from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink

from gtfs_rt_source import GTFSRTSource

FEED_URL = "https://cdn.mbta.com/realtime/VehiclePositions.pb"  # example; replace as needed

flow = Dataflow("gtfs_rt_poll_demo")
stream = op.input(
    "gtfs_rt_source",
    flow,
    GTFSRTSource(
        url=FEED_URL,
        poll_every=timedelta(seconds=15),
        headers={"User-Agent": "SIMOVI-ETA/0.1 +contact@example.org"},
        max_batches=10,   # demo: poll 6 times then exit
    ),
)

# you can branch by record_type, etc.
# Example: just print a few fields human-readable
def pretty(x):
    kind = x.get("record_type")
    if kind == "trip_update":
        return f"[TU] trip={x.get('trip_id')} stop={x.get('stop_id')} arr={x.get('arrival_time')} dep={x.get('departure_time')}"
    if kind == "vehicle_position":
        return f"[VP] veh={x.get('vehicle_id')} lat={x.get('lat')} lon={x.get('lon')} spd={x.get('speed')}"
    return str(x)

op.output("stdout", op.map("pretty", stream, pretty), StdOutSink())
