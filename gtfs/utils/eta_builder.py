import random
from datetime import datetime, timedelta

try:
    # Optional Bytewax integration (used by realtime ETA pipelines)
    from bytewax.dataflow import Dataflow
    from bytewax.inputs import ManualInputConfig
    from bytewax.outputs import StdOutputConfig
    BYTEWAX_AVAILABLE = True
except ImportError:
    BYTEWAX_AVAILABLE = False


class DeterministicETA:
    """
    Deterministic ETA estimator for synthetic stop_times generation.
    Produces reproducible arrival/departure times given a random seed.
    """

    def __init__(self, base_time="08:00:00", mean_delta=5, seed=42):
        self.base_time = datetime.strptime(base_time, "%H:%M:%S")
        self.mean_delta = mean_delta  # mean minutes between stops
        self.rng = random.Random(seed)

    def estimate(self, seq: int):
        """Return (arrival_time, departure_time) for a stop sequence."""
        offset = timedelta(minutes=self.mean_delta * seq + self.rng.randint(0, 2))
        arrival = self.base_time + offset
        departure = arrival + timedelta(minutes=1)
        return (
            arrival.strftime("%H:%M:%S"),
            departure.strftime("%H:%M:%S"),
        )


class ETABuilder:
    """
    ETA-based stop_times builder.

    Generates deterministic or dynamic stop_times for trips,
    optionally using Bytewax for real-time ETA streaming.
    """

    def __init__(self, eta_module=None, seed=42):
        self.eta = eta_module or DeterministicETA(seed=seed)
        self.seed = seed

    def build_stop_times(self, trip_id: str, stops: list, start_time="08:00:00"):
        """
        Build stop_times data for a single trip.

        Args:
            trip_id (str): Trip identifier.
            stops (list): List of tuples (stop_id, stop_name).
            start_time (str): Base time for deterministic generation.
        Returns:
            List[dict]: stop_times formatted for GTFS.
        """
        self.eta.base_time = datetime.strptime(start_time, "%H:%M:%S")

        stop_times = []
        for seq, (stop_id, stop_name) in enumerate(stops, start=1):
            arrival, departure = self.eta.estimate(seq)
            stop_times.append(
                {
                    "trip_id": trip_id,
                    "stop_id": stop_id,
                    "stop_sequence": seq,
                    "arrival_time": arrival,
                    "departure_time": departure,
                    "stop_headsign": stop_name,
                    "pickup_type": 0,
                    "drop_off_type": 0,
                    "timepoint": 1,
                }
            )
        return stop_times

    def run_bytewax_demo(self):
        """Optional Bytewax demonstration (prints ETA updates to stdout)."""
        if not BYTEWAX_AVAILABLE:
            print("Bytewax not installed. Skipping streaming ETA simulation.")
            return

        print("Running Bytewax ETA demo flow...")
        flow = Dataflow()
        flow.input("input", ManualInputConfig(lambda: [("trip1", i) for i in range(5)]))

        def compute_eta(item):
            trip, seq = item
            eta = self.eta.estimate(seq)
            return (trip, seq, eta)

        flow.map(compute_eta)
        flow.output("stdout", StdOutputConfig())

        # In a real system, you'd call `bytewax.run(flow)` externally
        print("Bytewax flow defined (demo only).")
