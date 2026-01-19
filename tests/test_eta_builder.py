import pytest
from gtfs.utils.eta_builder import ETABuilder, DeterministicETA


@pytest.mark.django_db
def test_eta_builder_deterministic_output():
    """Ensure ETABuilder produces deterministic and increasing times."""
    stops = [("ST1", "Central"), ("ST2", "North Park"), ("ST3", "University")]

    builder = ETABuilder(seed=42)
    stop_times = builder.build_stop_times("T100", stops, start_time="08:00:00")

    # --- Assertions ---
    assert len(stop_times) == 3
    assert all("arrival_time" in s for s in stop_times)
    assert all("departure_time" in s for s in stop_times)

    # Check deterministic reproducibility
    builder2 = ETABuilder(seed=42)
    stop_times_2 = builder2.build_stop_times("T100", stops, start_time="08:00:00")
    assert stop_times == stop_times_2, "ETA output should be deterministic with same seed"

    # Check time progression
    arrival_seq = [s["arrival_time"] for s in stop_times]
    assert arrival_seq == sorted(arrival_seq), "Arrival times should be increasing"


def test_eta_builder_no_bytewax():
    """Ensure Bytewax optional import does not break builder if not installed."""
    builder = ETABuilder()
    builder.run_bytewax_demo()  # Should not raise, even if Bytewax is missing
