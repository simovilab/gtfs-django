
# Create a base module for testing fake stop times in GTFS Django utilities.
# Other person is in charge of implementing the actual logic.

def fake_stop_times(journey=None, progression=None):
    """
    Stub temporal de fake_stop_times.
    Devuelve una lista determinística de stops para pruebas.
    """
    return [
        {"stop_id": "STOP_A", "arrival": {"time": 1730000000}},
        {"stop_id": "STOP_B", "arrival": {"time": 1730000600}},
        {"stop_id": "STOP_C", "arrival": {"time": 1730001200}},
    ]

