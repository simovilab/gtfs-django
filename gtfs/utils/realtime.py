from .stop_times import estimate_stop_times


def build_vehicle_positions():
    return "One day, vehicle_positions.pb"


def build_trip_updates():
    estimate_stop_times()
    return "One day, trip_updates.pb"


def build_alerts():
    return "One day, alerts.pb"


def get_vehicle_positions():
    return "Saved vehicle positions"


def get_trip_updates():
    return "Saved trip updates"


def get_alerts():
    return "Saved alerts"
