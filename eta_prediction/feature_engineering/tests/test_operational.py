# feature_engineering/tests/test_operational.py
from datetime import datetime, timezone, timedelta
import math
import types

import pytest

# Import the functions under test
from feature_engineering.operational import (
    calculate_headway,
    detect_congestion_proxy,
)

# ---------- Test Doubles (fake psycopg connection/cursor) ----------

class FakeCursor:
    """
    Minimal cursor that returns pre-seeded rows for each execute() call.
    - seed should be a list. Each execute() pop(0) will set what fetch* returns.
    - Each seed item can be:
        * {"one": {...}} -> fetchone() returns dict, fetchall() returns [dict]
        * {"many": [ {...}, {...} ]} -> fetchone() returns first dict, fetchall() returns list
        * {"none": True} -> fetchone() returns None, fetchall() returns []
    """
    def __init__(self, seed):
        self.seed = list(seed)
        self.last_sql = None
        self.last_params = None
        self.closed = False

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params
        # Pop the next response descriptor
        if not self.seed:
            raise AssertionError("FakeCursor called more times than seeded responses.")
        self.current = self.seed.pop(0)

    def fetchone(self):
        if "one" in self.current:
            return self.current["one"]
        if "many" in self.current:
            return self.current["many"][0] if self.current["many"] else None
        if "none" in self.current:
            return None
        raise AssertionError("Malformed seed entry for fetchone.")

    def fetchall(self):
        if "one" in self.current:
            return [self.current["one"]]
        if "many" in self.current:
            return self.current["many"]
        if "none" in self.current:
            return []
        raise AssertionError("Malformed seed entry for fetchall.")

    # Context manager API
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.closed = True


class FakeConn:
    """
    Fake psycopg.Connection that returns a FakeCursor with provided seed per cursor().
    """
    def __init__(self, seed):
        self.seed = seed

    def cursor(self, row_factory=None):
        # row_factory ignored here; we return dict rows directly
        return FakeCursor(self.seed)


# ------------------------------ Tests ------------------------------

def test_calculate_headway_basic():
    """
    Headway is computed as now - MAX(previous ts). The SQL returns headway_s directly.
    """
    # Simulate the single query returning headway_s = 120.0
    seed = [
        {"one": {"headway_s": 120.0}},
    ]
    conn = FakeConn(seed)
    now = datetime(2025, 10, 22, 1, 0, 0, tzinfo=timezone.utc)

    val = calculate_headway(conn, route_id="1", timestamp=now, vehicle_id="v42")
    assert isinstance(val, float)
    assert val == 120.0


def test_calculate_headway_none_returns_inf():
    """
    When no previous VP exists, the SQL returns NULL -> function returns inf.
    """
    seed = [
        {"one": {"headway_s": None}},
    ]
    conn = FakeConn(seed)
    now = datetime(2025, 10, 22, 1, 0, 0, tzinfo=timezone.utc)

    val = calculate_headway(conn, route_id="1", timestamp=now, vehicle_id="v42")
    assert math.isinf(val)


def test_detect_congestion_proxy_increasing_delay():
    """
    Three queries inside detect_congestion_proxy:
      1) avg speed (m/s) -> km/h
      2) distinct vehicles count
      3) delay rows for slope (increasing)
    """
    # 1) avg_speed_mps -> 5 m/s => 18.0 km/h
    # 2) vehicles_on_route -> 3
    # 3) delay samples with positive slope (seconds per minute)
    t0 = datetime(2025, 10, 22, 1, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=10)
    t2 = t0 + timedelta(minutes=20)

    seed = [
        {"one": {"avg_speed_mps": 5.0}},                # sql_speed
        {"one": {"n": 3}},                               # sql_veh
        {"many": [                                       # sql_delay
            {"t_epoch": t0.timestamp(), "delay_s": 60.0},
            {"t_epoch": t1.timestamp(), "delay_s": 140.0},
            {"t_epoch": t2.timestamp(), "delay_s": 260.0},
        ]},
    ]
    conn = FakeConn(seed)

    out = detect_congestion_proxy(
        conn,
        route_id="10",
        stop_sequence=5,
        timestamp=t2,
        speed_window_minutes=10,
        vehicles_window_minutes=10,
        delay_trend_window_minutes=30,
        slope_threshold_sec_per_min=5.0,
    )
    assert out["avg_speed_last_10min"] == pytest.approx(18.0)  # 5 m/s * 3.6
    assert out["vehicles_on_route"] == 3
    assert out["delay_trend"] == "increasing"


def test_detect_congestion_proxy_decreasing_delay():
    """
    Decreasing delays should label as 'decreasing'.
    """
    t0 = datetime(2025, 10, 22, 1, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=10)
    t2 = t0 + timedelta(minutes=20)

    seed = [
        {"one": {"avg_speed_mps": 8.0}},                # ~28.8 km/h
        {"one": {"n": 1}},
        {"many": [
            {"t_epoch": t0.timestamp(), "delay_s": 300.0},
            {"t_epoch": t1.timestamp(), "delay_s": 220.0},
            {"t_epoch": t2.timestamp(), "delay_s": 150.0},
        ]},
    ]
    conn = FakeConn(seed)

    out = detect_congestion_proxy(
        conn,
        route_id="10",
        stop_sequence=7,
        timestamp=t2,
        slope_threshold_sec_per_min=5.0,
    )
    assert out["avg_speed_last_10min"] == pytest.approx(28.8)
    assert out["vehicles_on_route"] == 1
    assert out["delay_trend"] == "decreasing"


def test_detect_congestion_proxy_stable_when_not_enough_points():
    """
    With fewer than 3 samples, trend defaults to 'stable'.
    """
    t0 = datetime(2025, 10, 22, 1, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(minutes=10)

    seed = [
        {"one": {"avg_speed_mps": None}},  # no speeds
        {"one": {"n": 0}},                 # no vehicles
        {"many": [
            {"t_epoch": t0.timestamp(), "delay_s": 30.0},
            {"t_epoch": t1.timestamp(), "delay_s": 35.0},
        ]},
    ]
    conn = FakeConn(seed)

    out = detect_congestion_proxy(
        conn,
        route_id="77",
        stop_sequence=3,
        timestamp=t1,
    )
    assert out["avg_speed_last_10min"] is None
    assert out["vehicles_on_route"] == 0
    assert out["delay_trend"] == "stable"
