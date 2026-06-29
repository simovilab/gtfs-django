"""Unit tests for the dataset builder's pure helpers.

Covers the new STOPPED_AT arrival detection and the bearing/schedule helpers.
The full build path needs the Django ORM (Trip/Stop/StopTime) + an S3 store and
is exercised end-to-end via `manage.py build_eta_sample`; here we pin the
isolated logic.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from feature_engineering.dataset_builder import (
    _angle_diff,
    _initial_bearing,
    _seconds_of_day,
    find_stopped_at_arrival,
)

_T0 = dt.datetime(2025, 1, 2, 8, 0, tzinfo=dt.timezone.utc)


def _future(status, stop_id, seq):
    n = len(status)
    return pd.DataFrame(
        {
            "ts": [_T0 + dt.timedelta(seconds=30 * i) for i in range(n)],
            "current_status": status,
            "stop_id": stop_id,
            "current_stop_sequence": seq,
        }
    )


def test_stopped_at_matches_by_stop_id():
    df = _future(
        ["IN_TRANSIT_TO", "STOPPED_AT", "STOPPED_AT"],
        ["A", "B", "B"],
        [1, 2, 2],
    )
    assert find_stopped_at_arrival(df, "B", 2) == df["ts"].iloc[1]


def test_stopped_at_falls_back_to_sequence():
    # stop_id never matches target -> fall back to current_stop_sequence.
    df = _future(["IN_TRANSIT_TO", "STOPPED_AT"], ["X", "X"], [4, 5])
    assert find_stopped_at_arrival(df, "B", 5) == df["ts"].iloc[1]


def test_stopped_at_none_when_status_missing():
    df = pd.DataFrame({"ts": [_T0], "stop_id": ["B"], "current_stop_sequence": [2]})
    assert find_stopped_at_arrival(df, "B", 2) is None


def test_stopped_at_none_when_never_stopped():
    df = _future(["IN_TRANSIT_TO", "INCOMING_AT"], ["B", "B"], [2, 2])
    assert find_stopped_at_arrival(df, "B", 2) is None


def test_initial_bearing_cardinals():
    assert _initial_bearing(0.0, 0.0, 1.0, 0.0) == pytest.approx(0.0, abs=1e-6)   # N
    assert _initial_bearing(0.0, 0.0, 0.0, 1.0) == pytest.approx(90.0, abs=1e-6)  # E


def test_angle_diff_wraps_and_handles_none():
    assert _angle_diff(350, 10) == pytest.approx(20.0)
    assert _angle_diff(10, 350) == pytest.approx(20.0)
    assert _angle_diff(None, 10) is None


def test_seconds_of_day():
    assert _seconds_of_day(dt.time(1, 2, 3)) == 3723
    assert _seconds_of_day(None) is None
