"""Tests for the S3-backed RT source adapter (Phase C1).

Seeds a local Hive-partitioned dataset via the storage writer, then verifies
the adapter returns the legacy ORM column shape with route pruning and
null-row filtering.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from feature_engineering.rt_source import LEGACY_COLUMNS, fetch_vehicle_positions
from rt_pipeline.storage import write_vehicle_positions


def _sample(route_id: str, n: int, vp: str = "v", with_null_trip: bool = False):
    t0 = dt.datetime(2025, 1, 2, 8, 0, tzinfo=dt.timezone.utc)
    df = pd.DataFrame(
        {
            "feed_name": ["mbta"] * n,
            "vehicle_id": [f"{vp}{i}" for i in range(n)],
            "ts": [t0 + dt.timedelta(seconds=15 * i) for i in range(n)],
            "lat": [42.0 + 0.001 * i for i in range(n)],
            "lon": [-71.0 - 0.001 * i for i in range(n)],
            "bearing": [90.0] * n,
            "speed": [10.0] * n,
            "route_id": [route_id] * n,
            "trip_id": [f"t{i}" for i in range(n)],
            "current_stop_sequence": list(range(n)),
            "ingested_at": [t0] * n,
        }
    )
    if with_null_trip:
        df.loc[0, "trip_id"] = None
    return df


def test_returns_legacy_schema_sorted(tmp_path):
    base = str(tmp_path / "vp")
    write_vehicle_positions(_sample("Green-D", 4), base)
    out = fetch_vehicle_positions(base_uri=base)
    assert list(out.columns) == LEGACY_COLUMNS
    assert out["ts"].is_monotonic_increasing  # single trip ordering proxy
    assert len(out) == 4


def test_route_pruning(tmp_path):
    base = str(tmp_path / "vp")
    write_vehicle_positions(_sample("Green-D", 4), base)
    write_vehicle_positions(_sample("Green-E", 2, vp="w"), base)
    out = fetch_vehicle_positions(route_ids=["Green-D"], base_uri=base)
    assert len(out) == 4
    assert "route_id" not in out.columns  # legacy shape drops partition col


def test_drops_null_trip_id(tmp_path):
    base = str(tmp_path / "vp")
    write_vehicle_positions(_sample("Green-D", 3, with_null_trip=True), base)
    out = fetch_vehicle_positions(base_uri=base)
    assert len(out) == 2
    assert out["trip_id"].notna().all()


def test_empty_dataset(tmp_path):
    out = fetch_vehicle_positions(base_uri=str(tmp_path / "none"))
    assert out.empty
    assert list(out.columns) == LEGACY_COLUMNS
