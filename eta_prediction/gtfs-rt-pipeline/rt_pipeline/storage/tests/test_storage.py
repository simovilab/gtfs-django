"""Round-trip tests for the S3 storage layer, run against a local tmpdir.

DuckDB reads/writes local Hive-partitioned Parquet the same way it does for
``s3://``, so these validate the real read/write/prune behaviour without any
network or MinIO dependency.
"""

from __future__ import annotations

import datetime as dt
import os

import pandas as pd
import pytest

from rt_pipeline.storage import (
    available_routes,
    list_partitions,
    read_vehicle_positions,
    write_vehicle_positions,
)
from rt_pipeline.storage.schema import add_partition_columns, missing_columns


def _sample(route_id: str, day: int, n: int, vehicle_prefix: str = "v") -> pd.DataFrame:
    base = dt.datetime(2025, 1, day, 8, 0, 0, tzinfo=dt.timezone.utc)
    return pd.DataFrame(
        {
            "feed_name": ["mbta"] * n,
            "vehicle_id": [f"{vehicle_prefix}{i}" for i in range(n)],
            "ts": [base + dt.timedelta(seconds=15 * i) for i in range(n)],
            "lat": [42.0 + 0.001 * i for i in range(n)],
            "lon": [-71.0 - 0.001 * i for i in range(n)],
            "bearing": [90.0] * n,
            "speed": [10.0] * n,
            "route_id": [route_id] * n,
            "trip_id": [f"trip-{i}" for i in range(n)],
            "current_stop_sequence": list(range(n)),
            "ingested_at": [base] * n,
        }
    )


def test_add_partition_columns_is_immutable():
    df = _sample("Green-D", 2, 3)
    out = add_partition_columns(df)
    assert "year" not in df.columns  # input untouched
    assert list(out[["year", "month", "day"]].iloc[0]) == [2025, 1, 2]


def test_missing_columns_detected():
    df = _sample("Green-D", 2, 2).drop(columns=["lat"])
    assert "lat" in missing_columns(df)
    with pytest.raises(ValueError):
        write_vehicle_positions(df, "ignored")


def test_write_creates_hive_layout(tmp_path):
    base = str(tmp_path / "vp")
    written = write_vehicle_positions(_sample("Green-D", 2, 5), base)
    assert written == 5
    expected = tmp_path / "vp" / "year=2025" / "month=1" / "day=2" / "route_id=Green-D"
    assert expected.is_dir()
    assert any(p.suffix == ".parquet" for p in expected.iterdir())


def test_round_trip_and_route_pruning(tmp_path):
    base = str(tmp_path / "vp")
    write_vehicle_positions(_sample("Green-D", 2, 5), base)
    write_vehicle_positions(_sample("Green-E", 2, 3, vehicle_prefix="w"), base)

    all_rows = read_vehicle_positions(base_uri=base)
    assert len(all_rows) == 8
    # partition keys reconstructed on read
    assert set(all_rows["route_id"]) == {"Green-D", "Green-E"}

    only_d = read_vehicle_positions(route_ids=["Green-D"], base_uri=base)
    assert len(only_d) == 5
    assert set(only_d["route_id"]) == {"Green-D"}
    # data columns survive the round trip
    assert {"feed_name", "vehicle_id", "ts", "lat", "trip_id"} <= set(only_d.columns)


def test_append_into_existing_partition(tmp_path):
    base = str(tmp_path / "vp")
    write_vehicle_positions(_sample("Green-D", 2, 2), base)
    write_vehicle_positions(_sample("Green-D", 2, 2, vehicle_prefix="x"), base)
    rows = read_vehicle_positions(route_ids=["Green-D"], base_uri=base)
    assert len(rows) == 4  # second batch appended, not overwritten


def test_date_range_filter(tmp_path):
    base = str(tmp_path / "vp")
    write_vehicle_positions(_sample("Green-D", 2, 4), base)  # Jan 2
    write_vehicle_positions(_sample("Green-D", 5, 6), base)  # Jan 5
    rows = read_vehicle_positions(
        start=dt.datetime(2025, 1, 4, tzinfo=dt.timezone.utc),
        end=dt.datetime(2025, 1, 6, tzinfo=dt.timezone.utc),
        base_uri=base,
    )
    assert len(rows) == 6
    assert set(rows["day"]) == {5}


def test_list_partitions_and_routes(tmp_path):
    base = str(tmp_path / "vp")
    write_vehicle_positions(_sample("Green-D", 2, 5), base)
    write_vehicle_positions(_sample("Green-E", 3, 3), base)
    parts = list_partitions(base_uri=base)
    assert len(parts) == 2
    assert set(parts["route_id"]) == {"Green-D", "Green-E"}
    assert int(parts.loc[parts["route_id"] == "Green-D", "rows"].iloc[0]) == 5
    assert available_routes(base_uri=base) == ["Green-D", "Green-E"]


def test_read_dedups_concurrent_writer_duplicates(tmp_path):
    base = str(tmp_path / "vp")
    # Two pollers capture the same feed snapshot -> identical (vehicle_id, ts).
    write_vehicle_positions(_sample("Green-D", 2, 4), base)
    write_vehicle_positions(_sample("Green-D", 2, 4), base)

    deduped = read_vehicle_positions(route_ids=["Green-D"], base_uri=base)
    assert len(deduped) == 4
    assert deduped.duplicated(subset=["vehicle_id", "ts"]).sum() == 0

    raw = read_vehicle_positions(route_ids=["Green-D"], base_uri=base, dedup=False)
    assert len(raw) == 8


def test_empty_df_writes_nothing(tmp_path):
    base = str(tmp_path / "vp")
    assert write_vehicle_positions(pd.DataFrame(), base) == 0


def test_read_missing_dataset_returns_empty(tmp_path):
    rows = read_vehicle_positions(base_uri=str(tmp_path / "nope"))
    assert rows.empty


def _import_vp_cache():
    """feature_engineering is a sibling of gtfs-rt-pipeline; make it importable."""
    import pathlib
    import sys

    eta_root = pathlib.Path(__file__).resolve().parents[4]  # eta_prediction/
    if str(eta_root) not in sys.path:
        sys.path.insert(0, str(eta_root))
    from feature_engineering.vp_cache import materialize_local_cache

    return materialize_local_cache


def test_materialize_local_cache_compacts_and_dedups(tmp_path):
    materialize_local_cache = _import_vp_cache()

    src = str(tmp_path / "src")  # stands in for the S3 store
    write_vehicle_positions(_sample("Green-D", 2, 4), src)
    write_vehicle_positions(_sample("Green-D", 2, 4), src)  # duplicate poll
    write_vehicle_positions(_sample("Green-E", 3, 3, vehicle_prefix="w"), src)

    dest = str(tmp_path / "cache")
    rows = materialize_local_cache(src, dest)
    assert rows == 7  # Green-D 4 (dup collapsed) + Green-E 3

    # Builder reads the local cache exactly like it reads S3.
    cached = read_vehicle_positions(base_uri=dest)
    assert len(cached) == 7
    assert cached.duplicated(subset=["vehicle_id", "ts"]).sum() == 0

    # The duped partition compacted to a single file in the cache.
    part = (
        tmp_path / "cache" / "year=2025" / "month=1" / "day=2" / "route_id=Green-D"
    )
    assert len(list(part.glob("*.parquet"))) == 1


def test_materialize_local_cache_refresh_is_idempotent(tmp_path):
    materialize_local_cache = _import_vp_cache()

    src = str(tmp_path / "src")
    write_vehicle_positions(_sample("Green-D", 2, 4), src)

    dest = str(tmp_path / "cache")
    materialize_local_cache(src, dest)
    materialize_local_cache(src, dest)  # refresh=True rebuilds, no accumulation

    part = (
        tmp_path / "cache" / "year=2025" / "month=1" / "day=2" / "route_id=Green-D"
    )
    assert len(list(part.glob("*.parquet"))) == 1
    assert len(read_vehicle_positions(base_uri=dest)) == 4
