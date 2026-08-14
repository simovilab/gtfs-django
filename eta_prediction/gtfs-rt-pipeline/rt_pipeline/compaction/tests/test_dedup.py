"""Dedup correctness: exact counts, and the tie-break keeps the latest recency.

This is the headline fix over the adopted script, which did `SELECT *` with
no dedup at all -- the MBTA data has measured 1.85x duplication that survived
the 2026-08-01 compaction untouched.
"""

from __future__ import annotations

import datetime as dt

from .factories import mbta_rows, write_parquet


def test_dedup_removes_exact_duplicates(tmp_path, store, duck):
    ts = dt.datetime(2026, 7, 8, 12, 0, 0)
    leaf = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=39"

    # file 1: three vehicles
    write_parquet(
        tmp_path,
        f"{leaf}/a.parquet",
        mbta_rows(["v1", "v2", "v3"], ts, dt.datetime(2026, 7, 8, 12, 0, 1)),
    )
    # file 2: the same three vehicles again (exact duplicate key), plus one new one
    write_parquet(
        tmp_path,
        f"{leaf}/b.parquet",
        mbta_rows(["v1", "v2", "v3", "v4"], ts, dt.datetime(2026, 7, 8, 12, 0, 2)),
    )

    counts = duck.compact_leaf(
        [(store.uri(f"{leaf}/*.parquet"), False)],
        store.uri(f"{leaf}/staged.parquet"),
        sort_col="ts",
        dedup_keys=("feed_name", "vehicle_id", "ts"),
        recency_col="ingested_at",
    )

    assert counts.src == 7
    assert counts.dst == 4
    assert counts.dupes_removed == 3

    out = duck.con.execute(
        f"SELECT * FROM read_parquet('{store.uri(f'{leaf}/staged.parquet')}')"
    ).df()
    assert len(out) == 4
    assert sorted(out["vehicle_id"]) == ["v1", "v2", "v3", "v4"]


def test_dedup_keeps_latest_recency_not_arbitrary_row(tmp_path, store, duck):
    ts = dt.datetime(2026, 7, 8, 12, 0, 0)
    leaf = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=39"

    stale = mbta_rows(["v1"], ts, dt.datetime(2026, 7, 8, 12, 0, 1))
    stale["lat"] = 10.0  # marker
    fresh = mbta_rows(["v1"], ts, dt.datetime(2026, 7, 8, 12, 5, 0))
    fresh["lat"] = 99.0  # marker

    # written out of chronological order, to prove file order isn't what decides it
    write_parquet(tmp_path, f"{leaf}/b_written_first.parquet", fresh)
    write_parquet(tmp_path, f"{leaf}/a_written_second.parquet", stale)

    counts = duck.compact_leaf(
        [(store.uri(f"{leaf}/*.parquet"), False)],
        store.uri(f"{leaf}/staged.parquet"),
        sort_col="ts",
        dedup_keys=("feed_name", "vehicle_id", "ts"),
        recency_col="ingested_at",
    )
    assert counts.dst == 1

    out = duck.con.execute(
        f"SELECT * FROM read_parquet('{store.uri(f'{leaf}/staged.parquet')}')"
    ).df()
    assert len(out) == 1
    assert out.iloc[0]["lat"] == 99.0
    assert out.iloc[0]["ingested_at"] == dt.datetime(2026, 7, 8, 12, 5, 0)
