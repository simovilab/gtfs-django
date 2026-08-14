"""--since / --until bound which closed leaves/days are selected for a run."""

from __future__ import annotations

import datetime as dt

from rt_pipeline.compaction.feeds import FEEDS_BY_NAME
from rt_pipeline.compaction.planning import discover_leaves, discover_staging_days

from .factories import mbta_rows, write_parquet


def _write_leaf(tmp_path, day: int):
    leaf_key = f"feeds/mbta/vehicle_positions/year=2026/month=7/day={day}/route_id=39"
    write_parquet(
        tmp_path,
        f"{leaf_key}/a.parquet",
        mbta_rows(["v1"], dt.datetime(2026, 7, day, 12, 0), dt.datetime(2026, 7, day, 12, 1)),
    )


def _write_staging_day(tmp_path, day: int):
    key = f"feeds/mbta/vehicle_positions_staging/year=2026/month=7/day={day}"
    write_parquet(
        tmp_path,
        f"{key}/hour.parquet",
        mbta_rows(
            ["v1"],
            dt.datetime(2026, 7, day, 12, 0),
            dt.datetime(2026, 7, day, 12, 1),
            include_route_id=True,
        ),
    )


def test_since_until_bound_leaves(tmp_path, store):
    for day in (6, 7, 8, 9):
        _write_leaf(tmp_path, day)

    feed = FEEDS_BY_NAME["mbta_vp"]
    today = dt.date(2026, 7, 10)

    leaves = discover_leaves(store, feed, today, since=dt.date(2026, 7, 7), until=dt.date(2026, 7, 8))
    assert sorted(lf.date for lf in leaves) == [dt.date(2026, 7, 7), dt.date(2026, 7, 8)]

    only_since = discover_leaves(store, feed, today, since=dt.date(2026, 7, 8))
    assert sorted(lf.date for lf in only_since) == [dt.date(2026, 7, 8), dt.date(2026, 7, 9)]

    only_until = discover_leaves(store, feed, today, until=dt.date(2026, 7, 7))
    assert sorted(lf.date for lf in only_until) == [dt.date(2026, 7, 6), dt.date(2026, 7, 7)]


def test_since_until_bound_staging_days(tmp_path, store):
    for day in (6, 7, 8, 9):
        _write_staging_day(tmp_path, day)

    feed = FEEDS_BY_NAME["mbta_vp"]
    today = dt.date(2026, 7, 10)

    days = discover_staging_days(store, feed, today, since=dt.date(2026, 7, 7), until=dt.date(2026, 7, 8))
    assert sorted(d.date for d in days) == [dt.date(2026, 7, 7), dt.date(2026, 7, 8)]
