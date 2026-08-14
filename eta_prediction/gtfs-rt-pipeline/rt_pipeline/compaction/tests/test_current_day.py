"""The current UTC day is still being written to by the collectors and must
never be discovered as a unit of work, for either compaction path."""

from __future__ import annotations

import datetime as dt

from rt_pipeline.compaction.feeds import FEEDS_BY_NAME
from rt_pipeline.compaction.planning import discover_leaves, discover_staging_days

from .factories import mbta_rows, write_parquet


def test_current_day_leaf_is_never_discovered(tmp_path, store):
    today = dt.date(2026, 7, 9)
    leaf_key = f"feeds/mbta/vehicle_positions/year={today.year}/month={today.month}/day={today.day}/route_id=39"
    write_parquet(
        tmp_path,
        f"{leaf_key}/a.parquet",
        mbta_rows(["v1"], dt.datetime.combine(today, dt.time(0, 0)), dt.datetime.combine(today, dt.time(0, 1))),
    )
    yesterday_key = f"feeds/mbta/vehicle_positions/year={today.year}/month={today.month}/day={today.day - 1}/route_id=39"
    write_parquet(
        tmp_path,
        f"{yesterday_key}/a.parquet",
        mbta_rows(["v1"], dt.datetime.combine(today, dt.time(0, 0)), dt.datetime.combine(today, dt.time(0, 1))),
    )

    feed = FEEDS_BY_NAME["mbta_vp"]
    leaves = discover_leaves(store, feed, today)

    assert len(leaves) == 1
    assert leaves[0].date == today - dt.timedelta(days=1)


def test_current_day_staging_is_never_discovered(tmp_path, store):
    today = dt.date(2026, 7, 9)
    staging_today = (
        f"feeds/mbta/vehicle_positions_staging/year={today.year}/month={today.month}/day={today.day}"
    )
    write_parquet(
        tmp_path,
        f"{staging_today}/hour.parquet",
        mbta_rows(["v1"], dt.datetime.combine(today, dt.time(0, 0)), dt.datetime.combine(today, dt.time(0, 1)), include_route_id=True),
    )

    feed = FEEDS_BY_NAME["mbta_vp"]
    days = discover_staging_days(store, feed, today)

    assert days == []
