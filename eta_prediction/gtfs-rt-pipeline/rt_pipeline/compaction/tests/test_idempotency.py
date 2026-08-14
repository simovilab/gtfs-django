"""A leaf that already holds its <date>.parquet is skipped, not reprocessed."""

from __future__ import annotations

import datetime as dt

from rt_pipeline.compaction.execution import process_leaf
from rt_pipeline.compaction.feeds import FEEDS_BY_NAME
from rt_pipeline.compaction.planning import discover_leaves

from .factories import mbta_rows, write_parquet


def test_leaf_idempotent_on_second_run(tmp_path, store, duck):
    ts = dt.datetime(2026, 7, 8, 12, 0, 0)
    leaf_key = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=39"
    write_parquet(tmp_path, f"{leaf_key}/a.parquet", mbta_rows(["v1", "v2"], ts, ts))

    feed = FEEDS_BY_NAME["mbta_vp"]
    today = dt.date(2026, 7, 9)

    leaves = discover_leaves(store, feed, today)
    assert len(leaves) == 1

    first = process_leaf(leaves[0], store, duck, dry_run=False)
    assert first.kind == "compacted"
    assert first.rows_out == 2

    out_path = tmp_path / leaf_key / "2026-07-08.parquet"
    assert out_path.is_file()
    content_first = out_path.read_bytes()

    # rediscover from scratch, exactly like a second cron invocation would
    leaves_again = discover_leaves(store, feed, today)
    assert len(leaves_again) == 1
    second = process_leaf(leaves_again[0], store, duck, dry_run=False)

    assert second.kind == "skipped"
    assert out_path.read_bytes() == content_first  # untouched
