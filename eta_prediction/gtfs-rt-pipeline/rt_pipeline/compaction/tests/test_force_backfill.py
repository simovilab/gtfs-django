"""`force=True` re-dedups a leaf a pre-dedup compaction already merged.

Roadmap 0.4b: the 28 historical MBTA days were compacted on 2026-08-01
without dedup, so each leaf already holds its `<date>.parquet`. The routine
skip guard (test_idempotency.py) means those leaves are permanently skipped
without an explicit override -- this exercises that override.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from rt_pipeline.compaction.execution import process_leaf
from rt_pipeline.compaction.feeds import FEEDS_BY_NAME
from rt_pipeline.compaction.planning import discover_leaves

from .factories import write_parquet


def _duped_output(vehicle_ids: list[str], ts: dt.datetime, dupes_per_row: int) -> pd.DataFrame:
    """Simulate a pre-dedup compacted file: each vehicle's row repeated."""
    rows = []
    for v in vehicle_ids:
        for _ in range(dupes_per_row):
            rows.append(
                {
                    "feed_name": "mbta",
                    "vehicle_id": v,
                    "ts": ts,
                    "lat": 42.0,
                    "lon": -71.0,
                    "bearing": 90.0,
                    "speed": 10.0,
                    "trip_id": f"trip-{v}",
                    "current_stop_sequence": 1,
                    "current_status": "IN_TRANSIT_TO",
                    "stop_id": "stop-1",
                    "ingested_at": ts,
                }
            )
    return pd.DataFrame(rows)


def test_force_recompacts_already_compacted_leaf_and_dedups(tmp_path, store, duck):
    ts = dt.datetime(2026, 7, 8, 12, 0, 0)
    leaf_key = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=39"
    # Simulate the Aug-1 no-dedup compaction: the leaf already holds only its
    # <date>.parquet, but with each (vehicle_id, ts) key duplicated 1.85x-ish.
    duped = _duped_output(["v1", "v2"], ts, dupes_per_row=2)
    write_parquet(tmp_path, f"{leaf_key}/2026-07-08.parquet", duped)

    feed = FEEDS_BY_NAME["mbta_vp"]
    today = dt.date(2026, 7, 9)
    leaves = discover_leaves(store, feed, today)
    assert len(leaves) == 1

    # Without force: the existing skip-guard behavior is untouched.
    skipped = process_leaf(leaves[0], store, duck, dry_run=False)
    assert skipped.kind == "skipped"

    out_path = tmp_path / leaf_key / "2026-07-08.parquet"
    assert len(pd.read_parquet(out_path)) == 4  # still the duplicated content

    # With force: re-read, dedup, overwrite.
    forced = process_leaf(leaves[0], store, duck, dry_run=False, force=True)
    assert forced.kind == "compacted"
    assert forced.rows_in == 4
    assert forced.rows_out == 2
    assert forced.dupes_removed == 2

    result = pd.read_parquet(out_path)
    assert len(result) == 2
    assert sorted(result["vehicle_id"]) == ["v1", "v2"]


def test_force_is_idempotent_on_a_second_run(tmp_path, store, duck):
    ts = dt.datetime(2026, 7, 8, 12, 0, 0)
    leaf_key = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=39"
    duped = _duped_output(["v1"], ts, dupes_per_row=3)
    write_parquet(tmp_path, f"{leaf_key}/2026-07-08.parquet", duped)

    feed = FEEDS_BY_NAME["mbta_vp"]
    today = dt.date(2026, 7, 9)

    first = process_leaf(discover_leaves(store, feed, today)[0], store, duck, dry_run=False, force=True)
    assert first.rows_in == 3 and first.rows_out == 1

    # A second forced run over already-deduped content must not lose or
    # duplicate rows -- dedup on already-unique keys is a no-op.
    second = process_leaf(discover_leaves(store, feed, today)[0], store, duck, dry_run=False, force=True)
    assert second.kind == "compacted"
    assert second.rows_in == 1 and second.rows_out == 1
