"""A partial run must self-recover on the next invocation, never orphan data.

Two cases, distinguished only by whether the leaf's sources were already
deleted when the crash happened:
  * sources gone, staged file present  -> crashed after delete, before the
    final rename -> promote the staged file (it is the only remaining copy).
  * sources still present, staged file present -> crashed before sources
    were deleted -> the staged file is stale (rebuilt fresh next pass);
    discard it, leave the untouched sources for the normal pass to redo.
"""

from __future__ import annotations

import datetime as dt

from rt_pipeline.compaction.execution import Stats, recover_staging, stage_key_for

from .factories import mbta_rows, write_parquet


def test_recovers_orphaned_stage_after_sources_deleted(tmp_path, store):
    leaf_key = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=39"
    stage_key = stage_key_for(leaf_key)
    write_parquet(
        tmp_path,
        stage_key,
        mbta_rows(["v1"], dt.datetime(2026, 7, 8, 12, 0, 0), dt.datetime(2026, 7, 8, 12, 0, 1)),
    )
    assert not store.has_any_object(leaf_key)  # sources already gone (the crash point)

    stats = Stats()
    recover_staging(store, stats, dry_run=False)

    assert stats.recovered == 1
    assert stats.errors == 0
    promoted = tmp_path / leaf_key / "2026-07-08.parquet"
    assert promoted.is_file()
    assert not (tmp_path / stage_key).exists()


def test_discards_stale_stage_when_sources_still_present(tmp_path, store):
    leaf_key = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=39"
    stage_key = stage_key_for(leaf_key)
    write_parquet(
        tmp_path,
        stage_key,
        mbta_rows(["v1"], dt.datetime(2026, 7, 8, 12, 0, 0), dt.datetime(2026, 7, 8, 12, 0, 1)),
    )
    write_parquet(
        tmp_path,
        f"{leaf_key}/a.parquet",
        mbta_rows(["v1", "v2"], dt.datetime(2026, 7, 8, 12, 0, 0), dt.datetime(2026, 7, 8, 12, 0, 0)),
    )

    stats = Stats()
    recover_staging(store, stats, dry_run=False)

    assert stats.recovered == 0
    assert not (tmp_path / stage_key).exists()  # stale stage discarded
    assert (tmp_path / leaf_key / "a.parquet").exists()  # sources left for normal pass
    assert not (tmp_path / leaf_key / "2026-07-08.parquet").exists()  # no premature publish


def test_dry_run_recovery_touches_nothing(tmp_path, store):
    leaf_key = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=39"
    stage_key = stage_key_for(leaf_key)
    write_parquet(
        tmp_path,
        stage_key,
        mbta_rows(["v1"], dt.datetime(2026, 7, 8, 12, 0, 0), dt.datetime(2026, 7, 8, 12, 0, 1)),
    )

    stats = Stats()
    recover_staging(store, stats, dry_run=True)

    assert stats.recovered == 1  # counted...
    assert (tmp_path / stage_key).is_file()  # ...but nothing actually moved
