"""End-to-end smoke test for `run_compaction()` -- the entrypoint the
`compact_vp_day` Celery task calls -- against the local backend, exercising
both the legacy in-place path and the staging->curated path together, plus
the thread-pool orchestration in `__init__.py`.
"""

from __future__ import annotations

import datetime as dt

from rt_pipeline.compaction import run_compaction
from rt_pipeline.compaction.duckdb_ops import DuckDB
from rt_pipeline.compaction.storage import LocalStorage

import duckdb

from .factories import bucr_rows, mbta_rows, write_parquet


def test_run_compaction_dry_run_changes_nothing(tmp_path):
    leaf_key = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=39"
    write_parquet(
        tmp_path,
        f"{leaf_key}/a.parquet",
        mbta_rows(["v1"], dt.datetime(2026, 7, 8, 12, 0), dt.datetime(2026, 7, 8, 12, 1)),
    )
    store = LocalStorage(tmp_path)
    duck = DuckDB.connect_local()

    summary = run_compaction(feeds=["mbta_vp"], dry_run=True, store=store, duck=duck)

    assert summary["dry_run"] is True
    assert summary["errors"] == 0
    assert not (tmp_path / leaf_key / "2026-07-08.parquet").exists()


def test_run_compaction_live_then_second_pass_is_clean(tmp_path):
    # Distinct days for the two flows: a real day is either still on the
    # pre-staging layout (legacy in-place merge) or already on the
    # hourly-staging one (repartition) -- never both at once, since a
    # collector writes to one prefix or the other. Using different days here
    # also avoids both workers touching the same curated day prefix
    # concurrently.
    leaf_key = "feeds/mbta/vehicle_positions/year=2026/month=7/day=7/route_id=39"
    write_parquet(
        tmp_path,
        f"{leaf_key}/a.parquet",
        mbta_rows(["v1", "v1"], dt.datetime(2026, 7, 7, 12, 0), dt.datetime(2026, 7, 7, 12, 1)),
    )
    staging_key = "feeds/mbta/vehicle_positions_staging/year=2026/month=7/day=8"
    write_parquet(
        tmp_path,
        f"{staging_key}/hour.parquet",
        mbta_rows(
            ["v2"],
            dt.datetime(2026, 7, 8, 6, 0),
            dt.datetime(2026, 7, 8, 6, 1),
            route_id="57",
            include_route_id=True,
        ),
    )
    store = LocalStorage(tmp_path)
    duck = DuckDB.connect_local()

    first = run_compaction(feeds=["mbta_vp"], dry_run=False, store=store, duck=duck)
    assert first["errors"] == 0
    assert first["compacted"] == 2  # one legacy leaf + one staging day
    assert first["dupes_removed"] == 1  # the exact-duplicate v1 row in the legacy leaf

    assert (tmp_path / leaf_key / "2026-07-07.parquet").is_file()
    assert (
        tmp_path
        / "feeds/mbta/vehicle_positions/year=2026/month=7/day=8/route_id=57/2026-07-08.parquet"
    ).is_file()

    second = run_compaction(feeds=["mbta_vp"], dry_run=False, store=store, duck=duck)
    assert second["errors"] == 0
    assert second["compacted"] == 0
    # 2 skips: the day-7 legacy leaf (already holds its <date>.parquet), and
    # day-8's now-curated route_id=57 leaf -- once staging->curated writes
    # it, the legacy in-place discovery also sees it and correctly
    # recognizes it as already compacted (its <date>.parquet exists), so it
    # is never touched by the legacy path again either. The staging day
    # itself is no longer discovered at all (its source was fully drained).
    assert second["skipped"] == 2


def test_cutover_day_present_in_both_layouts_is_not_double_processed(tmp_path):
    """On the day the collector switches prefixes, a date has legacy objects
    (written before the switch) AND staging objects (after). Both discoveries
    claim it; only the staging unit may run, since it unions with the curated
    prefix and would otherwise race the leaf worker over the same files."""
    day = "year=2026/month=7/day=8"
    legacy_key = f"feeds/bucr/navsat/{day}"
    staging_key = f"feeds/bucr/navsat_staging/{day}"

    write_parquet(
        tmp_path,
        f"{legacy_key}/pre-switch.parquet",
        bucr_rows(["P1"], "2026-07-08 06:00:00", dt.datetime(2026, 7, 8, 12, 0)),
    )
    write_parquet(
        tmp_path,
        f"{staging_key}/post-switch.parquet",
        bucr_rows(["P2"], "2026-07-08 07:00:00", dt.datetime(2026, 7, 8, 13, 0)),
    )

    store = LocalStorage(tmp_path)
    summary = run_compaction(
        feeds=["bucr_navsat"], store=store, duck=DuckDB.connect_local()
    )

    assert summary["errors"] == 0
    # The day must be claimed by exactly ONE unit. Asserting on the final
    # parquet contents would not catch the bug: with both units queued the
    # two workers race and often still converge on the right bytes, so the
    # unit count is the only deterministic signal that the leaf was dropped.
    assert summary["compacted"] == 1
    # exactly one curated output for the day, holding rows from BOTH layouts
    outputs = sorted(p.name for p in (tmp_path / legacy_key).glob("*.parquet"))
    assert outputs == ["2026-07-08.parquet"]

    con = duckdb.connect()
    try:
        plates = con.execute(
            f"SELECT plate_number FROM read_parquet("
            f"'{tmp_path / legacy_key / '2026-07-08.parquet'}') ORDER BY plate_number"
        ).fetchall()
    finally:
        con.close()
    assert [p[0] for p in plates] == ["P1", "P2"]
    # staging drained
    assert not list((tmp_path / staging_key).glob("*.parquet"))
