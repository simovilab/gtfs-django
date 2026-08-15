"""Staging -> curated repartition: the new hourly-staging flow.

`process_staging_day` reads a day's flat hourly-staging objects (no
route_id in the path) and rewrites them into the curated
`route_id=<r>/<date>.parquet` layout MBTA readers expect. The critical
property is the second test: a day's staging source is deleted once it has
been folded into the curated output, so a *second* run for the same day
must union the new staging batch with what is *already* curated -- reading
only the (now smaller/absent) staging source would silently drop rows that
only ever existed in the first run's curated output.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from rt_pipeline.compaction.execution import process_staging_day
from rt_pipeline.compaction.feeds import FEEDS_BY_NAME
from rt_pipeline.compaction.planning import discover_staging_days

from .factories import mbta_rows, write_parquet

MBTA_STAGING = "feeds/mbta/vehicle_positions_staging/year=2026/month=7/day=8"
MBTA_CURATED = "feeds/mbta/vehicle_positions/year=2026/month=7/day=8"


def _discover_one_day(store, today=dt.date(2026, 7, 9)):
    feed = FEEDS_BY_NAME["mbta_vp"]
    days = discover_staging_days(store, feed, today)
    assert len(days) == 1
    return days[0]


def test_staging_to_curated_produces_route_partitions(tmp_path, store, duck):
    write_parquet(
        tmp_path,
        f"{MBTA_STAGING}/hour06.parquet",
        mbta_rows(
            ["v1", "v2"],
            dt.datetime(2026, 7, 8, 6, 0, 0),
            dt.datetime(2026, 7, 8, 6, 2, 0),
            route_id="39",
            include_route_id=True,
        ),
    )
    write_parquet(
        tmp_path,
        f"{MBTA_STAGING}/hour07.parquet",
        mbta_rows(
            ["v3"],
            dt.datetime(2026, 7, 8, 7, 0, 0),
            dt.datetime(2026, 7, 8, 7, 2, 0),
            route_id="57",
            include_route_id=True,
        ),
    )

    day = _discover_one_day(store)
    result = process_staging_day(day, store, duck, dry_run=False)

    assert result.kind == "compacted"
    assert result.rows_in == 3
    assert result.rows_out == 3
    assert result.dupes_removed == 0

    p39 = tmp_path / MBTA_CURATED / "route_id=39" / "2026-07-08.parquet"
    p57 = tmp_path / MBTA_CURATED / "route_id=57" / "2026-07-08.parquet"
    assert p39.is_file()
    assert p57.is_file()

    out39 = duck.con.execute(f"SELECT * FROM read_parquet('{p39}')").df()
    assert sorted(out39["vehicle_id"]) == ["v1", "v2"]

    # staging source fully drained
    assert not (tmp_path / MBTA_STAGING).exists()


def test_staging_to_curated_rerun_unions_existing_curated_no_row_loss(tmp_path, store, duck):
    # first hourly batch: v1, v2 on route 39
    write_parquet(
        tmp_path,
        f"{MBTA_STAGING}/hour06.parquet",
        mbta_rows(
            ["v1", "v2"],
            dt.datetime(2026, 7, 8, 6, 0, 0),
            dt.datetime(2026, 7, 8, 6, 2, 0),
            route_id="39",
            include_route_id=True,
        ),
    )
    day = _discover_one_day(store)
    first = process_staging_day(day, store, duck, dry_run=False)
    assert first.rows_out == 2

    # staging source for day 8 is now gone -- v2's only remaining copy is the
    # curated file. A second hourly batch lands: v1 again (a retried flush,
    # same (feed_name, vehicle_id, ts) key -> duplicate) plus a brand-new v3.
    v1_dup = mbta_rows(
        ["v1"],
        dt.datetime(2026, 7, 8, 6, 0, 0),  # same ts as the already-curated v1
        dt.datetime(2026, 7, 8, 7, 30, 0),  # later ingested_at
        route_id="39",
        include_route_id=True,
    )
    v3_new = mbta_rows(
        ["v3"],
        dt.datetime(2026, 7, 8, 7, 0, 0),
        dt.datetime(2026, 7, 8, 7, 30, 0),
        route_id="39",
        include_route_id=True,
    )
    write_parquet(tmp_path, f"{MBTA_STAGING}/hour07.parquet", pd.concat([v1_dup, v3_new]))

    day2 = _discover_one_day(store)
    second = process_staging_day(day2, store, duck, dry_run=False)

    assert second.kind == "compacted"
    # union: 2 already-curated rows + 2 new staging rows = 4 raw; v1 collapses -> 3 unique
    assert second.rows_in == 4
    assert second.rows_out == 3
    assert second.dupes_removed == 1

    p39 = tmp_path / MBTA_CURATED / "route_id=39" / "2026-07-08.parquet"
    out = duck.con.execute(f"SELECT * FROM read_parquet('{p39}')").df()
    # v2 (never present in the second staging batch) must survive the rerun.
    assert sorted(out["vehicle_id"]) == ["v1", "v2", "v3"]
    assert len(out) == 3


def test_staging_to_curated_second_run_with_no_new_staging_is_skipped(tmp_path, store, duck):
    write_parquet(
        tmp_path,
        f"{MBTA_STAGING}/hour06.parquet",
        mbta_rows(
            ["v1"],
            dt.datetime(2026, 7, 8, 6, 0, 0),
            dt.datetime(2026, 7, 8, 6, 2, 0),
            route_id="39",
            include_route_id=True,
        ),
    )
    day = _discover_one_day(store)
    process_staging_day(day, store, duck, dry_run=False)

    feed = FEEDS_BY_NAME["mbta_vp"]
    days_again = discover_staging_days(store, feed, dt.date(2026, 7, 9))
    assert days_again == []  # nothing left to fold in -- not rediscovered at all


def test_staging_to_curated_many_routes_each_get_exactly_one_file(tmp_path, store, duck):
    """Regression test for the 2026-08-14 production incident: DuckDB's own
    `PARTITION_BY` writer produced 2-4 files for dozens of route partitions
    on the first automatic run, even under `PRAGMA threads=1`, tripping
    `_promote_partitioned`'s one-file-per-cell guard and leaving that day's
    curated output incomplete. `compact_partitioned` now writes each
    partition cell with its own explicit single-file `COPY` instead, so this
    asserts every route in a many-route batch lands as exactly one file with
    exactly its own rows -- no cross-partition contamination, no extras.
    """
    routes = ["11", "39", "57", "101", "Green-E"]
    for i, route in enumerate(routes):
        write_parquet(
            tmp_path,
            f"{MBTA_STAGING}/hour{i:02d}.parquet",
            mbta_rows(
                [f"v-{route}-a", f"v-{route}-b"],
                dt.datetime(2026, 7, 8, i, 0, 0),
                dt.datetime(2026, 7, 8, i, 2, 0),
                route_id=route,
                include_route_id=True,
            ),
        )

    day = _discover_one_day(store)
    result = process_staging_day(day, store, duck, dry_run=False)

    assert result.kind == "compacted"
    assert result.rows_in == 10
    assert result.rows_out == 10

    for route in routes:
        route_dir = tmp_path / MBTA_CURATED / f"route_id={route}"
        files = list(route_dir.glob("*.parquet"))
        assert len(files) == 1, f"route {route}: expected 1 file, got {len(files)}"
        # hive_partitioning=false so reading the lone file doesn't auto-reconstruct
        # route_id from its own parent directory name -- this checks what's
        # actually stored in the file, not a read-path artifact.
        out = duck.con.execute(
            f"SELECT * FROM read_parquet('{files[0]}', hive_partitioning=false)"
        ).df()
        assert sorted(out["vehicle_id"]) == [f"v-{route}-a", f"v-{route}-b"]
        assert "route_id" not in out.columns  # reconstructed from the path, not stored in-file

    # the scratch mirror is fully cleaned up, nothing left behind
    assert not (tmp_path / "_staging" / MBTA_CURATED).exists()


def test_bucr_staging_to_curated_single_file_per_day(tmp_path, store, duck):
    from .factories import bucr_rows

    staging = "feeds/bucr/navsat_staging/year=2026/month=7/day=8"
    curated = "feeds/bucr/navsat/year=2026/month=7/day=8"

    write_parquet(
        tmp_path,
        f"{staging}/batch1.parquet",
        bucr_rows(["ABC123", "DEF456"], "2026-07-08 06:00:00", dt.datetime(2026, 7, 8, 6, 1, 0)),
    )
    write_parquet(
        tmp_path,
        f"{staging}/batch2.parquet",
        bucr_rows(["ABC123"], "2026-07-08 06:00:00", dt.datetime(2026, 7, 8, 6, 30, 0)),  # dup
    )

    feed = FEEDS_BY_NAME["bucr_navsat"]
    days = discover_staging_days(store, feed, dt.date(2026, 7, 9))
    assert len(days) == 1

    result = process_staging_day(days[0], store, duck, dry_run=False)
    assert result.rows_in == 3
    assert result.rows_out == 2
    assert result.dupes_removed == 1

    out_path = tmp_path / curated / "2026-07-08.parquet"
    assert out_path.is_file()
    out = duck.con.execute(f"SELECT * FROM read_parquet('{out_path}')").df()
    assert sorted(out["plate_number"]) == ["ABC123", "DEF456"]
    assert not (tmp_path / staging).exists()
