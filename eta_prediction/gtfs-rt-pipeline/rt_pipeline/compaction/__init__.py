"""Daily compaction of the tiny per-poll/per-hour Parquet objects the GTFS
collectors write into MinIO.

Two collectors (MBTA VehiclePositions, bUCR navsat) each write a stream of
small Parquet objects. The MBTA one alone reached ~500,000 objects/day and
exhausted the server's inodes; MinIO spends several inodes per object. The
fix has two halves: the collectors now spool locally and flush hourly to a
`*_staging` prefix (another module's concern), and this package folds those
staged objects -- plus whatever is still sitting in the pre-staging legacy
layout -- into the curated, queryable layout once a day.

Two units of work, both idempotent and crash-safe (see `execution.py`):

  * legacy in-place merge -- `feeds/<x>/<dataset>/year=/month=/day=[/route_id=]/*`
    -> one file per leaf. Covers the ~7k objects left in the pre-staging
    layout; not needed once that backlog is cleared, but harmless to keep
    running (a leaf that already holds its `<date>.parquet` is skipped).
  * staging -> curated -- `feeds/<x>/<dataset>_staging/year=/month=/day=/*`
    (a handful of hourly objects) repartitioned + merged into the curated
    layout, UNIONed with anything already compacted for that day so
    re-running the same day never drops rows.

Both paths dedup on a per-feed natural key, keeping the most recently
ingested row (`feeds.FEEDS`). Neither ever touches the current UTC day --
the collectors are still writing to it.

`run_compaction()` is the entrypoint the `compact_vp_day` Celery task calls;
`compaction.cli` is a thin argparse wrapper around the same function for
manual / backfill runs.
"""

from __future__ import annotations

import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed

from .duckdb_ops import DuckDB
from .execution import Result, Stats, process_leaf, process_staging_day, recover_staging
from .feeds import FEEDS, FEEDS_BY_NAME, Feed
from .planning import Leaf, StagingDay, discover_leaves, discover_staging_days
from .storage import CommandError, LocalStorage, McStorage, Storage

__all__ = [
    "run_compaction",
    "FEEDS",
    "FEEDS_BY_NAME",
    "Feed",
    "Storage",
    "LocalStorage",
    "McStorage",
]


def _record(result: Result, *stats_objs: Stats) -> None:
    for stats in stats_objs:
        if result.kind == "compacted":
            stats.compacted += 1
            stats.rows_in += result.rows_in
            stats.rows_out += result.rows_out
            stats.details.append(
                f"COMPACT {result.key}  src={result.rows_in} dst={result.rows_out} "
                f"dupes_removed={result.dupes_removed}"
            )
        elif result.kind == "skipped":
            stats.skipped += 1
        else:
            stats.errors += 1
            stats.details.append(f"ERROR {result.key}: {result.detail}")


def run_compaction(
    feeds: list[str] | None = None,
    dry_run: bool = False,
    since: dt.date | None = None,
    until: dt.date | None = None,
    *,
    store: Storage | None = None,
    duck: DuckDB | None = None,
    alias: str = "simovilab",
    bucket: str = "transit",
    endpoint: str | None = None,
    workers: int = 12,
) -> dict:
    """Run one compaction pass. Returns a JSON-serializable summary dict.

    `feeds` (list of `Feed.name`, default: all), `dry_run`, `since`/`until`
    (inclusive UTC date bounds, default: unbounded up to but excluding
    today) match the signature the `compact_vp_day` Celery task calls.

    `store`/`duck` are the test injection points: pass a `LocalStorage` plus
    `DuckDB.connect_local()` to run entirely against a temp directory.
    Production callers omit both and get the `mc` + `s3://` backend, with
    credentials resolved from an `mc` alias or the environment.
    """
    if store is None or duck is None:
        from .credentials import load_credentials

        creds = load_credentials(alias, endpoint)
        store = store or McStorage(alias, bucket)
        duck = duck or DuckDB.connect_s3(creds)

    today = dt.datetime.now(dt.timezone.utc).date()
    selected = [f for f in FEEDS if not feeds or f.name in feeds]

    stats = Stats()
    recover_staging(store, stats, dry_run)

    per_feed: dict[str, dict] = {}
    for feed in selected:
        feed_stats = Stats()
        try:
            leaves: list[Leaf] = discover_leaves(store, feed, today, since=since, until=until)
            days: list[StagingDay] = discover_staging_days(
                store, feed, today, since=since, until=until
            )
        except CommandError as exc:
            feed_stats.errors += 1
            feed_stats.details.append(f"ERROR could not list {feed.name}: {exc}")
            stats.errors += 1
            per_feed[feed.name] = feed_stats.as_dict()
            continue

        # A date can legitimately appear in BOTH layouts on the cutover day: the
        # collector wrote legacy objects before the switch to the staging prefix
        # and staging objects after, and compaction only runs once the day has
        # closed. Running both units would put two pool workers on the same
        # curated day prefix at once. `process_staging_day` unions staging with
        # whatever already sits in the curated prefix, so it subsumes the legacy
        # leaf — keep the staging unit and drop the leaf.
        staged_dates = {d.date for d in days}
        units: list[tuple[str, Leaf | StagingDay]] = [
            ("leaf", lf) for lf in leaves if lf.date not in staged_dates
        ]
        units += [("day", d) for d in days]

        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {
                pool.submit(
                    _run_unit,
                    kind,
                    unit,
                    store,
                    duck.clone() if not dry_run else duck,
                    dry_run,
                ): unit
                for kind, unit in units
            }
            for fut in as_completed(futures):
                _record(fut.result(), stats, feed_stats)

        per_feed[feed.name] = feed_stats.as_dict()

    summary = stats.as_dict()
    summary["today_utc"] = today.isoformat()
    summary["dry_run"] = dry_run
    summary["feeds"] = per_feed
    return summary


def _run_unit(
    kind: str, unit: Leaf | StagingDay, store: Storage, duck: DuckDB, dry_run: bool
) -> Result:
    if kind == "leaf":
        return process_leaf(unit, store, duck, dry_run)  # type: ignore[arg-type]
    return process_staging_day(unit, store, duck, dry_run)  # type: ignore[arg-type]
