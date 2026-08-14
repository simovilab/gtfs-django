"""Per-unit compaction workers plus the crash-recovery pass.

`process_leaf` and `process_staging_day` are pure workers: given their unit
of work plus the two collaborators (Storage, DuckDB) they return a Result and
mutate no shared state, so many units can run in a thread pool safely (each
leaf/day is an independent path in the bucket).

Both verify `dst <= src` and `dst > 0` before ever deleting a source -- with
dedup in place, `dst < src` is the *expected* outcome (that is the point),
so the old `src == dst` assertion from the pre-dedup script would refuse
every real compaction. `dst > src` would mean dedup somehow manufactured
rows, which is corruption, not success.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .duckdb_ops import CompactionCounts, DuckDB
from .feeds import STAGING_NAME, STAGING_PREFIX, output_name
from .planning import Leaf, StagingDay, date_from_key
from .storage import CommandError, Storage

# --- legacy in-place leaf compaction ----------------------------------------


def stage_key_for(leaf_key: str) -> str:
    """Mirror a leaf's path under the staging prefix (reversible via leaf_from_stage)."""
    return f"{STAGING_PREFIX}/{leaf_key}/{STAGING_NAME}"


def leaf_from_stage(stage_key: str) -> str:
    inner = stage_key[len(STAGING_PREFIX) + 1 :]
    return inner[: -(len(STAGING_NAME) + 1)]


def mirror_key_for(day_key: str) -> str:
    """Staged mirror directory for a staging->curated day (route_id repartition)."""
    return f"{STAGING_PREFIX}/{day_key}"


@dataclass
class Result:
    kind: str  # "compacted" | "skipped" | "error"
    key: str
    rows_in: int = 0
    rows_out: int = 0
    detail: str = ""

    @property
    def dupes_removed(self) -> int:
        return self.rows_in - self.rows_out


@dataclass
class Stats:
    compacted: int = 0
    recovered: int = 0
    skipped: int = 0
    errors: int = 0
    rows_in: int = 0
    rows_out: int = 0
    details: list[str] = field(default_factory=list)

    @property
    def dupes_removed(self) -> int:
        return self.rows_in - self.rows_out

    def as_dict(self) -> dict:
        return {
            "compacted": self.compacted,
            "recovered": self.recovered,
            "skipped": self.skipped,
            "errors": self.errors,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "dupes_removed": self.dupes_removed,
            "details": list(self.details),
        }


def recover_staging(store: Storage, stats: Stats, dry_run: bool) -> None:
    """Finish or discard any staged files left behind by a crashed in-place run."""
    try:
        staged = store.list_recursive(STAGING_PREFIX)
    except CommandError:
        return  # no staging prefix yet
    for rel in staged:
        stage_key = f"{STAGING_PREFIX}/{rel}"
        if not stage_key.endswith(STAGING_NAME):
            continue
        leaf = leaf_from_stage(stage_key)
        if not store.has_any_object(leaf):
            # crashed after deleting sources -> promote the staged file
            if not dry_run:
                store.move(stage_key, f"{leaf}/{output_name(date_from_key(leaf))}")
            stats.recovered += 1
            stats.details.append(f"RECOVER  {leaf}")
        else:
            # stale staged file -> drop it, the normal pass rebuilds it
            if not dry_run:
                store.remove(stage_key)
            stats.details.append(f"CLEANUP  stale stage for {leaf}")


def process_leaf(
    leaf: Leaf, store: Storage, duck: DuckDB, dry_run: bool, force: bool = False
) -> Result:
    """Compact one legacy in-place leaf: merge + dedup its files into one.

    `force=True` bypasses the already-compacted guard below and re-reads the
    existing ``<date>.parquet`` as its own source -- for backfilling dedup
    onto leaves a pre-dedup compaction already merged into one file (roadmap
    0.4b). Never used by the routine daily pass; only an explicit `--force`
    backfill run sets it.
    """
    # A finished leaf holds only the compacted <date>.parquet (sources are
    # deleted before it is published), so its presence is a sufficient, cheap
    # "already compacted" guard for a closed day -- unless `force` asks to
    # redo it anyway.
    name = output_name(leaf.date)
    if not force and store.exists(f"{leaf.key}/{name}"):
        return Result("skipped", leaf.key)

    if dry_run:
        return Result("compacted", leaf.key)

    stage_key = stage_key_for(leaf.key)
    try:
        counts = duck.compact_leaf(
            [(store.uri(leaf.key.rstrip("/") + "/*.parquet"), False)],
            store.uri(stage_key),
            sort_col=leaf.feed.sort_col,
            dedup_keys=leaf.feed.dedup_keys,
            recency_col=leaf.feed.recency_col,
        )
        if counts.src == 0:
            return Result("skipped", leaf.key)
        _verify(counts)
        store.remove_prefix(leaf.key)  # drop sources
        store.move(stage_key, f"{leaf.key}/{name}")  # publish
    except CommandError as exc:
        if store.exists(stage_key):
            store.remove(stage_key)
        return Result("error", leaf.key, detail=str(exc))

    return Result("compacted", leaf.key, rows_in=counts.src, rows_out=counts.dst)


# --- staging -> curated repartition -----------------------------------------


def _verify(counts: CompactionCounts) -> None:
    if counts.dst > counts.src or counts.dst == 0:
        raise CommandError(f"bad row counts src={counts.src} dst={counts.dst}")


def _promote_partitioned(store: Storage, mirror_key: str, curated_key: str, date) -> None:
    """Move each route's single staged file into its final curated location."""
    name = output_name(date)
    for route_dir in store.child_dirs(mirror_key):
        files = [
            f for f in store.list_recursive(f"{mirror_key}/{route_dir}") if f.endswith(".parquet")
        ]
        if not files:
            continue
        if len(files) != 1:
            raise CommandError(
                f"expected one output file per partition, got {len(files)} under "
                f"{mirror_key}/{route_dir}"
            )
        route_final_dir = f"{curated_key}/{route_dir}"
        if store.has_any_object(route_final_dir):
            store.remove_prefix(route_final_dir)
        store.move(f"{mirror_key}/{route_dir}/{files[0]}", f"{route_final_dir}/{name}")


def process_staging_day(day: StagingDay, store: Storage, duck: DuckDB, dry_run: bool) -> Result:
    """Repartition one closed day of hourly-staging objects into the curated layout.

    Sources: the staging day's flat objects, UNIONed with any curated files
    already at this day (from a previous run) -- that union is what makes a
    second run for the same day safe; without it a re-run would drop rows
    already compacted. The staging source is only deleted after the curated
    output is fully in place, so a crash anywhere in between just means the
    next run redoes this day's work from scratch -- no explicit recovery
    bookkeeping needed for this path.

    Assumes a given day is exclusively on the legacy in-place layout or the
    hourly-staging one, never both -- true in practice, since a collector
    writes to one prefix or the other, never switching mid-day. If both
    happened to hold data for the same day, running `process_leaf` and
    `process_staging_day` for it concurrently (as `run_compaction`'s thread
    pool would) could race on the same curated-day files; that is out of
    scope here.
    """
    if not store.has_any_object(day.staging_key):
        return Result("skipped", day.staging_key)

    if dry_run:
        return Result("compacted", day.staging_key)

    mirror_key = mirror_key_for(day.curated_key)
    if store.has_any_object(mirror_key):
        store.remove_prefix(mirror_key)  # start the mirror clean each run

    staging_glob = store.uri(day.staging_key.rstrip("/") + "/*.parquet")
    curated_glob = (
        store.uri(day.curated_key.rstrip("/") + "/*/*.parquet")
        if day.feed.route_level
        else store.uri(day.curated_key.rstrip("/") + "/*.parquet")
    )

    try:
        if day.feed.route_level:
            # staging has no route_id in its path (hive_partitioning=False,
            # route_id is a real data column instead); curated stores
            # route_id only in the path (hive_partitioning=True to
            # reconstruct it as a column).
            counts = duck.compact_partitioned(
                [(staging_glob, False), (curated_glob, True)],
                store.uri(mirror_key),
                sort_col=day.feed.sort_col,
                dedup_keys=day.feed.dedup_keys,
                recency_col=day.feed.recency_col,
                partition_cols=("route_id",),
            )
        else:
            counts = duck.compact_leaf(
                [(staging_glob, False), (curated_glob, False)],
                store.uri(f"{mirror_key}/{output_name(day.date)}"),
                sort_col=day.feed.sort_col,
                dedup_keys=day.feed.dedup_keys,
                recency_col=day.feed.recency_col,
            )

        _verify(counts)

        if day.feed.route_level:
            _promote_partitioned(store, mirror_key, day.curated_key, day.date)
        else:
            store.remove_prefix(day.curated_key)
            store.move(
                f"{mirror_key}/{output_name(day.date)}",
                f"{day.curated_key}/{output_name(day.date)}",
            )

        store.remove_prefix(day.staging_key)
        if store.has_any_object(mirror_key):
            store.remove_prefix(mirror_key)
    except CommandError as exc:
        return Result("error", day.staging_key, detail=str(exc))

    return Result("compacted", day.staging_key, rows_in=counts.src, rows_out=counts.dst)
