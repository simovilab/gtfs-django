"""Local DuckDB spool for MBTA VehiclePositions.

Why this exists: ``s3_writer.write_vehicle_positions`` runs one
``COPY ... PARTITION_BY (year, month, day, route_id)`` per call. Calling it
once per poll means one S3 PUT per active route per poll -- with ~160 active
MBTA routes that's ~160 round-trips from Hetzner to the MinIO server in Costa
Rica every 5 seconds. That fan-out is what pushed poll latency to 330+
seconds against a 5-second beat schedule, backed up the Celery queue without
bound, and eventually OOM-killed the worker (see the plan / commit history
for the full incident writeup).

The fix: a poll writes to this local spool instead (~200ms), and a separate
hourly task (``rt_pipeline.tasks.flush_vp_spool_s3``) drains it to S3 with
ONE ``COPY ... PARTITION_BY (year, month, day)`` per (day) partition --
``route_id`` is deliberately excluded from the flush partitioning; that is
the fan-out this module exists to avoid.

Connection handling: DuckDB is single-writer -- only one process may hold an
open read/write connection to a given database file. This class opens a
fresh connection per method call and closes it before returning, rather than
holding one connection for the lifetime of the ``Spool`` object. That way no
lock is held across a Celery task boundary (a killed/restarted worker never
leaves a stale writer lock on the file). The corollary, per the plan, is that
the poll queue MUST run at ``--concurrency=1`` -- exactly one process may
write to the spool at a time. A poll takes ~200ms, so one worker comfortably
sustains a 5s cadence.

UTC invariant: this module follows the same rule as
``schema.py::add_partition_columns`` -- a tz-naive timestamp is ALWAYS
interpreted as UTC, never local time. ``ts``/``ingested_at`` are stored as
DuckDB ``TIMESTAMP`` (tz-naive) columns holding UTC wall-clock values;
cutoffs passed to ``select_before``/``delete_before`` are normalized to
tz-naive UTC before comparison (see ``_as_utc_naive``).
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from pathlib import Path
from typing import Any, Optional

import duckdb
import pandas as pd

from .config import S3Config
from .s3_writer import connect
from .schema import COMPRESSION, DATA_COLUMNS, add_partition_columns, missing_columns

# DuckDB column types for the spool table, keyed by column name. The DDL is
# derived from schema.DATA_COLUMNS + route_id (SPOOL_COLUMNS below) rather
# than a hardcoded column list, so a schema change to DATA_COLUMNS only needs
# a type added here (unrecognized columns default to VARCHAR).
_COLUMN_TYPES: dict[str, str] = {
    "feed_name": "VARCHAR",
    "vehicle_id": "VARCHAR",
    "ts": "TIMESTAMP",
    "lat": "DOUBLE",
    "lon": "DOUBLE",
    "bearing": "DOUBLE",
    "speed": "DOUBLE",
    "trip_id": "VARCHAR",
    "current_stop_sequence": "INTEGER",
    "current_status": "VARCHAR",
    "stop_id": "VARCHAR",
    "ingested_at": "TIMESTAMP",
    "route_id": "VARCHAR",
}

# Spool table columns = the data columns (schema.py) + the route partition
# key, in a fixed order used for both DDL and INSERT statements.
SPOOL_COLUMNS: tuple[str, ...] = (*DATA_COLUMNS, "route_id")

# Natural key from S3_LAYOUT.md: "Natural key for dedupe on replay/backfill:
# (feed_name, vehicle_id, ts)".
NATURAL_KEY: tuple[str, ...] = ("feed_name", "vehicle_id", "ts")

DEFAULT_SPOOL_PATH = "/data/spool/vp_spool.duckdb"

TABLE_NAME = "vp_spool"


def _column_type(name: str) -> str:
    return _COLUMN_TYPES.get(name, "VARCHAR")


def _as_utc_naive(ts: dt.datetime) -> dt.datetime:
    """Normalize a cutoff to tz-naive UTC.

    Mirrors the invariant in ``schema.py::add_partition_columns``: tz-naive
    values are already treated as UTC and pass through unchanged; tz-aware
    values are converted to UTC and stripped of tzinfo so they compare
    correctly against the tz-naive ``TIMESTAMP`` columns in the spool.
    """
    if ts.tzinfo is not None:
        return ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return ts


class Spool:
    """Durable local staging area for VehiclePositions between polls and flush.

    Backed by a DuckDB file on disk (never ``:memory:``) so a container
    restart cannot lose fixes collected since the last hourly flush --
    durability here is the entire reason polls no longer write straight to
    S3.
    """

    def __init__(self, path: Optional[str] = None, *, must_exist: bool = False) -> None:
        self.path = path or DEFAULT_SPOOL_PATH
        if must_exist and not Path(self.path).exists():
            # Guards against opening a spool in the wrong process. The flush
            # ran on a worker with no spool volume mounted once: DuckDB happily
            # created an empty database there and the task reported a healthy
            # {'flushed': 0} while the real spool grew without bound. An absent
            # file at flush time is always a misconfiguration -- the poll task
            # creates it long before the first flush -- so fail loudly.
            raise FileNotFoundError(
                f"spool database missing at {self.path!r}. The flush must run on "
                "the worker that mounts the spool volume (queue 'fetch'); "
                "opening it elsewhere silently creates an empty database."
            )
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(self.path)
        # Force UTC as the session timezone. Without this, casting a
        # tz-aware (UTC) pandas column into this table's tz-naive TIMESTAMP
        # columns silently applies the HOST's local timezone offset instead
        # of UTC -- verified this empirically: on a UTC-6 host, 08:00 UTC
        # landed in the table as 02:00. That would silently violate the UTC
        # invariant documented above on any non-UTC host (dev laptop, a
        # misconfigured container, ...).
        con.execute("SET TimeZone='UTC'")
        return con

    def _init_schema(self) -> None:
        cols_sql = ", ".join(f"{c} {_column_type(c)}" for c in SPOOL_COLUMNS)
        key_sql = ", ".join(NATURAL_KEY)
        con = self._connect()
        try:
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} "
                f"({cols_sql}, UNIQUE ({key_sql}))"
            )
        finally:
            con.close()

    def append(self, df: pd.DataFrame) -> int:
        """Insert new rows, skipping any that collide on the natural key.

        Returns the number of rows NEWLY inserted -- not the number offered
        -- so callers can distinguish ``rows_fetched`` from ``rows_new``
        (e.g. for status reporting). Never mutates ``df``.
        """
        if df is None or df.empty:
            return 0
        miss = missing_columns(df)
        if miss:
            raise ValueError(f"DataFrame missing required columns: {miss}")

        # Fixed column order matching SPOOL_COLUMNS; a new frame, not a
        # mutation of the caller's df.
        pdf = df.loc[:, list(SPOOL_COLUMNS)]
        cols_sql = ", ".join(SPOOL_COLUMNS)
        key_sql = ", ".join(NATURAL_KEY)
        con = self._connect()
        try:
            before = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            con.register("vp_batch", pdf)
            try:
                con.execute(
                    f"INSERT INTO {TABLE_NAME} ({cols_sql}) "
                    f"SELECT {cols_sql} FROM vp_batch "
                    f"ON CONFLICT ({key_sql}) DO NOTHING"
                )
            finally:
                con.unregister("vp_batch")
            after = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            return int(after - before)
        finally:
            con.close()

    def select_before(self, cutoff: dt.datetime) -> pd.DataFrame:
        """Rows with ``ts < cutoff`` (half-open, UTC). Read-only.

        Deliberately separate from ``delete_before`` so a flush can write to
        S3, verify the write, and only then delete -- a failed PUT never
        loses rows.
        """
        cutoff_utc = _as_utc_naive(cutoff)
        con = self._connect()
        try:
            return con.execute(
                f"SELECT * FROM {TABLE_NAME} WHERE ts < ? ORDER BY ts",
                [cutoff_utc],
            ).df()
        finally:
            con.close()

    def delete_before(self, cutoff: dt.datetime) -> int:
        """Delete rows with ``ts < cutoff`` (half-open, UTC). Returns rows deleted."""
        cutoff_utc = _as_utc_naive(cutoff)
        con = self._connect()
        try:
            before = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            con.execute(f"DELETE FROM {TABLE_NAME} WHERE ts < ?", [cutoff_utc])
            after = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            return int(before - after)
        finally:
            con.close()

    def stats(self) -> dict:
        """Row count, oldest/newest ``ts``, and on-disk file size.

        Safe to call on an empty spool (``oldest_ts``/``newest_ts`` are
        ``None``, ``rows`` is ``0``).
        """
        con = self._connect()
        try:
            rows, oldest_ts, newest_ts = con.execute(
                f"SELECT count(*), min(ts), max(ts) FROM {TABLE_NAME}"
            ).fetchone()
        finally:
            con.close()
        try:
            size_bytes = os.path.getsize(self.path)
        except OSError:
            size_bytes = 0
        return {
            "rows": int(rows or 0),
            "oldest_ts": oldest_ts,
            "newest_ts": newest_ts,
            "bytes": size_bytes,
        }


def flush_to_staging(
    spool: Spool,
    base_uri: str,
    cutoff: dt.datetime,
    *,
    config: Optional[S3Config] = None,
) -> dict[str, Any]:
    """Drain rows older than ``cutoff`` from ``spool`` into a staging Parquet
    dataset at ``base_uri``, partitioned ONLY by ``(year, month, day)``.

    Deliberately no ``route_id`` in the partitioning -- that per-route
    fan-out on every write is exactly what turned each poll into ~160 S3
    PUTs and caused the OOM incident this spool exists to fix. One flush
    call now writes at most a handful of objects (one per day the drained
    rows span).

    Write-then-verify-then-delete: after ``COPY``, the row count is read
    back from the written files' Parquet footers (no data scan) and checked
    against what was meant to be written; rows are deleted from the spool
    ONLY after that check passes. If the write or verify raises, the
    exception propagates and the spool is left untouched -- callers should
    treat that as "try again next flush", not data loss. A retried flush's
    overlapping rows are expected; downstream compaction dedups them.

    Returns ``{"flushed": 0, "days": []}`` when there is nothing to drain,
    else ``{"flushed": <rows deleted>, "days": [<"YYYY-MM-DD">, ...]}``.
    """
    df = spool.select_before(cutoff)
    if df.empty:
        return {"flushed": 0, "days": []}

    pdf = add_partition_columns(df)
    file_id = uuid.uuid4().hex
    con = connect(base_uri, config)
    try:
        con.register("vp_flush", pdf)
        try:
            con.execute(
                f"""
                COPY vp_flush TO '{base_uri}' (
                    FORMAT PARQUET,
                    PARTITION_BY (year, month, day),
                    COMPRESSION '{COMPRESSION}',
                    FILENAME_PATTERN '{file_id}_{{i}}',
                    OVERWRITE_OR_IGNORE TRUE
                );
                """
            )
        finally:
            con.unregister("vp_flush")

        # Verify: sum the row counts (footers only) of exactly the files
        # this call wrote, identified by the uuid filename prefix.
        written_glob = f"{base_uri}/**/{file_id}_*.parquet"
        row = con.execute(
            f"SELECT coalesce(sum(num_rows), 0) FROM parquet_file_metadata('{written_glob}')"
        ).fetchone()
        verified_rows = int(row[0]) if row else 0
        if verified_rows != len(pdf):
            raise RuntimeError(
                f"flush verify mismatch: wrote {len(pdf)} rows, "
                f"found {verified_rows} in staging"
            )
    finally:
        con.close()

    # Only reached once the write is verified -> safe to delete.
    deleted = spool.delete_before(cutoff)
    days_df = pdf[["year", "month", "day"]].drop_duplicates()
    days = [
        f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        for y, m, d in days_df.itertuples(index=False)
    ]
    return {"flushed": deleted, "days": days}
