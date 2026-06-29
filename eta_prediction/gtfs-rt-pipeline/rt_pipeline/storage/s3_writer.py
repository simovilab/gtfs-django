"""Write/read MBTA VehiclePositions as Hive-partitioned Parquet via DuckDB.

DuckDB handles both sides: ``COPY ... PARTITION_BY`` for writes and
``read_parquet(..., hive_partitioning=true)`` for reads, with partition
pruning on ``route_id`` (and year/month/day). The same code path works for a
local directory (tests) or an ``s3://`` URI (MinIO) — only the connection
setup differs.
"""

from __future__ import annotations

import datetime as dt
import re
import uuid
from typing import Optional, Sequence

import duckdb
import pandas as pd

from .config import S3Config
from .schema import (
    COMPRESSION,
    PARTITION_COLUMNS,
    add_partition_columns,
    missing_columns,
    s3_base_uri,
)


def _is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def connect(
    base_uri: str, config: Optional[S3Config] = None
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, configuring the S3 secret for ``s3://`` URIs."""
    con = duckdb.connect()
    if _is_s3(base_uri):
        if config is None:
            config = S3Config.from_env()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(
            f"""
            CREATE OR REPLACE SECRET s3 (
                TYPE s3, PROVIDER config,
                KEY_ID '{config.access_key}',
                SECRET '{config.secret_key}',
                REGION '{config.region}',
                ENDPOINT '{config.endpoint}',
                USE_SSL {str(config.use_ssl).lower()},
                URL_STYLE '{config.url_style}'
            );
            """
        )
    return con


def write_vehicle_positions(
    df: pd.DataFrame,
    base_uri: Optional[str] = None,
    *,
    config: Optional[S3Config] = None,
    compression: str = COMPRESSION,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> int:
    """Append a batch of VehiclePositions to the partitioned dataset.

    Files get a uuid name so concurrent/repeated batch writes into the same
    partition never clobber each other. Returns the number of rows written.
    """
    if df is None or df.empty:
        return 0
    miss = missing_columns(df)
    if miss:
        raise ValueError(f"DataFrame missing required columns: {miss}")

    base_uri = base_uri or s3_base_uri()
    pdf = add_partition_columns(df)
    owns_con = con is None
    con = con or connect(base_uri, config)
    try:
        con.register("vp_batch", pdf)
        file_id = uuid.uuid4().hex
        part_cols = ", ".join(PARTITION_COLUMNS)
        con.execute(
            f"""
            COPY vp_batch TO '{base_uri}' (
                FORMAT PARQUET,
                PARTITION_BY ({part_cols}),
                COMPRESSION '{compression}',
                FILENAME_PATTERN '{file_id}_{{i}}',
                OVERWRITE_OR_IGNORE TRUE
            );
            """
        )
        return len(pdf)
    finally:
        con.unregister("vp_batch")
        if owns_con:
            con.close()


def _q(value: str) -> str:
    """SQL-quote a string literal."""
    return "'" + value.replace("'", "''") + "'"


_PART_RE = re.compile(
    r"year=(?P<year>\d+)/month=(?P<month>\d+)/day=(?P<day>\d+)/"
    r"route_id=(?P<route_id>[^/]+)/"
)


def _path_matches(
    path: str,
    route_ids: Optional[set[str]],
    start_day: Optional[dt.date],
    end_day: Optional[dt.date],
) -> bool:
    """Prune a partition file path by route and (coarse) day before opening it.

    ``start_day``/``end_day`` are inclusive day bounds derived from the
    half-open ``[start, end)`` ts range; exact ts filtering still happens in
    SQL. Paths that don't carry the expected partition layout are kept (so an
    unexpected layout fails loudly later rather than being silently dropped).
    """
    m = _PART_RE.search(path)
    if not m:
        return True
    if route_ids is not None and m.group("route_id") not in route_ids:
        return False
    if start_day is not None or end_day is not None:
        d = dt.date(int(m.group("year")), int(m.group("month")), int(m.group("day")))
        if start_day is not None and d < start_day:
            return False
        if end_day is not None and d > end_day:
            return False
    return True


def read_vehicle_positions(
    route_ids: Optional[Sequence[str]] = None,
    start: Optional[dt.datetime] = None,
    end: Optional[dt.datetime] = None,
    base_uri: Optional[str] = None,
    *,
    config: Optional[S3Config] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    dedup: bool = True,
) -> pd.DataFrame:
    """Read VehiclePositions, pruning partitions by route and date.

    To avoid opening every tiny per-poll file (the dataset is many thousands
    of small parquet objects), this first lists paths with a cheap ``glob()``
    LIST call, prunes them in Python by ``route_ids`` and day, then reads only
    the surviving files. ``start``/``end`` (UTC, half-open ``[start, end)``)
    additionally filter ``ts`` exactly. Returns an empty frame when nothing
    matches.

    ``dedup`` (default on) collapses duplicate observations keyed by
    ``(vehicle_id, ts)`` — keeping the latest ``ingested_at`` — so multiple
    concurrent pollers writing the same MBTA feed into one dataset stay
    idempotent. Pass ``dedup=False`` to get every raw row.
    """
    base_uri = base_uri or s3_base_uri()
    glob = f"{base_uri}/**/*.parquet"
    owns_con = con is None
    con = con or connect(base_uri, config)
    try:
        try:
            paths = [row[0] for row in con.execute(
                f"SELECT file FROM glob({_q(glob)})"
            ).fetchall()]
        except duckdb.IOException:
            # No files yet (empty/nonexistent dataset) -> empty result.
            return pd.DataFrame()

        route_set = {str(r) for r in route_ids} if route_ids else None
        start_day = start.date() if start is not None else None
        end_day = end.date() if end is not None else None
        files = [p for p in paths if _path_matches(p, route_set, start_day, end_day)]
        if not files:
            return pd.DataFrame()

        file_list = "[" + ", ".join(_q(p) for p in files) + "]"
        sql = (
            f"SELECT * FROM read_parquet({file_list}, hive_partitioning=true, "
            "union_by_name=true)"
        )
        conds: list[str] = []
        if start is not None:
            conds.append(f"ts >= TIMESTAMP {_q(start.strftime('%Y-%m-%d %H:%M:%S'))}")
        if end is not None:
            conds.append(f"ts < TIMESTAMP {_q(end.strftime('%Y-%m-%d %H:%M:%S'))}")
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        if dedup:
            sql += (
                " QUALIFY row_number() OVER "
                "(PARTITION BY vehicle_id, ts ORDER BY ingested_at DESC) = 1"
            )
        try:
            return con.execute(sql).df()
        except duckdb.IOException:
            return pd.DataFrame()
    finally:
        if owns_con:
            con.close()
