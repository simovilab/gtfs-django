"""Write/read MBTA VehiclePositions as Hive-partitioned Parquet via DuckDB.

DuckDB handles both sides: ``COPY ... PARTITION_BY`` for writes and
``read_parquet(..., hive_partitioning=true)`` for reads, with partition
pruning on ``route_id`` (and year/month/day). The same code path works for a
local directory (tests) or an ``s3://`` URI (MinIO) — only the connection
setup differs.
"""

from __future__ import annotations

import datetime as dt
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


def read_vehicle_positions(
    route_ids: Optional[Sequence[str]] = None,
    start: Optional[dt.datetime] = None,
    end: Optional[dt.datetime] = None,
    base_uri: Optional[str] = None,
    *,
    config: Optional[S3Config] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> pd.DataFrame:
    """Read VehiclePositions, pruning partitions by route and date.

    ``route_ids`` prunes the innermost partition; ``start``/``end`` (UTC,
    half-open ``[start, end)``) prune date partitions and filter ``ts``.
    Returns an empty frame (with no rows) when nothing matches.
    """
    base_uri = base_uri or s3_base_uri()
    glob = f"{base_uri}/**/*.parquet"
    owns_con = con is None
    con = con or connect(base_uri, config)
    try:
        sql = (
            f"SELECT * FROM read_parquet({_q(glob)}, hive_partitioning=true, "
            "union_by_name=true)"
        )
        conds: list[str] = []
        if route_ids:
            ids = ", ".join(_q(str(r)) for r in route_ids)
            conds.append(f"route_id IN ({ids})")
        if start is not None:
            conds.append(f"ts >= TIMESTAMP {_q(start.strftime('%Y-%m-%d %H:%M:%S'))}")
            conds.append(
                f"(year > {start.year} OR (year = {start.year} AND "
                f"(month > {start.month} OR (month = {start.month} AND "
                f"day >= {start.day}))))"
            )
        if end is not None:
            conds.append(f"ts < TIMESTAMP {_q(end.strftime('%Y-%m-%d %H:%M:%S'))}")
            conds.append(
                f"(year < {end.year} OR (year = {end.year} AND "
                f"(month < {end.month} OR (month = {end.month} AND "
                f"day <= {end.day}))))"
            )
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        try:
            return con.execute(sql).df()
        except duckdb.IOException:
            # No files yet (empty/nonexistent dataset) -> empty result.
            return pd.DataFrame()
    finally:
        if owns_con:
            con.close()
