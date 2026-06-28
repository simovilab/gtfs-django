"""Partition index for the VehiclePositions dataset.

Rather than maintaining a hand-written manifest that can drift from reality,
the index is derived by querying the partition columns directly via DuckDB.
This lists which (date, route) partitions exist and how many rows each holds —
enough to drive route discovery and date-range planning at dataset-build time.
"""

from __future__ import annotations

from typing import Optional

import duckdb
import pandas as pd

from .s3_writer import connect
from .schema import s3_base_uri


def list_partitions(
    base_uri: Optional[str] = None,
    *,
    config=None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> pd.DataFrame:
    """Return a frame of (year, month, day, route_id, rows) for all partitions.

    Empty frame with the expected columns when the dataset has no files yet.
    """
    base_uri = base_uri or s3_base_uri()
    glob = f"{base_uri}/**/*.parquet"
    cols = ["year", "month", "day", "route_id", "rows"]
    owns_con = con is None
    con = con or connect(base_uri, config)
    try:
        sql = (
            "SELECT year, month, day, route_id, COUNT(*) AS rows "
            f"FROM read_parquet('{glob}', hive_partitioning=true, union_by_name=true) "
            "GROUP BY year, month, day, route_id "
            "ORDER BY year, month, day, route_id"
        )
        try:
            return con.execute(sql).df()
        except duckdb.IOException:
            return pd.DataFrame(columns=cols)
    finally:
        if owns_con:
            con.close()


def available_routes(
    base_uri: Optional[str] = None,
    *,
    config=None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> list[str]:
    """Distinct route_ids present in the dataset."""
    parts = list_partitions(base_uri, config=config, con=con)
    if parts.empty:
        return []
    return sorted(parts["route_id"].astype(str).unique().tolist())
