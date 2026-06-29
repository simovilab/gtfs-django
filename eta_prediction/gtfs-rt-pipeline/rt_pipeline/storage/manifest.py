"""Partition index for the VehiclePositions dataset.

Rather than maintaining a hand-written manifest that can drift from reality,
the index is derived by querying object paths directly via DuckDB. This lists
which (date, route) partitions exist and how many rows each holds — enough to
drive route discovery and date-range planning at dataset-build time.

The dataset is many thousands of tiny per-poll files, so this avoids scanning
file *data*: ``available_routes`` parses partitions from the cheap ``glob()``
LIST, and ``list_partitions`` reads only parquet footers (``num_rows``) rather
than every data column.
"""

from __future__ import annotations

import re
from typing import Optional

import duckdb
import pandas as pd

from .s3_writer import connect
from .schema import s3_base_uri

_PART_RE = re.compile(
    r"year=(?P<year>\d+)/month=(?P<month>\d+)/day=(?P<day>\d+)/"
    r"route_id=(?P<route_id>[^/]+)/"
)


def _glob_paths(con: duckdb.DuckDBPyConnection, base_uri: str) -> list[str]:
    glob = f"{base_uri}/**/*.parquet"
    try:
        return [row[0] for row in con.execute(
            f"SELECT file FROM glob('{glob}')"
        ).fetchall()]
    except duckdb.IOException:
        return []


def list_partitions(
    base_uri: Optional[str] = None,
    *,
    config=None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> pd.DataFrame:
    """Return a frame of (year, month, day, route_id, rows) for all partitions.

    Row counts come from parquet footers (no data scan). Empty frame with the
    expected columns when the dataset has no files yet.
    """
    base_uri = base_uri or s3_base_uri()
    glob = f"{base_uri}/**/*.parquet"
    cols = ["year", "month", "day", "route_id", "rows"]
    owns_con = con is None
    con = con or connect(base_uri, config)
    try:
        try:
            meta = con.execute(
                f"SELECT file_name, num_rows FROM parquet_file_metadata('{glob}')"
            ).df()
        except duckdb.IOException:
            return pd.DataFrame(columns=cols)
        if meta.empty:
            return pd.DataFrame(columns=cols)

        parts = meta["file_name"].str.extract(_PART_RE)
        parts["rows"] = meta["num_rows"].to_numpy()
        parts = parts.dropna(subset=["year", "month", "day", "route_id"])
        if parts.empty:
            return pd.DataFrame(columns=cols)
        for c in ("year", "month", "day"):
            parts[c] = parts[c].astype("int64")
        grouped = (
            parts.groupby(["year", "month", "day", "route_id"], as_index=False)["rows"]
            .sum()
            .sort_values(["year", "month", "day", "route_id"])
            .reset_index(drop=True)
        )
        return grouped
    finally:
        if owns_con:
            con.close()


def available_routes(
    base_uri: Optional[str] = None,
    *,
    config=None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> list[str]:
    """Distinct route_ids present in the dataset (derived from paths, no scan)."""
    base_uri = base_uri or s3_base_uri()
    owns_con = con is None
    con = con or connect(base_uri, config)
    try:
        routes: set[str] = set()
        for path in _glob_paths(con, base_uri):
            m = _PART_RE.search(path)
            if m:
                routes.add(m.group("route_id"))
        return sorted(routes)
    finally:
        if owns_con:
            con.close()
