"""Sample-row builders and a bare parquet writer for compaction tests.

Deliberately independent of `rt_pipeline.storage` -- the compaction package
only needs a Parquet file with the right columns to exist at a path; it does
not go through the collectors' write path.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import pandas as pd


def write_parquet(root: Path, rel_path: str, df: pd.DataFrame) -> None:
    """Write `df` as a standalone parquet file at `root/rel_path`."""
    target = root / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.register("df", df)
        con.execute(f"COPY df TO '{target}' (FORMAT PARQUET)")
    finally:
        con.close()


def mbta_rows(
    vehicle_ids: list[str],
    ts: dt.datetime,
    ingested_at: dt.datetime,
    route_id: str = "39",
    include_route_id: bool = False,
) -> pd.DataFrame:
    """Rows matching `rt_pipeline.storage.schema.DATA_COLUMNS`.

    `include_route_id=True` mirrors the hourly-staging spool (route_id is a
    real data column there, per the plan's spool schema); leave it False for
    the legacy in-place layout, where route_id lives only in the leaf's path.
    """
    n = len(vehicle_ids)
    data = {
        "feed_name": ["mbta"] * n,
        "vehicle_id": list(vehicle_ids),
        "ts": [ts] * n,
        "lat": [42.0 + i * 0.001 for i in range(n)],
        "lon": [-71.0 - i * 0.001 for i in range(n)],
        "bearing": [90.0] * n,
        "speed": [10.0] * n,
        "trip_id": [f"trip-{v}" for v in vehicle_ids],
        "current_stop_sequence": [1] * n,
        "current_status": ["IN_TRANSIT_TO"] * n,
        "stop_id": ["stop-1"] * n,
        "ingested_at": [ingested_at] * n,
    }
    if include_route_id:
        data["route_id"] = [route_id] * n
    return pd.DataFrame(data)


def bucr_rows(
    plates: list[str],
    cr_datetime: str,
    ingested_at_utc: dt.datetime,
) -> pd.DataFrame:
    n = len(plates)
    return pd.DataFrame(
        {
            "plate_number": list(plates),
            "cr_datetime": [cr_datetime] * n,
            "lat": [9.9] * n,
            "lon": [-84.1] * n,
            "speed_kmh": [30.0] * n,
            "odometer_km": [1000.0] * n,
            "estado": ["movimiento"] * n,
            "lugar": ["San Jose"] * n,
            "ingested_at_utc": [ingested_at_utc] * n,
        }
    )
