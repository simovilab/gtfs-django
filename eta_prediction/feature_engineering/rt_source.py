"""RT VehiclePosition source for the dataset builder.

Reads collected VehiclePositions from the S3 Hive-partitioned Parquet store
(``rt_pipeline.storage``) instead of the live Postgres ORM. Returns a frame
with exactly the columns the dataset builder expects, so the rest of the
pipeline is unchanged. Route filtering pushes down to the ``route_id``
partition; date filtering pushes down to the year/month/day partitions.
"""

from __future__ import annotations

import datetime as dt
from typing import List, Optional

import pandas as pd

from rt_pipeline.storage import read_vehicle_positions

# The exact schema the legacy ORM path produced (order matters downstream).
LEGACY_COLUMNS = ["trip_id", "vehicle_id", "ts", "lat", "lon", "bearing", "speed"]


def fetch_vehicle_positions(
    route_ids: Optional[List[str]] = None,
    start: Optional[dt.datetime] = None,
    end: Optional[dt.datetime] = None,
    base_uri: Optional[str] = None,
    *,
    config=None,
) -> pd.DataFrame:
    """Fetch VehiclePositions from S3, shaped like the old ORM query.

    Drops rows with null ``trip_id``/``lat``/``lon`` and sorts by
    ``(trip_id, ts)`` — matching the legacy ``exclude(...).order_by(...)``.
    Returns an empty frame with ``LEGACY_COLUMNS`` when nothing matches.
    """
    df = read_vehicle_positions(
        route_ids=route_ids,
        start=start,
        end=end,
        base_uri=base_uri,
        config=config,
    )
    if df.empty:
        return pd.DataFrame(columns=LEGACY_COLUMNS)

    df = df[df["trip_id"].notna() & df["lat"].notna() & df["lon"].notna()]
    if df.empty:
        return pd.DataFrame(columns=LEGACY_COLUMNS)

    return (
        df[LEGACY_COLUMNS]
        .sort_values(["trip_id", "ts"])
        .reset_index(drop=True)
    )
