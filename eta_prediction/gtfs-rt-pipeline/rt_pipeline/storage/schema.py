"""S3 Hive-partitioned Parquet layout + schema for MBTA VehiclePositions.

Source of truth for the persisted columns is the flat ingest model
``rt_pipeline.models.VehiclePosition``. Partition keys live in the object
path (Hive style) so route-filtered reads prune partitions cheaply.

Layout::

    s3://transit/feeds/mbta/vehicle_positions/
        year=YYYY/month=M/day=D/route_id=<rid>/<uuid>.parquet

Day-level time leaf (not hourly) keeps file counts manageable across MBTA's
many routes; ``route_id`` is the innermost partition so per-route reads touch
only matching prefixes.
"""

from __future__ import annotations

import pandas as pd

# --- Storage layout -------------------------------------------------------
BUCKET = "transit"
BASE_PREFIX = "feeds/mbta/vehicle_positions"
# Order matters: this is the on-disk partition nesting order.
PARTITION_COLUMNS = ("year", "month", "day", "route_id")
COMPRESSION = "zstd"  # better ratio than snappy for cold S3 storage

# Columns persisted inside each Parquet file (mirrors the flat ingest model).
# route_id + year/month/day are encoded in the Hive path, not duplicated here.
DATA_COLUMNS = (
    "feed_name",
    "vehicle_id",
    "ts",
    "lat",
    "lon",
    "bearing",
    "speed",
    "trip_id",
    "current_stop_sequence",
    "current_status",  # VehicleStopStatus name (e.g. STOPPED_AT); null on old data
    "stop_id",         # stop the status refers to; null on old data
    "ingested_at",
)

# All columns the writer needs as input (data + the route partition key).
REQUIRED_INPUT_COLUMNS = (*DATA_COLUMNS, "route_id")


def s3_base_uri(bucket: str = BUCKET, prefix: str = BASE_PREFIX) -> str:
    """Return the canonical ``s3://`` base URI for the dataset."""
    return f"s3://{bucket}/{prefix}"


def add_partition_columns(df: pd.DataFrame, ts_col: str = "ts") -> pd.DataFrame:
    """Return a copy of ``df`` with year/month/day derived from ``ts_col``.

    Immutable: never mutates the input frame.
    """
    out = df.copy()
    ts = pd.to_datetime(out[ts_col], utc=True)
    out["year"] = ts.dt.year.astype("int64")
    out["month"] = ts.dt.month.astype("int64")
    out["day"] = ts.dt.day.astype("int64")
    return out


def partition_relpath(year: int, month: int, day: int, route_id: str) -> str:
    """Hive partition sub-path for a single (date, route) cell."""
    return f"year={year}/month={month}/day={day}/route_id={route_id}"


def missing_columns(df: pd.DataFrame) -> list[str]:
    """Columns required by the writer that are absent from ``df``."""
    return [c for c in REQUIRED_INPUT_COLUMNS if c not in df.columns]
