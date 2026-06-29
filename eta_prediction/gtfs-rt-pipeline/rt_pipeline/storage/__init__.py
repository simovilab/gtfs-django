"""S3 Hive-partitioned Parquet storage for MBTA VehiclePositions.

Public API:
    write_vehicle_positions(df, base_uri=None)   -> rows written
    read_vehicle_positions(route_ids, start, end, base_uri=None) -> DataFrame
    list_partitions(base_uri=None)               -> partition index
    available_routes(base_uri=None)              -> [route_id, ...]
    compact(base_uri=None, ...)                  -> CompactionResult
"""

from __future__ import annotations

from .compaction import CompactionResult, compact
from .config import S3Config
from .manifest import available_routes, list_partitions
from .s3_writer import connect, read_vehicle_positions, write_vehicle_positions
from .schema import (
    BASE_PREFIX,
    BUCKET,
    COMPRESSION,
    DATA_COLUMNS,
    PARTITION_COLUMNS,
    s3_base_uri,
)

__all__ = [
    "S3Config",
    "write_vehicle_positions",
    "read_vehicle_positions",
    "list_partitions",
    "available_routes",
    "compact",
    "CompactionResult",
    "connect",
    "s3_base_uri",
    "BUCKET",
    "BASE_PREFIX",
    "COMPRESSION",
    "DATA_COLUMNS",
    "PARTITION_COLUMNS",
]
