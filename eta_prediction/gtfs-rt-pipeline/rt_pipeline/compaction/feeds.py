"""Per-feed compaction configuration: layout, sort/dedup keys.

The curated layout and column names here are a contract shared with the
collectors and readers -- MBTA's is documented in full in
``eta_prediction/docs/S3_LAYOUT.md`` and mirrored in
``rt_pipeline/storage/schema.py``. Changing a prefix or dedup key here without
updating those readers breaks the dataset builder.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

# Bucket-relative prefix under which in-flight (staged) files are mirrored
# during a compaction, and the fixed name used for the legacy in-place
# single-file merge (see execution.stage_key_for).
STAGING_PREFIX = "_staging"
STAGING_NAME = "_compacting.parquet"


def output_name(date: dt.date) -> str:
    """Name of a compacted file for a partition: its date, e.g. 2026-07-08.parquet."""
    return f"{date:%Y-%m-%d}.parquet"


@dataclass(frozen=True)
class Feed:
    name: str
    curated_prefix: str
    # Hourly-staging prefix the collector writes to, or None if this feed has
    # no staging flow (compaction then only does the legacy in-place merge).
    staging_prefix: str | None
    sort_col: str  # column to sort rows by inside a compacted file
    route_level: bool  # True -> leaf is .../day=/route_id=; False -> .../day=
    # Natural key: identifies "the same observation" across duplicate writes
    # (retried polls, overlapping staging flushes, re-runs).
    dedup_keys: tuple[str, ...]
    # Tie-break among rows sharing a dedup key: keep the row with the latest
    # value here (i.e. the most recently ingested copy).
    recency_col: str


FEEDS: list[Feed] = [
    Feed(
        "mbta_vp",
        curated_prefix="feeds/mbta/vehicle_positions",
        staging_prefix="feeds/mbta/vehicle_positions_staging",
        sort_col="ts",
        route_level=True,
        dedup_keys=("feed_name", "vehicle_id", "ts"),
        recency_col="ingested_at",
    ),
    Feed(
        "bucr_navsat",
        curated_prefix="feeds/bucr/navsat",
        staging_prefix="feeds/bucr/navsat_staging",
        sort_col="cr_datetime",
        route_level=False,
        dedup_keys=("plate_number", "cr_datetime"),
        recency_col="ingested_at_utc",
    ),
]

FEEDS_BY_NAME: dict[str, Feed] = {f.name: f for f in FEEDS}
