"""Materialize a local, compacted copy of the S3 VehiclePosition store.

The live poller writes a tiny (~2 KB) parquet object every cycle, so each
``(day, route)`` partition on S3 holds thousands of files and dataset builds
spend most of their time on S3 round-trips. This script reads the relevant
slice from S3 **once** (deduped on ``(vehicle_id, ts)``), and writes it back as
a local Hive-partitioned parquet store with one file per partition. The dataset
builder then reads from that local store instead of S3.

It never mutates S3 -- it only reads. The local cache lives under
``eta_prediction/datasets/vp_cache/`` (git-ignored, regenerable).

Standalone usage (reads gtfs-rt-pipeline/.env for AWS_* credentials):

    uv run python feature_engineering/vp_cache.py \
        --route-ids 111,32,Blue --start 2026-06-28 --end 2026-06-29

    # then build from the local cache:
    cd gtfs-rt-pipeline
    python manage.py build_eta_sample --route-ids 111,32,Blue \
        --start-date 2026-06-28 --end-date 2026-06-29 \
        --vp-source-uri ../datasets/vp_cache
"""

from __future__ import annotations

import datetime as dt
import shutil
import sys
from pathlib import Path
from typing import Callable, List, Optional, Sequence

# --- Make the Django-free storage layer importable without bootstrapping Django.
_HERE = Path(__file__).resolve()
_ETA_ROOT = _HERE.parents[1]  # eta_prediction/
_RT_PIPELINE = _ETA_ROOT / "gtfs-rt-pipeline"
if str(_RT_PIPELINE) not in sys.path:
    sys.path.insert(0, str(_RT_PIPELINE))

from rt_pipeline.storage import (  # noqa: E402
    read_vehicle_positions,
    write_vehicle_positions,
)
from rt_pipeline.storage.schema import s3_base_uri  # noqa: E402

DEFAULT_CACHE_DIR = _ETA_ROOT / "datasets" / "vp_cache"


def _utc_midnight(d: dt.date) -> dt.datetime:
    return dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc)


def _iter_days(
    start: dt.datetime, end: dt.datetime
) -> List[tuple[dt.datetime, dt.datetime]]:
    """Half-open daily windows covering ``[start, end)`` to bound memory."""
    windows: list[tuple[dt.datetime, dt.datetime]] = []
    day = start.date()
    last = (end - dt.timedelta(microseconds=1)).date()
    while day <= last:
        w_start = max(start, _utc_midnight(day))
        w_end = min(end, _utc_midnight(day) + dt.timedelta(days=1))
        windows.append((w_start, w_end))
        day += dt.timedelta(days=1)
    return windows


def materialize_local_cache(
    source_uri: str,
    dest_dir: str,
    *,
    route_ids: Optional[Sequence[str]] = None,
    start: Optional[dt.datetime] = None,
    end: Optional[dt.datetime] = None,
    refresh: bool = True,
    progress: Optional[Callable[[str], None]] = None,
) -> int:
    """Read VPs from ``source_uri`` (S3 or local) into a local partitioned store.

    Reuses ``read_vehicle_positions`` (deduped, partition-pruned) and
    ``write_vehicle_positions`` (Hive ``COPY ... PARTITION_BY``). When a date
    range is given, reads day-by-day so a wide pull never loads the whole
    dataset into memory at once. Returns total rows written.
    """
    dest = Path(dest_dir)
    if refresh and dest.exists():
        shutil.rmtree(dest)

    def _say(msg: str) -> None:
        if progress is not None:
            progress(msg)

    windows = (
        _iter_days(start, end)
        if (start is not None and end is not None)
        else [(start, end)]
    )

    total = 0
    for w_start, w_end in windows:
        df = read_vehicle_positions(
            route_ids=route_ids, start=w_start, end=w_end, base_uri=source_uri
        )
        n = write_vehicle_positions(df, str(dest))
        total += n
        if w_start is not None:
            _say(f"{w_start.date()}: {n:,} rows")
    return total


def _parse_date(value: str) -> dt.datetime:
    return dt.datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse
    import os

    from dotenv import load_dotenv

    # Load AWS_* / S3_VP_BASE_URI from the pipeline's .env (never hardcoded).
    load_dotenv(_RT_PIPELINE / ".env")

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--source-uri",
        default=os.environ.get("S3_VP_BASE_URI") or s3_base_uri(),
        help="S3 base URI to read from (default: $S3_VP_BASE_URI or storage default)",
    )
    parser.add_argument(
        "--dest",
        default=str(DEFAULT_CACHE_DIR),
        help=f"Local cache dir to write (default: {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument("--route-ids", help="Comma-separated route IDs (default: all)")
    parser.add_argument("--start", type=_parse_date, help="Start date YYYY-MM-DD (UTC)")
    parser.add_argument("--end", type=_parse_date, help="End date YYYY-MM-DD (UTC, exclusive)")
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Append to the cache instead of rebuilding it from scratch",
    )
    args = parser.parse_args(argv)

    route_ids = (
        [r.strip() for r in args.route_ids.split(",") if r.strip()]
        if args.route_ids
        else None
    )

    print(f"Source (S3, read-only): {args.source_uri}")
    print(f"Dest (local cache):     {args.dest}")
    if route_ids:
        print(f"Routes:                 {', '.join(route_ids)}")

    total = materialize_local_cache(
        args.source_uri,
        args.dest,
        route_ids=route_ids,
        start=args.start,
        end=args.end,
        refresh=not args.no_refresh,
        progress=lambda msg: print(f"  {msg}"),
    )
    print(f"Done: wrote {total:,} rows to {args.dest}")
    if total == 0:
        print("(No rows matched. Check routes/date range and that the poller has data.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
