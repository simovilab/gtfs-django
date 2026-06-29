"""Compact the many tiny per-poll parquet files into one file per partition.

The live poller appends a small (~2 KB) parquet object every cycle, so each
``(day, route)`` partition accumulates thousands of files and reads become
thousands of S3 round-trips. Compaction rewrites each partition into a single
file (deduped by ``(vehicle_id, ts)``) and deletes the originals, preserving
the Hive layout so route/date pruning still works.

Safety:
  - Only partitions strictly before ``before_date`` are touched, so the day the
    poller is actively writing is never compacted out from under it.
  - Only the exact source files captured up front are deleted; files that land
    after the listing survive.
  - The compacted file is written *before* the sources are deleted. If a delete
    partially fails, the compacted file and leftover sources coexist
    harmlessly -- read-time dedup collapses the overlap, so counts stay correct
    (the partition is just not yet smaller).
"""

from __future__ import annotations

import datetime as dt
import os
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

import duckdb

from .config import S3Config
from .s3_writer import _PART_RE, _q, connect
from .schema import COMPRESSION, DATA_COLUMNS, s3_base_uri


@dataclass(frozen=True)
class CompactionResult:
    partitions_compacted: int
    partitions_skipped: int
    files_removed: int
    rows_written: int
    dry_run: bool


def _is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def _split_s3_uri(uri: str) -> tuple[str, str]:
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    return bucket, key


def _s3_client(config: S3Config):
    import boto3
    from botocore.config import Config as BotoConfig

    scheme = "https" if config.use_ssl else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{config.endpoint}",
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region,
        config=BotoConfig(
            s3={"addressing_style": "path"}, signature_version="s3v4"
        ),
    )


def _delete_files(files: Sequence[str], config: Optional[S3Config]) -> int:
    """Delete a batch of files (local paths or ``s3://`` URIs). Returns count."""
    removed = 0
    local = [f for f in files if not _is_s3(f)]
    for path in local:
        try:
            os.remove(path)
            removed += 1
        except FileNotFoundError:
            pass

    s3_files = [f for f in files if _is_s3(f)]
    if s3_files:
        client = _s3_client(config or S3Config.from_env())
        by_bucket: dict[str, list[str]] = defaultdict(list)
        for uri in s3_files:
            bucket, key = _split_s3_uri(uri)
            by_bucket[bucket].append(key)
        for bucket, keys in by_bucket.items():
            for i in range(0, len(keys), 1000):
                chunk = keys[i : i + 1000]
                client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": k} for k in chunk]},
                )
                removed += len(chunk)
    return removed


def compact(
    base_uri: Optional[str] = None,
    *,
    route_ids: Optional[Sequence[str]] = None,
    before_date: Optional[dt.date] = None,
    min_files: int = 2,
    dedup: bool = True,
    dry_run: bool = False,
    config: Optional[S3Config] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> CompactionResult:
    """Merge small files within each ``(day, route)`` partition into one file.

    ``before_date`` (default: today UTC) is an exclusive upper bound on the
    partition day -- the actively-written day is left alone. Only partitions
    with at least ``min_files`` files are rewritten. ``dedup`` collapses
    duplicate ``(vehicle_id, ts)`` rows (keeping latest ``ingested_at``).
    """
    base_uri = base_uri or s3_base_uri()
    if before_date is None:
        before_date = dt.datetime.now(dt.timezone.utc).date()
    route_set = {str(r) for r in route_ids} if route_ids else None
    owns_con = con is None
    con = con or connect(base_uri, config)

    def _say(msg: str) -> None:
        if progress is not None:
            progress(msg)

    try:
        glob = f"{base_uri}/**/*.parquet"
        try:
            paths = [
                row[0]
                for row in con.execute(f"SELECT file FROM glob({_q(glob)})").fetchall()
            ]
        except duckdb.IOException:
            paths = []

        # Group eligible files by their partition sub-path.
        groups: dict[str, list[str]] = defaultdict(list)
        for path in paths:
            m = _PART_RE.search(path)
            if not m:
                continue
            if route_set is not None and m.group("route_id") not in route_set:
                continue
            day = dt.date(
                int(m.group("year")), int(m.group("month")), int(m.group("day"))
            )
            if day >= before_date:
                continue
            groups[m.group(0)].append(path)  # m.group(0) ends with "/"

        compacted = skipped = files_removed = rows_written = 0
        for relpath, files in sorted(groups.items()):
            if len(files) < min_files:
                skipped += 1
                continue

            if dry_run:
                _say(f"[dry-run] {relpath} -> 1 file (from {len(files)})")
                compacted += 1
                files_removed += len(files)
                continue

            out_uri = f"{base_uri}/{relpath}part-{uuid.uuid4().hex}.parquet"
            cols = ", ".join(DATA_COLUMNS)
            file_list = "[" + ", ".join(_q(f) for f in files) + "]"
            select = (
                f"SELECT {cols} FROM read_parquet({file_list}, "
                "hive_partitioning=true, union_by_name=true)"
            )
            if dedup:
                select += (
                    " QUALIFY row_number() OVER "
                    "(PARTITION BY vehicle_id, ts ORDER BY ingested_at DESC) = 1"
                )
            copy_sql = (
                f"COPY ({select}) TO {_q(out_uri)} "
                f"(FORMAT PARQUET, COMPRESSION {_q(COMPRESSION)})"
            )
            result = con.execute(copy_sql).fetchone()
            rows = int(result[0]) if result else 0

            files_removed += _delete_files(files, config)
            compacted += 1
            rows_written += rows
            _say(f"{relpath} -> 1 file ({rows} rows, removed {len(files)})")

        return CompactionResult(
            partitions_compacted=compacted,
            partitions_skipped=skipped,
            files_removed=files_removed,
            rows_written=rows_written,
            dry_run=dry_run,
        )
    finally:
        if owns_con:
            con.close()
