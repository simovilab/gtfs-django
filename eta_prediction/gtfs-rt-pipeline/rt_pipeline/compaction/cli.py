#!/usr/bin/env python3
"""CLI entrypoint for the compaction package.

    python -m rt_pipeline.compaction.cli                  # compact everything up to today
    python -m rt_pipeline.compaction.cli --dry-run         # show what would happen, touch nothing
    python -m rt_pipeline.compaction.cli --feed bucr_navsat
    python -m rt_pipeline.compaction.cli --since 2026-07-01 --until 2026-07-31  # bounded backfill
    python -m rt_pipeline.compaction.cli --alias simovilab --bucket transit
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys

from . import run_compaction
from .feeds import FEEDS


def _parse_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date {value!r}") from exc


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--alias", default="simovilab", help="mc alias (default: simovilab)")
    p.add_argument("--bucket", default="transit", help="bucket name (default: transit)")
    p.add_argument(
        "--endpoint",
        default=None,
        help="S3 endpoint host (e.g. data.simovilab.org); overrides env when there is no mc config",
    )
    p.add_argument(
        "--feed",
        action="append",
        choices=[f.name for f in FEEDS],
        help="only process this feed (repeatable); default: all",
    )
    p.add_argument(
        "--workers", type=int, default=12, help="parallel units to compact at once (default: 12)"
    )
    p.add_argument("--dry-run", action="store_true", help="show the plan, change nothing")
    p.add_argument(
        "--since", type=_parse_date, default=None, help="ISO date, inclusive lower bound"
    )
    p.add_argument(
        "--until", type=_parse_date, default=None, help="ISO date, inclusive upper bound"
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    summary = run_compaction(
        feeds=args.feed,
        dry_run=args.dry_run,
        since=args.since,
        until=args.until,
        alias=args.alias,
        bucket=args.bucket,
        endpoint=args.endpoint,
        workers=args.workers,
    )

    mode = "DRY-RUN" if summary["dry_run"] else "LIVE"
    print(f"[{mode}] today(UTC)={summary['today_utc']}", flush=True)

    for name, feed_summary in summary["feeds"].items():
        print(f"\n== feed {name} ==", flush=True)
        for line in feed_summary["details"]:
            print(f"  {line}", flush=True)
        print(
            f"  compacted={feed_summary['compacted']} skipped={feed_summary['skipped']} "
            f"errors={feed_summary['errors']} rows_in={feed_summary['rows_in']} "
            f"rows_out={feed_summary['rows_out']} "
            f"dupes_removed={feed_summary['dupes_removed']}",
            flush=True,
        )

    print("\n--- summary ---")
    print(f"  compacted     : {summary['compacted']}")
    print(f"  recovered     : {summary['recovered']}")
    print(f"  skipped       : {summary['skipped']} (already compacted / empty)")
    print(f"  errors        : {summary['errors']}")
    if not summary["dry_run"]:
        print(f"  rows_in       : {summary['rows_in']}")
        print(f"  rows_out      : {summary['rows_out']}")
        print(f"  dupes_removed : {summary['dupes_removed']}")
    return 1 if summary["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
