"""Compact the many tiny per-poll parquet files into one file per partition.

The live poller writes a small parquet object every cycle, so each
``(day, route)`` partition fills with thousands of ~2 KB files and dataset
builds spend most of their time on S3 round-trips. This rewrites each eligible
partition into a single deduped file and deletes the originals, preserving the
Hive layout (route/date pruning keeps working).

By default only partitions strictly before today (UTC) are compacted, so the
day the poller is actively writing is left untouched. Run it on a schedule
(e.g. nightly) to keep the dataset cheap to read.

Examples:
    python manage.py compact_s3 --dry-run
    python manage.py compact_s3
    python manage.py compact_s3 --route-ids 111,32,Blue --before-date 2026-06-29
    python manage.py compact_s3 \
        --base-uri s3://transit/feeds/mbta/vehicle_positions_devtest
"""

from __future__ import annotations

import datetime as dt

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from rt_pipeline.storage import compact


class Command(BaseCommand):
    help = "Compact small per-poll parquet files into one file per (day, route) partition."

    def add_arguments(self, parser):
        parser.add_argument(
            "--base-uri",
            type=str,
            help=(
                "S3 base URI (default: settings.S3_VP_BASE_URI or "
                "s3://transit/feeds/mbta/vehicle_positions)"
            ),
        )
        parser.add_argument(
            "--route-ids",
            type=str,
            help="Comma-separated route IDs to limit compaction (default: all)",
        )
        parser.add_argument(
            "--before-date",
            type=str,
            help=(
                "Only compact partitions with day < this date (YYYY-MM-DD). "
                "Default: today (UTC), so the actively-written day is skipped."
            ),
        )
        parser.add_argument(
            "--min-files",
            type=int,
            default=2,
            help="Only compact partitions with at least this many files (default: 2)",
        )
        parser.add_argument(
            "--no-dedup",
            action="store_true",
            help="Keep duplicate (vehicle_id, ts) rows instead of collapsing them",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what would be compacted without writing or deleting",
        )

    def handle(self, *args, **opts):
        base_uri = opts.get("base_uri") or getattr(
            settings, "S3_VP_BASE_URI", ""
        ) or None

        before_date = None
        if opts.get("before_date"):
            try:
                before_date = dt.datetime.strptime(
                    opts["before_date"], "%Y-%m-%d"
                ).date()
            except ValueError as e:
                raise CommandError(f"Invalid --before-date: {e}. Use YYYY-MM-DD")

        route_ids = None
        if opts.get("route_ids"):
            route_ids = [r.strip() for r in opts["route_ids"].split(",") if r.strip()]

        self.stdout.write(
            self.style.NOTICE(
                "Compacting "
                f"{base_uri or 's3 default (transit/feeds/mbta/vehicle_positions)'}"
                + (f" routes={route_ids}" if route_ids else "")
                + (f" before {before_date}" if before_date else " before today (UTC)")
                + (" [dry-run]" if opts["dry_run"] else "")
            )
        )

        result = compact(
            base_uri,
            route_ids=route_ids,
            before_date=before_date,
            min_files=opts["min_files"],
            dedup=not opts["no_dedup"],
            dry_run=opts["dry_run"],
            progress=lambda msg: self.stdout.write(f"  {msg}"),
        )

        verb = "would compact" if result.dry_run else "compacted"
        self.stdout.write(
            self.style.SUCCESS(
                f"Done: {verb} {result.partitions_compacted} partition(s), "
                f"{result.rows_written:,} rows written, "
                f"{result.files_removed:,} file(s) removed, "
                f"{result.partitions_skipped} partition(s) skipped."
            )
        )
