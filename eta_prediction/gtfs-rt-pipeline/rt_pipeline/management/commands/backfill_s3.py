"""Replay Postgres VehiclePosition rows into the S3 Hive-Parquet store.

Iterates day-by-day to bound memory. NOTE: not idempotent — re-running over a
range that was already backfilled appends duplicate rows (files get uuid
names). Run a given range once, or clear the target partitions first.

Examples:
    python manage.py backfill_s3 --start 2025-01-01 --end 2025-01-08
    python manage.py backfill_s3 --start 2025-01-01 --end 2025-02-01 \
        --route-ids Green-D,Green-E --dry-run
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from rt_pipeline.models import VehiclePosition
from rt_pipeline.storage import write_vehicle_positions

VP_FIELDS = [
    "feed_name",
    "vehicle_id",
    "ts",
    "lat",
    "lon",
    "bearing",
    "speed",
    "route_id",
    "trip_id",
    "current_stop_sequence",
    "ingested_at",
]


def _parse_dt(value: str) -> dt.datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(value, fmt).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
    raise CommandError(f"Unrecognized date/time (use YYYY-MM-DD[ HH:MM:SS]): {value!r}")


class Command(BaseCommand):
    help = "Replay Postgres VehiclePositions into the S3 Hive-Parquet store."

    def add_arguments(self, parser):
        parser.add_argument("--start", required=True, help="UTC, inclusive")
        parser.add_argument("--end", required=True, help="UTC, exclusive")
        parser.add_argument("--route-ids", help="Comma-separated route_ids")
        parser.add_argument("--base-uri", help="Override target base URI")
        parser.add_argument(
            "--dry-run", action="store_true", help="Count rows, write nothing"
        )

    def handle(self, *args, **opts):
        start = _parse_dt(opts["start"])
        end = _parse_dt(opts["end"])
        if end <= start:
            raise CommandError("--end must be after --start")

        routes = (
            [r.strip() for r in opts["route_ids"].split(",")]
            if opts.get("route_ids")
            else None
        )
        base_uri = opts.get("base_uri") or (getattr(settings, "S3_VP_BASE_URI", "") or None)
        dry = opts["dry_run"]

        if not dry:
            self.stdout.write(
                self.style.WARNING(
                    "Backfill is not idempotent; ensure this range was not "
                    "already written (duplicate rows otherwise)."
                )
            )

        total = 0
        day = dt.datetime(start.year, start.month, start.day, tzinfo=dt.timezone.utc)
        while day < end:
            nxt = day + dt.timedelta(days=1)
            lo, hi = max(day, start), min(nxt, end)
            qs = VehiclePosition.objects.filter(ts__gte=lo, ts__lt=hi)
            if routes:
                qs = qs.filter(route_id__in=routes)
            df = pd.DataFrame.from_records(list(qs.values(*VP_FIELDS)))
            n = len(df)
            if n:
                if dry:
                    self.stdout.write(f"[dry-run] {day.date()}: {n} rows")
                else:
                    written = write_vehicle_positions(df, base_uri)
                    total += written
                    self.stdout.write(f"{day.date()}: wrote {written} rows")
            day = nxt

        verb = "would write" if dry else "wrote"
        self.stdout.write(self.style.SUCCESS(f"Backfill done: {verb} {total} rows"))
