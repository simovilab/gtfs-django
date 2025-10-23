from __future__ import annotations

import sys
from pathlib import Path
from datetime import timedelta, UTC

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

# --- Make sibling "feature_engineering" importable (no packaging needed) ---
BASE_DIR = Path(getattr(settings, "BASE_DIR", Path(__file__).resolve().parents[3]))
ETA_PREDICTION_ROOT = BASE_DIR.parent
FEATURE_ENG_ROOT = ETA_PREDICTION_ROOT / "feature_engineering"

if str(ETA_PREDICTION_ROOT) not in sys.path:
    sys.path.insert(0, str(ETA_PREDICTION_ROOT))

try:
    from feature_engineering.dataset_builder import build_training_dataset, save_dataset
    from sch_pipeline.utils import top_routes_by_scheduled_trips
except ImportError as e:
    print(f"ERROR: Failed to import required modules: {e}")
    print(f"ETA_PREDICTION_ROOT: {ETA_PREDICTION_ROOT}")
    print(f"FEATURE_ENG_ROOT: {FEATURE_ENG_ROOT}")
    print(f"sys.path: {sys.path[:3]}")
    raise


class Command(BaseCommand):
    help = "Build a small ETA training dataset for the top-N busiest routes (by scheduled trips)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--top-routes",
            type=int,
            default=3,
            help="Top N routes by scheduled trips (global)"
        )
        parser.add_argument(
            "--days",
            type=int,
            default=14,
            help="Lookback window in days"
        )
        parser.add_argument(
            "--min-observations",
            type=int,
            default=10,
            help="Min observations per stop"
        )
        parser.add_argument(
            "--out",
            type=str,
            default="eta_sample.parquet",
            help="Output parquet path"
        )
        parser.add_argument(
            "--no-weather",
            action="store_true",
            help="Disable weather features"
        )
        parser.add_argument(
            "--route-ids",
            type=str,
            help="Comma-separated route IDs (overrides --top-routes)"
        )

    def handle(self, *args, **opts):
        n = opts["top_routes"]
        days = opts["days"]
        min_obs = opts["min_observations"]
        out = opts["out"]
        attach_weather = not opts["no_weather"]
        manual_routes = opts.get("route_ids")

        # or for a fixed day window matching your data:
        start = timezone.datetime(2025, 10, 8, 0, 0, tzinfo=UTC)
        end   = timezone.datetime(2025, 10, 9, 0, 0, tzinfo=UTC)
        # Determine which routes to use
        if manual_routes:
            route_ids = [r.strip() for r in manual_routes.split(",")]
            self.stdout.write(
                self.style.NOTICE(f"Using manually specified routes: {', '.join(route_ids)}")
            )
        else:
            self.stdout.write(
                self.style.NOTICE(f"Selecting top {n} routes by scheduled trips...")
            )
            try:
                route_ids = top_routes_by_scheduled_trips(n=n)
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"Failed to get top routes: {e}")
                )
                return
            
            if not route_ids:
                self.stdout.write(
                    self.style.WARNING(
                        "No routes found. Is the schedule loaded? "
                        "Check: python manage.py shell -c 'from sch_pipeline.models import Trip; print(Trip.objects.count())'"
                    )
                )
                return
            
            self.stdout.write(
                self.style.SUCCESS(f"Found routes: {', '.join(route_ids)}")
            )

        # Display configuration
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.NOTICE("Configuration:"))
        self.stdout.write(f"  Routes: {', '.join(route_ids)}")
        self.stdout.write(f"  Date range: {start.date()} to {end.date()} ({days} days)")
        self.stdout.write(f"  Min observations/stop: {min_obs}")
        self.stdout.write(f"  Weather features: {'enabled' if attach_weather else 'disabled'}")
        self.stdout.write(f"  Output: {out}")
        self.stdout.write("="*60 + "\n")

        # Build dataset
        try:
            self.stdout.write(self.style.NOTICE("Building dataset..."))
            df = build_training_dataset(
                provider_id=None,
                route_ids=route_ids,
                start_date=start,
                end_date=end,
                min_observations_per_stop=min_obs,
                attach_weather=attach_weather,
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Failed to build dataset: {e}")
            )
            import traceback
            self.stdout.write(traceback.format_exc())
            return

        if df.empty:
            self.stdout.write(
                self.style.WARNING(
                    "Resulting dataset is empty. Possible issues:\n"
                    "  1. No TripUpdate data in the date range\n"
                    "  2. No matching stop_sequences between StopTime and TripUpdate\n"
                    "  3. All data filtered out by min_observations threshold\n"
                    "\nDebug queries:\n"
                    "  - Check TripUpdate count: python manage.py shell -c 'from rt_pipeline.models import TripUpdate; print(TripUpdate.objects.count())'\n"
                    "  - Check date range: python manage.py shell -c 'from rt_pipeline.models import TripUpdate; print(TripUpdate.objects.aggregate(min=Min(\"ts\"), max=Max(\"ts\")))'"
                )
            )
            return

        # Display summary statistics
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("Dataset Summary:"))
        self.stdout.write(f"  Total rows: {len(df):,}")
        self.stdout.write(f"  Unique trips: {df['trip_id'].nunique():,}")
        self.stdout.write(f"  Unique stops: {df['stop_id'].nunique():,}")
        self.stdout.write(f"  Routes: {df['route_id'].nunique()}")
        
        if "delay_seconds" in df.columns:
            delay_stats = df["delay_seconds"].describe()
            self.stdout.write(f"\n  Delay statistics (seconds):")
            self.stdout.write(f"    Mean: {delay_stats['mean']:.1f}")
            self.stdout.write(f"    Median: {delay_stats['50%']:.1f}")
            self.stdout.write(f"    Std: {delay_stats['std']:.1f}")
            self.stdout.write(f"    Min: {delay_stats['min']:.1f}")
            self.stdout.write(f"    Max: {delay_stats['max']:.1f}")
        
        missing = df.isnull().sum()
        if missing.any():
            self.stdout.write(f"\n  Missing values:")
            for col, count in missing[missing > 0].items():
                pct = 100 * count / len(df)
                self.stdout.write(f"    {col}: {count:,} ({pct:.1f}%)")
        
        self.stdout.write("="*60 + "\n")

        # Save dataset
        try:
            save_dataset(df, out)
            self.stdout.write(
                self.style.SUCCESS(f"✓ Successfully saved to {out}")
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Failed to save dataset: {e}")
            )
            import traceback
            self.stdout.write(traceback.format_exc())
            return