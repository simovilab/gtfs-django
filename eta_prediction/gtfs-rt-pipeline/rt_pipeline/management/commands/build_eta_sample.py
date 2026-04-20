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
    from feature_engineering.dataset_builder import build_vp_training_dataset, save_dataset
    from sch_pipeline.utils import top_routes_by_scheduled_trips
except ImportError as e:
    print(f"ERROR: Failed to import required modules: {e}")
    print(f"ETA_PREDICTION_ROOT: {ETA_PREDICTION_ROOT}")
    print(f"FEATURE_ENG_ROOT: {FEATURE_ENG_ROOT}")
    print(f"sys.path: {sys.path[:3]}")
    raise


class Command(BaseCommand):
    help = "Build ETA training dataset from VehiclePosition data for the top-N busiest routes."

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
            "--distance-threshold",
            type=float,
            default=50.0,
            help="Distance threshold (meters) to consider vehicle 'arrived' at stop"
        )
        parser.add_argument(
            "--max-stops-ahead",
            type=int,
            default=5,
            help="Maximum number of upcoming stops to include per VP"
        )
        parser.add_argument(
            "--vp-sample-interval",
            type=int,
            default=30,
            help="Sample VPs every N seconds per vehicle (0=no sampling, use all VPs)"
        )
        parser.add_argument(
            "--out",
            type=str,
            default="eta_vp_sample.parquet",
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
        parser.add_argument(
            "--start-date",
            type=str,
            help="Start date (YYYY-MM-DD format, overrides --days)"
        )
        parser.add_argument(
            "--end-date",
            type=str,
            help="End date (YYYY-MM-DD format, overrides --days)"
        )

    def handle(self, *args, **opts):
        n = opts["top_routes"]
        days = opts["days"]
        min_obs = opts["min_observations"]
        distance_threshold = opts["distance_threshold"]
        max_stops_ahead = opts["max_stops_ahead"]
        # vp_sample_interval = opts["vp_sample_interval"]
        out = "../datasets/" + opts["out"]
        attach_weather = not opts["no_weather"]
        manual_routes = opts.get("route_ids")
        start_date_str = opts.get("start_date")
        end_date_str = opts.get("end_date")

        # Determine date range
        if start_date_str and end_date_str:
            try:
                start = timezone.datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=UTC)
                end = timezone.datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=UTC)
            except ValueError as e:
                self.stdout.write(
                    self.style.ERROR(f"Invalid date format: {e}. Use YYYY-MM-DD")
                )
                return
        else:
            # Default: use fixed date range or calculate from --days
            # For testing, using fixed dates:
            start = timezone.datetime(2025, 10, 8, 0, 0, tzinfo=UTC)
            end = timezone.datetime(2025, 10, 9, 0, 0, tzinfo=UTC)
            # Or calculate from days:
            # end = timezone.now()
            # start = end - timedelta(days=days)
        
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
        self.stdout.write(f"  Date range: {start.date()} to {end.date()}")
        self.stdout.write(f"  Distance threshold: {distance_threshold}m")
        self.stdout.write(f"  Max stops ahead: {max_stops_ahead}")
        # # self.stdout.write(f"  VP sample interval: {vp_sample_interval}s ({'all VPs' if vp_sample_interval == 0 else 'sampled'})")
        self.stdout.write(f"  Min observations/stop: {min_obs}")
        self.stdout.write(f"  Weather features: {'enabled' if attach_weather else 'disabled'}")
        self.stdout.write(f"  Output: {out}")
        self.stdout.write("="*60 + "\n")

        # Check for VehiclePosition data
        from rt_pipeline.models import VehiclePosition
        vp_count = VehiclePosition.objects.filter(
            ts__gte=start,
            ts__lt=end
        ).count()
        
        if vp_count == 0:
            self.stdout.write(
                self.style.WARNING(
                    f"No VehiclePosition data found in date range {start.date()} to {end.date()}\n"
                    "Check data availability:\n"
                    "  python manage.py shell -c 'from rt_pipeline.models import VehiclePosition; "
                    "from django.db.models import Min, Max; "
                    "print(VehiclePosition.objects.aggregate(min=Min(\"ts\"), max=Max(\"ts\")))'"
                )
            )
            return
        else:
            self.stdout.write(
                self.style.SUCCESS(f"Found {vp_count:,} VehiclePosition records in date range")
            )

        # Build dataset
        try:
            self.stdout.write(self.style.NOTICE("\nBuilding dataset..."))
            df = build_vp_training_dataset(
                route_ids=route_ids,
                start_date=start,
                end_date=end,
                distance_threshold=distance_threshold,
                max_stops_ahead=max_stops_ahead,
                # min_observations_per_stop=min_obs,
                # vp_sample_interval_seconds=vp_sample_interval,
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
                    "  1. No VehiclePosition data in the date range\n"
                    "  2. VPs not matching any trips with stop sequences\n"
                    "  3. Vehicles never came close enough to stops (try increasing --distance-threshold)\n"
                    "  4. All data filtered out by --min-observations threshold\n"
                    "  5. No future VPs available to detect arrivals (incomplete trips)\n"
                    "\nDebug queries:\n"
                    "  - Check VP count: python manage.py shell -c 'from rt_pipeline.models import VehiclePosition; print(VehiclePosition.objects.count())'\n"
                    "  - Check date range: python manage.py shell -c 'from rt_pipeline.models import VehiclePosition; from django.db.models import Min, Max; print(VehiclePosition.objects.aggregate(min=Min(\"ts\"), max=Max(\"ts\")))'\n"
                    "  - Check StopTime data: python manage.py shell -c 'from sch_pipeline.models import StopTime, Stop; print(f\"StopTimes: {StopTime.objects.count()}, Stops with coords: {Stop.objects.exclude(stop_lat__isnull=True).count()}\")'\n"
                    "\nTry adjusting parameters:\n"
                    "  - Increase --distance-threshold (current: {})m\n"
                    "  - Reduce --min-observations (current: {})\n"
                    "  - Increase --max-stops-ahead (current: {})\n"
                    "  - Set --vp-sample-interval to 0 to use all VPs".format(
                        distance_threshold, min_obs, max_stops_ahead
                    )
                )
            )
            return

        # Display summary statistics
        self.stdout.write("\n" + "="*60)
        self.stdout.write(self.style.SUCCESS("Dataset Summary:"))
        self.stdout.write(f"  Total rows: {len(df):,}")
        self.stdout.write(f"  Unique trips: {df['trip_id'].nunique():,}")
        self.stdout.write(f"  Unique routes: {df['route_id'].nunique()}")
        self.stdout.write(f"  Unique vehicles: {df['vehicle_id'].nunique():,}")
        self.stdout.write(f"  Unique stops: {df['stop_id'].nunique():,}")
        
        if "time_to_arrival_seconds" in df.columns:
            tta_stats = df["time_to_arrival_seconds"].describe()
            self.stdout.write(f"\n  Time-to-arrival statistics:")
            self.stdout.write(f"    Mean: {tta_stats['mean']:.1f}s ({tta_stats['mean']/60:.1f} min)")
            self.stdout.write(f"    Median: {tta_stats['50%']:.1f}s ({tta_stats['50%']/60:.1f} min)")
            self.stdout.write(f"    Std: {tta_stats['std']:.1f}s")
            self.stdout.write(f"    Min: {tta_stats['min']:.1f}s")
            self.stdout.write(f"    Max: {tta_stats['max']:.1f}s ({tta_stats['max']/60:.1f} min)")
        
        if "distance_to_stop" in df.columns:
            dist_stats = df["distance_to_stop"].describe()
            self.stdout.write(f"\n  Distance-to-stop statistics:")
            self.stdout.write(f"    Mean: {dist_stats['mean']:.1f}m")
            self.stdout.write(f"    Median: {dist_stats['50%']:.1f}m")
            self.stdout.write(f"    Min: {dist_stats['min']:.1f}m")
            self.stdout.write(f"    Max: {dist_stats['max']:.1f}m")
        
        if "current_speed_kmh" in df.columns:
            speed_stats = df[df["current_speed_kmh"] > 0]["current_speed_kmh"].describe()
            if not speed_stats.empty:
                self.stdout.write(f"\n  Speed statistics (km/h):")
                self.stdout.write(f"    Mean: {speed_stats['mean']:.1f}")
                self.stdout.write(f"    Median: {speed_stats['50%']:.1f}")
        
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
            
            # Provide guidance on next steps
            self.stdout.write("\n" + self.style.NOTICE("Next steps:"))
            self.stdout.write("  1. Inspect the dataset: ")
            self.stdout.write(f"     import pandas as pd; df = pd.read_parquet('{out}'); df.head()")
            self.stdout.write("  2. Check feature distributions and correlations")
            self.stdout.write("  3. Train a model predicting 'time_to_arrival_seconds' from:")
            self.stdout.write("     - distance_to_stop")
            self.stdout.write("     - current_speed_kmh")
            self.stdout.write("     - temporal features (hour, is_peak_hour, etc.)")
            self.stdout.write("     - operational features (headway_seconds)")
            self.stdout.write("     - weather features (if enabled)")
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Failed to save dataset: {e}")
            )
            import traceback
            self.stdout.write(traceback.format_exc())
            return