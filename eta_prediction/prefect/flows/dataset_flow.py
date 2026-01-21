"""
Prefect flow for building ETA prediction datasets.

Wraps the existing feature engineering from feature_engineering/dataset_builder.py
and adds Prefect orchestration, artifacts, and notifications.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure repo modules are importable
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.core import Block


def _notify_blocks(block_names: List[str], message: str, logger: Any) -> None:
    """Send notifications via Prefect notification blocks."""
    for block_name in block_names:
        if not block_name:
            continue
        try:
            block = Block.load(block_name)
            notify_fn = getattr(block, "notify", None) or getattr(block, "send", None)
            if callable(notify_fn):
                notify_fn(message)
                logger.info("Sent notification via %s", block_name)
        except Exception as exc:
            logger.warning("Failed to send notification via %s: %s", block_name, exc)


@task(name="build-vp-dataset", retries=1, retry_delay_seconds=30)
def build_vp_dataset_task(
    route_ids: Optional[List[str]],
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    distance_threshold: float,
    max_stops_ahead: int,
    attach_weather: bool,
    use_shapes: bool,
) -> Dict[str, Any]:
    """Build training dataset from VehiclePosition data."""
    logger = get_run_logger()

    # Import here to handle Django setup
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gtfs_rt_pipeline.settings")
    django.setup()

    from feature_engineering.dataset_builder import build_vp_training_dataset

    logger.info(
        "Building dataset: routes=%s, start=%s, end=%s",
        route_ids, start_date, end_date
    )

    df = build_vp_training_dataset(
        route_ids=route_ids,
        start_date=start_date,
        end_date=end_date,
        distance_threshold=distance_threshold,
        max_stops_ahead=max_stops_ahead,
        attach_weather=attach_weather,
        use_shapes=use_shapes,
    )

    if df.empty:
        logger.warning("Dataset build returned empty dataframe")
        return {
            "success": False,
            "n_samples": 0,
            "n_routes": 0,
            "n_trips": 0,
            "dataframe": df,
        }

    stats = {
        "success": True,
        "n_samples": len(df),
        "n_routes": df["route_id"].nunique(),
        "n_trips": df["trip_id"].nunique(),
        "n_stops": df["stop_id"].nunique(),
        "avg_time_to_arrival_sec": df["time_to_arrival_seconds"].mean(),
        "dataframe": df,
    }

    logger.info(
        "Dataset built: %d samples, %d routes, %d trips",
        stats["n_samples"], stats["n_routes"], stats["n_trips"]
    )

    return stats


@task(name="save-dataset")
def save_dataset_task(
    df,
    output_name: str,
    output_dir: str,
    version_suffix: Optional[str] = None,
) -> str:
    """Save dataset to parquet file."""
    logger = get_run_logger()

    from feature_engineering.dataset_builder import save_dataset

    # Generate versioned filename
    if version_suffix is None:
        version_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

    output_path = Path(output_dir) / f"{output_name}_{version_suffix}.parquet"
    save_dataset(df, str(output_path))

    logger.info("Dataset saved to %s", output_path)
    return str(output_path)


@task(name="seed-redis-caches")
def seed_redis_caches_task(
    redis_host: str,
    redis_port: int,
    redis_db: int,
    redis_password: Optional[str],
) -> Dict[str, int]:
    """Seed Redis with route_stops and route_shape caches from GTFS."""
    logger = get_run_logger()
    import json
    import redis

    # Import Django models
    import django
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "gtfs_rt_pipeline.settings")
    django.setup()

    from sch_pipeline.models import StopTime, Stop, Trip, Route

    client = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        decode_responses=True,
    )

    routes_seeded = 0
    stops_seeded = 0

    # Get all routes
    routes = Route.objects.all()
    logger.info("Seeding Redis caches for %d routes", routes.count())

    for route in routes:
        route_id = route.route_id

        # Get stops for this route via trips -> stop_times
        trips = Trip.objects.filter(route_id=route_id).values_list("trip_id", flat=True)
        if not trips:
            continue

        # Get unique stops with coordinates
        stop_times = StopTime.objects.filter(
            trip_id__in=trips[:1]  # Just use first trip's stops
        ).select_related("stop").order_by("stop_sequence")

        stops_data = []
        for st in stop_times:
            if st.stop:
                stops_data.append({
                    "stop_id": st.stop.stop_id,
                    "stop_name": st.stop.stop_name,
                    "lat": float(st.stop.stop_lat) if st.stop.stop_lat else None,
                    "lon": float(st.stop.stop_lon) if st.stop.stop_lon else None,
                    "sequence": st.stop_sequence,
                })
                stops_seeded += 1

        if stops_data:
            cache_key = f"route_stops:{route_id}"
            client.set(cache_key, json.dumps({"stops": stops_data}), ex=86400)  # 24h TTL
            routes_seeded += 1

    logger.info("Seeded %d routes, %d stops", routes_seeded, stops_seeded)
    return {"routes_seeded": routes_seeded, "stops_seeded": stops_seeded}


@task(name="create-dataset-summary")
def create_dataset_summary_task(
    dataset_stats: Dict[str, Any],
    output_path: str,
    build_params: Dict[str, Any],
) -> str:
    """Create a markdown summary artifact for the dataset build."""
    logger = get_run_logger()

    lines = [
        "# Dataset Build Summary",
        "",
        f"**Output Path:** `{output_path}`",
        f"**Timestamp:** {datetime.now().isoformat()}",
        "",
        "## Dataset Statistics",
        "",
        f"- **Samples:** {dataset_stats.get('n_samples', 0):,}",
        f"- **Routes:** {dataset_stats.get('n_routes', 0)}",
        f"- **Trips:** {dataset_stats.get('n_trips', 0):,}",
        f"- **Stops:** {dataset_stats.get('n_stops', 0):,}",
        f"- **Avg Time to Arrival:** {dataset_stats.get('avg_time_to_arrival_sec', 0):.1f} sec",
        "",
        "## Build Parameters",
        "",
    ]

    for key, value in build_params.items():
        lines.append(f"- **{key}:** {value}")

    summary = "\n".join(lines)

    create_markdown_artifact(
        key="dataset-build-summary",
        markdown=summary,
        description="Summary of dataset build process",
    )

    logger.info("Dataset summary artifact created")
    return summary


@flow(name="dataset-build")
def dataset_build_flow(
    output_name: str = "various_dataset",
    output_dir: Optional[str] = None,
    route_ids: Optional[List[str]] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    days_back: int = 7,
    distance_threshold: float = 50.0,
    max_stops_ahead: int = 5,
    attach_weather: bool = False,
    use_shapes: bool = True,
    seed_redis: bool = False,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_db: int = 0,
    redis_password: Optional[str] = None,
    gtfs_path: Optional[str] = None,
    telemetry_source: Optional[str] = None,
    notification_blocks: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Build ETA prediction dataset from telemetry and GTFS data.

    Args:
        output_name: Base name for output parquet file
        output_dir: Directory to save dataset (default: datasets/)
        route_ids: List of routes to include (None = all)
        start_date: Start of date range (default: days_back from now)
        end_date: End of date range (default: now)
        days_back: Days of data to include if start_date not specified
        distance_threshold: Meters to consider "arrived at stop"
        max_stops_ahead: Max future stops per vehicle position
        attach_weather: Whether to fetch weather data
        use_shapes: Whether to use GTFS shapes for progress calculation
        seed_redis: Whether to seed Redis caches after building
        redis_*: Redis connection parameters
        gtfs_path: Path to GTFS data (uses DB if not specified)
        telemetry_source: Telemetry source URL/path
        notification_blocks: Prefect blocks for notifications

    Returns:
        Dictionary with build results and statistics
    """
    logger = get_run_logger()
    notification_blocks = notification_blocks or []

    # Set defaults
    if output_dir is None:
        output_dir = str(REPO_ROOT / "datasets")

    if end_date is None:
        end_date = datetime.now()

    if start_date is None:
        start_date = end_date - timedelta(days=days_back)

    logger.info(
        "Starting dataset build: output=%s, routes=%s, date_range=%s to %s",
        output_name, route_ids, start_date, end_date
    )

    build_params = {
        "output_name": output_name,
        "route_ids": route_ids,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "distance_threshold": distance_threshold,
        "max_stops_ahead": max_stops_ahead,
        "attach_weather": attach_weather,
        "use_shapes": use_shapes,
    }

    try:
        # Build dataset
        dataset_stats = build_vp_dataset_task(
            route_ids=route_ids,
            start_date=start_date,
            end_date=end_date,
            distance_threshold=distance_threshold,
            max_stops_ahead=max_stops_ahead,
            attach_weather=attach_weather,
            use_shapes=use_shapes,
        )

        if not dataset_stats["success"]:
            _notify_blocks(
                notification_blocks,
                f"Dataset build failed: empty result for {output_name}",
                logger,
            )
            return {"success": False, "error": "Empty dataset"}

        # Save dataset
        df = dataset_stats.pop("dataframe")
        output_path = save_dataset_task(
            df=df,
            output_name=output_name,
            output_dir=output_dir,
        )

        # Optionally seed Redis caches
        redis_stats = None
        if seed_redis:
            redis_stats = seed_redis_caches_task(
                redis_host=redis_host,
                redis_port=redis_port,
                redis_db=redis_db,
                redis_password=redis_password,
            )

        # Create summary artifact
        create_dataset_summary_task(dataset_stats, output_path, build_params)

        # Send success notification
        if notification_blocks:
            _notify_blocks(
                notification_blocks,
                f"Dataset build complete: {dataset_stats['n_samples']:,} samples saved to {output_path}",
                logger,
            )

        logger.info("Dataset build complete: %s", output_path)

        return {
            "success": True,
            "output_path": output_path,
            "stats": dataset_stats,
            "redis_stats": redis_stats,
        }

    except Exception as exc:
        logger.error("Dataset build failed: %s", exc)
        _notify_blocks(
            notification_blocks,
            f"Dataset build failed: {exc}",
            logger,
        )
        raise


if __name__ == "__main__":
    # Test run (requires Django/DB setup)
    result = dataset_build_flow(
        output_name="test_dataset",
        days_back=1,
        max_stops_ahead=3,
        attach_weather=False,
    )
    print(result)
