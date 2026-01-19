"""
Prefect version of the Redis -> ETA -> Redis streaming loop.

The flow keeps polling Redis for `vehicle:*` snapshots (populated by the MQTT
subscriber), fetches the cached `route_stops:<route_id>` definitions, calls
`estimate_stop_times()` from `eta_service`, and writes the predictions back to
`predictions:<vehicle_id>`.

This intentionally mirrors the original Bytewax pipeline but with a much
simpler control loop so we can run it anywhere Prefect is available.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import redis

# Import the Prefect library before we add the local `prefect/` directory to
# sys.path (to avoid shadowing the real package via namespace packages).
REPO_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT_STR = str(REPO_ROOT)
MODEL_DIR = (REPO_ROOT / "models" / "trained").resolve()
# Force absolute path so Prefect runs launched from `prefect/` or elsewhere
# never fall back to a missing relative directory.
os.environ["MODEL_REGISTRY_DIR"] = str(MODEL_DIR)
_removed_repo_root = False
if REPO_ROOT_STR in sys.path:
    sys.path.remove(REPO_ROOT_STR)
    _removed_repo_root = True

from prefect import flow, get_run_logger, task  # type: ignore

# Ensure repository modules (eta_service, models, etc.) are importable once
# Prefect is imported.
if _removed_repo_root or REPO_ROOT_STR not in sys.path:
    sys.path.insert(0, REPO_ROOT_STR)

from eta_service.estimator import estimate_stop_times
from models.common.registry import get_registry

_registry_probe = get_registry()
print(
    f"[Prefect Runtime] MODEL_REGISTRY_DIR={_registry_probe.base_dir} "
    f"(models loaded: {len(getattr(_registry_probe, 'registry', {}))})"
)


class DurationProfiler:
    """Collect durations and export histogram + summary to CSV."""

    def __init__(self, name: str, output_dir: Path, bins: int = 10) -> None:
        self.name = name
        self.output_dir = output_dir
        self.bins = max(bins, 1)
        self.values_ms: List[float] = []

    @property
    def output_path(self) -> Path:
        filename = f"{self.name}_times.csv"
        return self.output_dir / filename

    def reset(self) -> None:
        self.values_ms.clear()

    def record(self, duration_seconds: Optional[float]) -> None:
        if duration_seconds is None:
            return
        if duration_seconds < 0:
            return
        self.values_ms.append(duration_seconds * 1000.0)

    def _summary(self) -> Optional[Dict[str, Any]]:
        if not self.values_ms:
            return None
        count = len(self.values_ms)
        min_v = min(self.values_ms)
        max_v = max(self.values_ms)
        avg_v = sum(self.values_ms) / count
        return {
            "count": count,
            "min_ms": min_v,
            "max_ms": max_v,
            "avg_ms": avg_v,
        }

    def _histogram(self) -> List[Dict[str, Any]]:
        if not self.values_ms:
            return []
        min_v = min(self.values_ms)
        max_v = max(self.values_ms)
        if min_v == max_v:
            return [
                {
                    "bin_start": min_v,
                    "bin_end": max_v,
                    "count": len(self.values_ms),
                    "percentage": 100.0,
                }
            ]

        bin_count = min(self.bins, max(len(self.values_ms), 1))
        width = (max_v - min_v) / bin_count
        # Guard against very small ranges that could zero width due to float.
        width = width or (max_v - min_v)
        edges = [min_v + i * width for i in range(bin_count)]
        edges.append(max_v)
        counts = [0 for _ in range(bin_count)]

        for value in self.values_ms:
            if value >= max_v:
                idx = bin_count - 1
            else:
                idx = int((value - min_v) / width)
                idx = max(0, min(idx, bin_count - 1))
            counts[idx] += 1

        total = len(self.values_ms)
        histogram = []
        for idx, count in enumerate(counts):
            bin_start = edges[idx]
            bin_end = edges[idx + 1] if idx + 1 < len(edges) else max_v
            percentage = (count / total) * 100 if total else 0.0
            histogram.append(
                {
                    "bin_start": bin_start,
                    "bin_end": bin_end,
                    "count": count,
                    "percentage": percentage,
                }
            )
        return histogram

    def write_csv(self) -> None:
        summary = self._summary()
        if not summary:
            return
        self.output_dir.mkdir(parents=True, exist_ok=True)
        histogram = self._histogram()
        with self.output_path.open("w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["metric", "count", "min_ms", "max_ms", "avg_ms"])
            writer.writerow(
                [
                    self.name,
                    summary["count"],
                    f"{summary['min_ms']:.3f}",
                    f"{summary['max_ms']:.3f}",
                    f"{summary['avg_ms']:.3f}",
                ]
            )
            writer.writerow([])
            writer.writerow(["bin_start_ms", "bin_end_ms", "count", "percentage"])
            for row in histogram:
                writer.writerow(
                    [
                        f"{row['bin_start']:.3f}",
                        f"{row['bin_end']:.3f}",
                        row["count"],
                        f"{row['percentage']:.2f}",
                    ]
                )


class ProfilingSuite:
    """Keeps all metric recorders together for easier orchestration."""

    def __init__(self, output_dir: Path) -> None:
        self.recorders = {
            "redis_fetch": DurationProfiler("redis_fetch", output_dir),
            "eta_inference": DurationProfiler("eta_inference", output_dir),
            "redis_write": DurationProfiler("redis_write", output_dir),
            "pipeline_latency": DurationProfiler("pipeline_latency", output_dir),
        }

    def reset(self) -> None:
        for recorder in self.recorders.values():
            recorder.reset()

    def record(self, metric: str, seconds: Optional[float]) -> None:
        recorder = self.recorders.get(metric)
        if recorder:
            recorder.record(seconds)

    def write_reports(self) -> None:
        for recorder in self.recorders.values():
            recorder.write_csv()


PROFILING = ProfilingSuite(output_dir=Path(__file__).resolve().parent / "profiling")


@dataclass
class RedisPipelineConfig:
    """Settings for the Prefect polling loop."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    vehicle_key_pattern: str = "vehicle:*"
    route_stops_key_prefix: str = "route_stops:"
    route_shape_key_prefix: str = "route_shape:"
    predictions_key_prefix: str = "predictions:"
    predictions_ttl_seconds: int = 300
    poll_interval_seconds: float = 1.0
    max_vehicle_batch: Optional[int] = None
    max_stops_per_vehicle: int = 3
    model_key: Optional[str] = None
    
    # (
    #     "xgboost_various_dataset_5_spatial-temporal_global_20251202_063133_"
    #     "handle_nan=drop_learning_rate=0.05_max_depth=5_n_estimators=200"
    # )

    def redis_kwargs(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "password": self.password,
            "decode_responses": True,
        }


# Simple in-memory caches to avoid re-fetching the same JSON blobs every poll.
_ROUTE_STOPS_CACHE: Dict[str, List[Dict[str, Any]]] = {}


def _build_redis_client(config: RedisPipelineConfig) -> redis.Redis:
    return redis.Redis(**config.redis_kwargs())


def _load_json(client: redis.Redis, key: str) -> Any:
    raw = client.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _normalize_stops(data: Any) -> List[Dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, dict) and "stops" in data:
        stops = data["stops"]
    elif isinstance(data, list):
        stops = data
    else:
        return []

    normalized: List[Dict[str, Any]] = []
    total = len(stops)
    for idx, stop in enumerate(stops, start=1):
        stop_dict = dict(stop)
        stop_dict.setdefault("stop_sequence", stop_dict.get("sequence", idx))
        stop_dict.setdefault("total_stop_sequence", total)
        normalized.append(stop_dict)
    return normalized


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ensure_vehicle_identifiers(data: Dict[str, Any]) -> Optional[str]:
    vehicle_id = (
        data.get("vehicle_id")
        or data.get("vehicleId")
        or data.get("id")
        or (data.get("vehicle") or {}).get("id")
    )
    if vehicle_id:
        data["vehicle_id"] = str(vehicle_id)
    return data.get("vehicle_id")


def _extract_route_id(data: Dict[str, Any]) -> Optional[str]:
    route_id = (
        data.get("route_id")
        or data.get("route")
        or data.get("routeId")
        or data.get("route_short_name")
    )
    if route_id:
        data["route"] = route_id
    return route_id


def _validate_vehicle_snapshot(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    vehicle_id = _ensure_vehicle_identifiers(data)
    lat = _coerce_float(data.get("lat"))
    lon = _coerce_float(data.get("lon"))
    speed = _coerce_float(data.get("speed", 0))
    timestamp = data.get("timestamp")
    route_id = _extract_route_id(data)

    if not all([vehicle_id, lat is not None, lon is not None, timestamp, route_id]):
        return None

    data["lat"] = lat
    data["lon"] = lon
    data["speed"] = speed or 0.0
    data.setdefault("trip_id", data.get("trip") or data.get("trip_id"))
    return data


def _is_duplicate_snapshot(
    client: redis.Redis,
    config: RedisPipelineConfig,
    vehicle_id: str,
    source_timestamp: Optional[str],
) -> bool:
    if not vehicle_id or not source_timestamp:
        return False
    prediction_key = f"{config.predictions_key_prefix}{vehicle_id}"
    raw_prediction = client.get(prediction_key)
    if not raw_prediction:
        return False
    try:
        cached_prediction = json.loads(raw_prediction)
    except json.JSONDecodeError:
        return False
    cached_timestamp = cached_prediction.get("source_timestamp")
    return cached_timestamp == source_timestamp


@task
def fetch_vehicle_snapshots(config: RedisPipelineConfig) -> List[Dict[str, Any]]:
    client = _build_redis_client(config)
    snapshots: List[Dict[str, Any]] = []
    for key in client.scan_iter(match=config.vehicle_key_pattern):
        fetch_start = time.perf_counter()
        payload = client.get(key)
        PROFILING.record("redis_fetch", time.perf_counter() - fetch_start)
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue

        data["_redis_key"] = key
        data["_fetched_at"] = datetime.now(timezone.utc).isoformat()
        data["_prefect_pipeline_start"] = time.perf_counter()
        sanitized = _validate_vehicle_snapshot(data)
        if sanitized:
            if _is_duplicate_snapshot(
                client,
                config,
                sanitized.get("vehicle_id"),
                sanitized.get("timestamp"),
            ):
                continue
            snapshots.append(sanitized)

        if config.max_vehicle_batch and len(snapshots) >= config.max_vehicle_batch:
            break
    return snapshots


def _load_route_stops(route_id: str, client: redis.Redis, config: RedisPipelineConfig) -> List[Dict[str, Any]]:
    if route_id in _ROUTE_STOPS_CACHE:
        return _ROUTE_STOPS_CACHE[route_id]
    key = f"{config.route_stops_key_prefix}{route_id}"
    stops = _normalize_stops(_load_json(client, key))
    if stops:
        _ROUTE_STOPS_CACHE[route_id] = stops
    return stops


@task
def predict_for_snapshots(
    config: RedisPipelineConfig,
    snapshots: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    client = _build_redis_client(config)
    predictions: List[Dict[str, Any]] = []

    for vehicle in snapshots:
        route_id = _extract_route_id(vehicle)
        if not route_id:
            continue
        stops = _load_route_stops(route_id, client, config)
        if not stops:
            continue

        max_stops = config.max_stops_per_vehicle
        inference_start = time.perf_counter()
        result = estimate_stop_times(
            vehicle_position=vehicle,
            upcoming_stops=stops[:max_stops] if max_stops else stops,
            route_id=route_id,
            trip_id=vehicle.get("trip_id"),
            # model_type="ewma",
            model_key=config.model_key,
            max_stops=max_stops,
        )
        PROFILING.record("eta_inference", time.perf_counter() - inference_start)

        result["vehicle_id"] = vehicle["vehicle_id"]
        result["route_id"] = route_id
        result["source"] = "prefect"
        result["redis_key"] = vehicle.get("_redis_key")
        result["fetched_at"] = vehicle.get("_fetched_at")
        result["persisted_at"] = datetime.now(timezone.utc).isoformat()
        result["source_timestamp"] = vehicle.get("timestamp")
        if "_prefect_pipeline_start" in vehicle:
            result["_prefect_pipeline_start"] = vehicle["_prefect_pipeline_start"]
        predictions.append(result)

    return predictions


@task
def write_predictions_to_redis(
    config: RedisPipelineConfig, results: Iterable[Dict[str, Any]]
) -> int:
    client = _build_redis_client(config)
    count = 0
    for result in results:
        vehicle_id = result.get("vehicle_id")
        if not vehicle_id:
            continue
        redis_key = f"{config.predictions_key_prefix}{vehicle_id}"
        start_ts = result.get("_prefect_pipeline_start")
        if isinstance(start_ts, float):
            end_to_end = time.perf_counter() - start_ts
            PROFILING.record("pipeline_latency", end_to_end)
            result["prefect_pipeline_latency_ms"] = round(end_to_end * 1000, 3)
        result.pop("_prefect_pipeline_start", None)
        payload = json.dumps(result)
        write_start = time.perf_counter()
        client.set(redis_key, payload, ex=config.predictions_ttl_seconds)
        PROFILING.record("redis_write", time.perf_counter() - write_start)
        count += 1
    return count


@flow(name="prefect-eta-runtime")
def prefect_eta_runtime(
    config: RedisPipelineConfig,
    iterations: int = 0,
) -> None:
    """
    Run the polling loop. If `iterations` is zero it will run forever.
    """

    logger = get_run_logger()
    PROFILING.reset()
    loop = 0
    try:
        while True:
            loop += 1
            snapshots = fetch_vehicle_snapshots(config)
            if not snapshots:
                logger.info("No vehicle snapshots found.")
            else:
                predictions = predict_for_snapshots(config, snapshots)
                written = write_predictions_to_redis(config, predictions)
                logger.info(
                    "Processed %s vehicles → %s predictions",
                    len(snapshots),
                    written,
                )

            if iterations and loop >= iterations:
                break

            time.sleep(config.poll_interval_seconds)
    finally:
        PROFILING.write_reports()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Prefect ETA streaming loop.")
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--redis-db", type=int, default=0)
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Seconds between polls.")
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Number of polling iterations to run (0 runs forever).",
    )
    parser.add_argument(
        "--max-batch",
        type=int,
        default=None,
        help="Limit the number of vehicle snapshots processed per iteration.",
    )
    parser.add_argument(
        "--max-stops",
        type=int,
        default=3,
        help="Max stops passed to the estimator per vehicle.",
    )
    parser.add_argument(
        "--model-key",
        default=RedisPipelineConfig.model_key,
        help="Model registry key to load for inference (defaults to global xgboost model).",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    config = RedisPipelineConfig(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        password=args.redis_password,
        poll_interval_seconds=args.poll_interval,
        max_vehicle_batch=args.max_batch,
        max_stops_per_vehicle=args.max_stops,
        model_key=args.model_key,
    )
    prefect_eta_runtime(config=config, iterations=args.iterations)


if __name__ == "__main__":
    main()
