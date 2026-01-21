#!/usr/bin/env python3
"""
Utilities to bootstrap Prefect blocks for the ETA runtime flow.

For Prefect 3.x - uses prefect.yaml for deployments.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

# Ensure the prefect directory is in the path for imports
PREFECT_DIR = Path(__file__).resolve().parent
if str(PREFECT_DIR) not in sys.path:
    sys.path.insert(0, str(PREFECT_DIR))

from runtime_config import EtaRuntimeSettings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage Prefect runtime blocks."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    block_parser = subparsers.add_parser(
        "save-block", help="Create or update the EtaRuntimeSettings Prefect block."
    )
    block_parser.add_argument(
        "--block-name",
        default="eta-runtime-default",
        help="Prefect block name.",
    )
    block_parser.add_argument("--redis-host", default="localhost")
    block_parser.add_argument("--redis-port", type=int, default=6379)
    block_parser.add_argument("--redis-db", type=int, default=0)
    block_parser.add_argument("--redis-password-secret", default=None)
    block_parser.add_argument("--poll-interval", type=float, default=1.0)
    block_parser.add_argument("--model-key", default=None)
    block_parser.add_argument("--predictions-ttl", type=int, default=300)
    block_parser.add_argument("--max-batch", type=int, default=None)
    block_parser.add_argument("--max-stops", type=int, default=5)
    block_parser.add_argument(
        "--zero-prediction-threshold",
        type=int,
        default=0,
        help="Iterations without predictions that trigger alerts.",
    )
    block_parser.add_argument(
        "--notification-block",
        action="append",
        default=None,
        help="Prefect notification block name (repeat to add multiples).",
    )
    block_parser.add_argument(
        "--profiling-artifact-key-prefix",
        default="eta-runtime",
        help="Artifact key prefix for profiling CSV uploads.",
    )
    block_parser.add_argument(
        "--model-registry-dir",
        default=None,
        help="Override MODEL_REGISTRY_DIR propagated to runtime workers.",
    )
    block_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacing an existing Prefect block.",
    )

    return parser.parse_args()


def _save_block(args: argparse.Namespace) -> None:
    notification_blocks: List[str] = []
    if args.notification_block:
        notification_blocks.extend(args.notification_block)

    settings = EtaRuntimeSettings(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_password_secret=args.redis_password_secret,
        poll_interval_seconds=args.poll_interval,
        model_key=args.model_key,
        predictions_ttl_seconds=args.predictions_ttl,
        max_vehicle_batch=args.max_batch,
        max_stops_per_vehicle=args.max_stops,
        zero_prediction_alert_threshold=args.zero_prediction_threshold,
        notification_blocks=notification_blocks,
        profiling_artifact_key_prefix=args.profiling_artifact_key_prefix,
        model_registry_dir=args.model_registry_dir,
    )
    settings.save(name=args.block_name, overwrite=args.overwrite)
    print(f"Saved EtaRuntimeSettings block -> {args.block_name}")


def main() -> None:
    args = _parse_args()
    if args.command == "save-block":
        _save_block(args)


if __name__ == "__main__":
    main()
