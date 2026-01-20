#!/usr/bin/env python3
"""
Utilities to bootstrap Prefect blocks and deployments for the ETA runtime flow.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import List, Optional

from prefect.blocks.core import Block
from prefect.infrastructure.process import Process

from prefect_eta_flow import prefect_eta_runtime
from runtime_config import EtaRuntimeSettings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage Prefect runtime blocks and deployments."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    block_parser = subparsers.add_parser(
        "save-block", help="Create or update the EtaRuntimeSettings Prefect block."
    )
    block_parser.add_argument(
        "--block-name",
        default="eta-runtime/default",
        help="Fully qualified Prefect block name (collection/name).",
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

    deploy_parser = subparsers.add_parser(
        "deploy", help="Build and apply the Prefect eta-runtime deployment."
    )
    deploy_parser.add_argument(
        "--deployment-name",
        default="eta-runtime",
        help="Prefect deployment name.",
    )
    deploy_parser.add_argument(
        "--block-name",
        default="eta-runtime/default",
        help="EtaRuntimeSettings block that supplies parameters.",
    )
    deploy_parser.add_argument(
        "--work-pool",
        required=True,
        help="Prefect work pool that should execute this deployment.",
    )
    deploy_parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Override the number of polling iterations (0 = run forever).",
    )
    deploy_parser.add_argument(
        "--version",
        default=None,
        help="Optional deployment version (defaults to GIT_COMMIT or dev).",
    )
    deploy_parser.add_argument(
        "--description",
        default="Prefect ETA runtime streaming deployment.",
    )
    deploy_parser.add_argument(
        "--tag",
        action="append",
        default=None,
        help="Optional deployment tags.",
    )
    deploy_parser.add_argument(
        "--env-block",
        default=None,
        help="EnvironmentVariables block injected into the Process infrastructure.",
    )
    deploy_parser.add_argument(
        "--working-dir",
        default=None,
        help="Working directory for the Process infrastructure (defaults to prefect/ folder).",
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
    print(f"✓ Saved EtaRuntimeSettings block -> {args.block_name}")


def _load_env(args: argparse.Namespace) -> Optional[dict]:
    if not args.env_block:
        return None
    block = Block.load(args.env_block)
    # EnvironmentVariables exposes `variables`; other blocks may store dict-like values.
    if hasattr(block, "variables"):
        return dict(block.variables)
    if hasattr(block, "value"):
        value = block.value
    else:
        value = getattr(block, "dict", lambda: block)()
    if isinstance(value, dict):
        return value
    raise ValueError(
        f"Block {args.env_block} did not return a dictionary of environment variables."
    )


def _build_process_infra(args: argparse.Namespace) -> Process:
    working_dir = args.working_dir or Path(__file__).parent
    env = _load_env(args) or None
    return Process(working_dir=str(Path(working_dir).resolve()), env=env)


def _deploy(args: argparse.Namespace) -> None:
    tags = args.tag or ["eta", "runtime"]
    version = args.version or os.getenv("GIT_COMMIT", "dev")
    parameters = {"runtime_settings_block": args.block_name, "iterations": args.iterations}
    deployment_result = prefect_eta_runtime.deploy(
        name=args.deployment_name,
        work_pool_name=args.work_pool,
        parameters=parameters,
        tags=tags,
        version=version,
        description=args.description,
        infrastructure=_build_process_infra(args),
    )
    deployment_name = getattr(deployment_result, "name", args.deployment_name)
    deployment_id = getattr(deployment_result, "deployment_id", None)
    print(
        f"✓ Deployment '{deployment_name}' registered against work pool "
        f"'{args.work_pool}' with block {args.block_name}"
    )
    if deployment_id:
        print(f"  Deployment ID: {deployment_id}")


def main() -> None:
    args = _parse_args()
    if args.command == "save-block":
        _save_block(args)
    elif args.command == "deploy":
        _deploy(args)


if __name__ == "__main__":
    main()
