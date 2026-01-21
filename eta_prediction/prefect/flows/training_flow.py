"""
Prefect flow for training ETA prediction models.

Wraps the existing training infrastructure from models/train_all_models.py
and adds Prefect orchestration, artifacts, and notifications.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure repo modules are importable
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.blocks.core import Block

from models.common.registry import get_registry
from models.common.data import load_dataset
from models.historical_mean.train import train_historical_mean
from models.polyreg_distance.train import train_polyreg_distance
from models.polyreg_time.train import train_polyreg_time
from models.ewma.train import train_ewma
from models.xgb.train import train_xgboost


# Default train/val/test split fractions
DEFAULT_TRAIN_FRAC = 0.7
DEFAULT_VAL_FRAC = 0.1


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


def _temporal_split_df(df, train_frac: float, val_frac: float):
    """Deterministically split a dataframe by vp_ts."""
    import pandas as pd

    df_sorted = df.sort_values("vp_ts").reset_index(drop=True)
    n = len(df_sorted)
    train_end = int(n * train_frac)
    val_end = int(n * (train_frac + val_frac))
    train_df = df_sorted.iloc[:train_end].copy()
    val_df = df_sorted.iloc[train_end:val_end].copy()
    test_df = df_sorted.iloc[val_end:].copy()
    return train_df, val_df, test_df


def _clone_splits(splits):
    """Return deep copies of precomputed splits."""
    return tuple(split.copy() for split in splits)


@task(name="load-dataset", retries=2, retry_delay_seconds=10)
def load_training_dataset(dataset_name: str) -> Dict[str, Any]:
    """Load and clean the training dataset."""
    logger = get_run_logger()
    logger.info("Loading dataset: %s", dataset_name)

    dataset = load_dataset(dataset_name)
    dataset.clean_data()

    n_samples = len(dataset.df)
    n_routes = dataset.df["route_id"].nunique()
    n_trips = dataset.df["trip_id"].nunique()

    logger.info(
        "Dataset loaded: %d samples, %d routes, %d trips",
        n_samples, n_routes, n_trips
    )

    return {
        "dataset": dataset,
        "n_samples": n_samples,
        "n_routes": n_routes,
        "n_trips": n_trips,
        "dataset_name": dataset_name,
    }


@task(name="train-historical-mean")
def train_historical_mean_task(
    dataset_name: str,
    route_id: Optional[str],
    pre_split: tuple,
    save_model: bool,
) -> Dict[str, Any]:
    """Train historical mean model."""
    logger = get_run_logger()
    scope = f"route {route_id}" if route_id else "global"
    logger.info("Training Historical Mean (%s)", scope)

    result = train_historical_mean(
        dataset_name=dataset_name,
        route_id=route_id,
        save_model=save_model,
        pre_split=pre_split,
    )

    logger.info(
        "Historical Mean (%s): MAE=%.3f min, RMSE=%.1f sec",
        scope,
        result["metrics"].get("test_mae_minutes", 0),
        result["metrics"].get("test_rmse_seconds", 0),
    )
    return result


@task(name="train-polyreg-distance")
def train_polyreg_distance_task(
    dataset_name: str,
    route_id: Optional[str],
    pre_split: tuple,
    save_model: bool,
    degree: int = 2,
) -> Dict[str, Any]:
    """Train polynomial regression (distance) model."""
    logger = get_run_logger()
    scope = f"route {route_id}" if route_id else "global"
    logger.info("Training PolyReg Distance (%s)", scope)

    result = train_polyreg_distance(
        dataset_name=dataset_name,
        route_id=route_id,
        degree=degree,
        save_model=save_model,
        pre_split=pre_split,
    )

    logger.info(
        "PolyReg Distance (%s): MAE=%.3f min, RMSE=%.1f sec",
        scope,
        result["metrics"].get("test_mae_minutes", 0),
        result["metrics"].get("test_rmse_seconds", 0),
    )
    return result


@task(name="train-polyreg-time")
def train_polyreg_time_task(
    dataset_name: str,
    route_id: Optional[str],
    pre_split: tuple,
    save_model: bool,
    poly_degree: int = 2,
    include_temporal: bool = True,
) -> Dict[str, Any]:
    """Train polynomial regression (time) model."""
    logger = get_run_logger()
    scope = f"route {route_id}" if route_id else "global"
    logger.info("Training PolyReg Time (%s)", scope)

    result = train_polyreg_time(
        dataset_name=dataset_name,
        route_id=route_id,
        poly_degree=poly_degree,
        include_temporal=include_temporal,
        save_model=save_model,
        pre_split=pre_split,
    )

    logger.info(
        "PolyReg Time (%s): MAE=%.3f min, RMSE=%.1f sec",
        scope,
        result["metrics"].get("test_mae_minutes", 0),
        result["metrics"].get("test_rmse_seconds", 0),
    )
    return result


@task(name="train-ewma")
def train_ewma_task(
    dataset_name: str,
    route_id: Optional[str],
    pre_split: tuple,
    save_model: bool,
    alpha: float = 0.3,
) -> Dict[str, Any]:
    """Train EWMA model."""
    logger = get_run_logger()
    scope = f"route {route_id}" if route_id else "global"
    logger.info("Training EWMA (%s)", scope)

    result = train_ewma(
        dataset_name=dataset_name,
        route_id=route_id,
        alpha=alpha,
        save_model=save_model,
        pre_split=pre_split,
    )

    logger.info(
        "EWMA (%s): MAE=%.3f min, RMSE=%.1f sec",
        scope,
        result["metrics"].get("test_mae_minutes", 0),
        result["metrics"].get("test_rmse_seconds", 0),
    )
    return result


@task(name="train-xgboost")
def train_xgboost_task(
    dataset_name: str,
    route_id: Optional[str],
    pre_split: tuple,
    save_model: bool,
) -> Dict[str, Any]:
    """Train XGBoost model."""
    logger = get_run_logger()
    scope = f"route {route_id}" if route_id else "global"
    logger.info("Training XGBoost (%s)", scope)

    result = train_xgboost(
        dataset_name=dataset_name,
        route_id=route_id,
        save_model=save_model,
        pre_split=pre_split,
    )

    logger.info(
        "XGBoost (%s): MAE=%.3f min, RMSE=%.1f sec, R2=%.3f",
        scope,
        result["metrics"].get("test_mae_minutes", 0),
        result["metrics"].get("test_rmse_seconds", 0),
        result["metrics"].get("test_r2", 0),
    )
    return result


@task(name="create-training-summary")
def create_training_summary(
    results: Dict[str, Dict[str, Any]],
    dataset_info: Dict[str, Any],
    by_route: bool,
) -> str:
    """Create a markdown summary of training results."""
    logger = get_run_logger()

    lines = [
        "# Training Summary",
        "",
        f"**Dataset:** {dataset_info['dataset_name']}",
        f"**Samples:** {dataset_info['n_samples']:,}",
        f"**Routes:** {dataset_info['n_routes']}",
        f"**Trips:** {dataset_info['n_trips']:,}",
        f"**Mode:** {'Per-Route' if by_route else 'Global'}",
        f"**Timestamp:** {datetime.now().isoformat()}",
        "",
        "## Model Performance",
        "",
        "| Model | MAE (min) | RMSE (sec) | R² |",
        "|-------|-----------|------------|-----|",
    ]

    for model_type, result in results.items():
        metrics = result.get("metrics", {})
        mae = metrics.get("test_mae_minutes", "-")
        rmse = metrics.get("test_rmse_seconds", "-")
        r2 = metrics.get("test_r2", "-")

        mae_str = f"{mae:.3f}" if isinstance(mae, (int, float)) else mae
        rmse_str = f"{rmse:.1f}" if isinstance(rmse, (int, float)) else rmse
        r2_str = f"{r2:.3f}" if isinstance(r2, (int, float)) else r2

        lines.append(f"| {model_type} | {mae_str} | {rmse_str} | {r2_str} |")

    lines.extend([
        "",
        "## Model Keys",
        "",
    ])

    for model_type, result in results.items():
        model_key = result.get("model_key", "N/A")
        lines.append(f"- **{model_type}:** `{model_key}`")

    summary = "\n".join(lines)

    # Create Prefect artifact
    create_markdown_artifact(
        key="training-summary",
        markdown=summary,
        description="Summary of model training results",
    )

    logger.info("Training summary artifact created")
    return summary


@flow(name="model-training")
def training_flow(
    dataset_name: str = "various_dataset_5",
    by_route: bool = False,
    model_types: Optional[List[str]] = None,
    save_models: bool = True,
    notification_blocks: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Train ETA prediction models.

    Args:
        dataset_name: Name of the dataset to train on
        by_route: If True, train separate models per route
        model_types: List of model types to train (default: all)
        save_models: Whether to save models to registry
        notification_blocks: Prefect notification blocks for alerts

    Returns:
        Dictionary with training results and metrics
    """
    logger = get_run_logger()
    notification_blocks = notification_blocks or []

    if model_types is None:
        model_types = [
            "historical_mean",
            "polyreg_distance",
            "polyreg_time",
            "ewma",
            "xgboost",
        ]

    logger.info(
        "Starting training flow: dataset=%s, by_route=%s, models=%s",
        dataset_name, by_route, model_types
    )

    # Load dataset
    dataset_info = load_training_dataset(dataset_name)
    dataset = dataset_info["dataset"]

    results = {}

    if by_route:
        # Per-route training
        routes = sorted(dataset.df["route_id"].unique())
        logger.info("Training %d routes: %s", len(routes), routes)

        for route_id in routes:
            route_df = dataset.df[dataset.df["route_id"] == route_id].copy()
            if route_df.empty:
                logger.warning("Skipping route %s: no samples", route_id)
                continue

            route_splits = _temporal_split_df(
                route_df, DEFAULT_TRAIN_FRAC, DEFAULT_VAL_FRAC
            )

            route_results = {}

            if "historical_mean" in model_types:
                route_results["historical_mean"] = train_historical_mean_task(
                    dataset_name, route_id, _clone_splits(route_splits), save_models
                )

            if "polyreg_distance" in model_types:
                route_results["polyreg_distance"] = train_polyreg_distance_task(
                    dataset_name, route_id, _clone_splits(route_splits), save_models
                )

            if "polyreg_time" in model_types:
                route_results["polyreg_time"] = train_polyreg_time_task(
                    dataset_name, route_id, _clone_splits(route_splits), save_models
                )

            if "ewma" in model_types:
                route_results["ewma"] = train_ewma_task(
                    dataset_name, route_id, _clone_splits(route_splits), save_models
                )

            if "xgboost" in model_types:
                route_results["xgboost"] = train_xgboost_task(
                    dataset_name, route_id, _clone_splits(route_splits), save_models
                )

            results[route_id] = route_results

    else:
        # Global training
        global_splits = dataset.temporal_split(
            train_frac=DEFAULT_TRAIN_FRAC,
            val_frac=DEFAULT_VAL_FRAC,
        )

        if "historical_mean" in model_types:
            results["historical_mean"] = train_historical_mean_task(
                dataset_name, None, _clone_splits(global_splits), save_models
            )

        if "polyreg_distance" in model_types:
            results["polyreg_distance"] = train_polyreg_distance_task(
                dataset_name, None, _clone_splits(global_splits), save_models
            )

        if "polyreg_time" in model_types:
            results["polyreg_time"] = train_polyreg_time_task(
                dataset_name, None, _clone_splits(global_splits), save_models
            )

        if "ewma" in model_types:
            results["ewma"] = train_ewma_task(
                dataset_name, None, _clone_splits(global_splits), save_models
            )

        if "xgboost" in model_types:
            results["xgboost"] = train_xgboost_task(
                dataset_name, None, _clone_splits(global_splits), save_models
            )

    # Create summary
    summary = create_training_summary(results, dataset_info, by_route)

    # Send notification
    if notification_blocks:
        n_models = len(results) if not by_route else sum(len(r) for r in results.values())
        _notify_blocks(
            notification_blocks,
            f"Training complete: {n_models} models trained on {dataset_name}",
            logger,
        )

    logger.info("Training flow complete")

    return {
        "dataset_info": dataset_info,
        "results": results,
        "summary": summary,
    }


if __name__ == "__main__":
    # Test run
    training_flow(
        dataset_name="various_dataset_5",
        by_route=False,
        model_types=["xgboost"],
        save_models=False,
    )
