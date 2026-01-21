"""
Prefect flow for evaluating and comparing ETA prediction models.

Reads model metrics from the registry, compares performance across
model types and routes, and identifies promotion candidates.
"""

from __future__ import annotations

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


@task(name="load-registry-models")
def load_registry_models_task() -> Dict[str, Any]:
    """Load all models from registry with their metrics."""
    logger = get_run_logger()
    registry = get_registry()

    models_df = registry.list_models()
    logger.info("Loaded %d models from registry", len(models_df))

    # Load detailed metadata for each model
    models_data = []
    for _, row in models_df.iterrows():
        model_key = row["model_key"]
        try:
            metadata = registry.load_metadata(model_key)
            metrics = metadata.get("metrics", {})
            models_data.append({
                "model_key": model_key,
                "model_type": row.get("model_type", "unknown"),
                "route_id": row.get("route_id", "global"),
                "dataset": metadata.get("dataset", "unknown"),
                "saved_at": row.get("saved_at"),
                "n_samples": metadata.get("n_samples"),
                "test_mae_seconds": metrics.get("test_mae_seconds"),
                "test_mae_minutes": metrics.get("test_mae_minutes"),
                "test_rmse_seconds": metrics.get("test_rmse_seconds"),
                "test_r2": metrics.get("test_r2"),
            })
        except Exception as exc:
            logger.warning("Failed to load metadata for %s: %s", model_key, exc)

    return {
        "models": models_data,
        "n_models": len(models_data),
        "model_types": list(set(m["model_type"] for m in models_data)),
    }


@task(name="compare-global-models")
def compare_global_models_task(
    models_data: List[Dict[str, Any]],
    metric: str,
    minimize: bool,
) -> Dict[str, Any]:
    """Compare global models to find the best performer."""
    logger = get_run_logger()

    # Filter to global models only
    global_models = [m for m in models_data if m["route_id"] == "global"]
    logger.info("Comparing %d global models", len(global_models))

    if not global_models:
        return {"best_model": None, "rankings": []}

    # Sort by metric
    valid_models = [m for m in global_models if m.get(metric) is not None]
    if not valid_models:
        return {"best_model": None, "rankings": []}

    sorted_models = sorted(
        valid_models,
        key=lambda x: x[metric],
        reverse=not minimize,
    )

    best_model = sorted_models[0]
    logger.info(
        "Best global model: %s (%s=%s)",
        best_model["model_key"],
        metric,
        best_model[metric],
    )

    rankings = []
    for rank, model in enumerate(sorted_models, 1):
        rankings.append({
            "rank": rank,
            "model_key": model["model_key"],
            "model_type": model["model_type"],
            metric: model[metric],
            "dataset": model["dataset"],
        })

    return {
        "best_model": best_model,
        "rankings": rankings,
    }


@task(name="compare-route-models")
def compare_route_models_task(
    models_data: List[Dict[str, Any]],
    metric: str,
    minimize: bool,
) -> Dict[str, Dict[str, Any]]:
    """Compare models for each route to find route-specific best performers."""
    logger = get_run_logger()

    # Group by route
    routes = set(m["route_id"] for m in models_data if m["route_id"] != "global")
    logger.info("Comparing models for %d routes", len(routes))

    route_results = {}
    for route_id in routes:
        route_models = [m for m in models_data if m["route_id"] == route_id]
        valid_models = [m for m in route_models if m.get(metric) is not None]

        if not valid_models:
            continue

        sorted_models = sorted(
            valid_models,
            key=lambda x: x[metric],
            reverse=not minimize,
        )

        best = sorted_models[0]
        route_results[route_id] = {
            "best_model": best,
            "n_models": len(valid_models),
            "best_metric_value": best[metric],
        }

    return route_results


@task(name="identify-promotion-candidates")
def identify_promotion_candidates_task(
    global_comparison: Dict[str, Any],
    route_comparisons: Dict[str, Dict[str, Any]],
    current_active_model: Optional[str],
    improvement_threshold: float,
) -> List[Dict[str, Any]]:
    """Identify models that should be promoted based on performance improvement."""
    logger = get_run_logger()
    candidates = []

    # Check global model
    best_global = global_comparison.get("best_model")
    if best_global:
        if current_active_model is None:
            # No current model, promote the best
            candidates.append({
                "model_key": best_global["model_key"],
                "scope": "global",
                "reason": "No current active model",
                "metric_value": best_global.get("test_mae_seconds"),
            })
        elif best_global["model_key"] != current_active_model:
            # Different model is best, check if improvement is significant
            # For now, we just flag it as a candidate
            candidates.append({
                "model_key": best_global["model_key"],
                "scope": "global",
                "reason": "Better performance than current model",
                "metric_value": best_global.get("test_mae_seconds"),
            })

    # Check route-specific models
    for route_id, route_result in route_comparisons.items():
        best_route = route_result.get("best_model")
        if best_route:
            candidates.append({
                "model_key": best_route["model_key"],
                "scope": f"route:{route_id}",
                "reason": f"Best model for route {route_id}",
                "metric_value": best_route.get("test_mae_seconds"),
            })

    logger.info("Identified %d promotion candidates", len(candidates))
    return candidates


@task(name="create-evaluation-report")
def create_evaluation_report_task(
    models_info: Dict[str, Any],
    global_comparison: Dict[str, Any],
    route_comparisons: Dict[str, Dict[str, Any]],
    candidates: List[Dict[str, Any]],
    metric: str,
) -> str:
    """Create a markdown evaluation report as a Prefect artifact."""
    logger = get_run_logger()

    lines = [
        "# Model Evaluation Report",
        "",
        f"**Timestamp:** {datetime.now().isoformat()}",
        f"**Metric:** {metric}",
        f"**Total Models:** {models_info['n_models']}",
        f"**Model Types:** {', '.join(models_info['model_types'])}",
        "",
        "## Global Model Rankings",
        "",
    ]

    rankings = global_comparison.get("rankings", [])
    if rankings:
        lines.extend([
            "| Rank | Model | Type | MAE (sec) | Dataset |",
            "|------|-------|------|-----------|---------|",
        ])
        for r in rankings[:10]:  # Top 10
            lines.append(
                f"| {r['rank']} | `{r['model_key'][:40]}...` | {r['model_type']} | "
                f"{r.get(metric, 'N/A'):.1f} | {r['dataset']} |"
            )
    else:
        lines.append("*No global models found*")

    lines.extend([
        "",
        "## Best Models by Route",
        "",
    ])

    if route_comparisons:
        lines.extend([
            "| Route | Best Model | MAE (sec) | # Models |",
            "|-------|------------|-----------|----------|",
        ])
        for route_id, result in sorted(route_comparisons.items()):
            best = result["best_model"]
            lines.append(
                f"| {route_id} | {best['model_type']} | "
                f"{result['best_metric_value']:.1f} | {result['n_models']} |"
            )
    else:
        lines.append("*No route-specific models found*")

    lines.extend([
        "",
        "## Promotion Candidates",
        "",
    ])

    if candidates:
        lines.extend([
            "| Model | Scope | Reason | MAE (sec) |",
            "|-------|-------|--------|-----------|",
        ])
        for c in candidates:
            metric_val = c.get("metric_value")
            metric_str = f"{metric_val:.1f}" if metric_val else "N/A"
            lines.append(
                f"| `{c['model_key'][:40]}...` | {c['scope']} | {c['reason']} | {metric_str} |"
            )
    else:
        lines.append("*No promotion candidates identified*")

    report = "\n".join(lines)

    create_markdown_artifact(
        key="evaluation-report",
        markdown=report,
        description="Model evaluation and comparison report",
    )

    logger.info("Evaluation report artifact created")
    return report


@flow(name="model-evaluation")
def evaluation_flow(
    metric: str = "test_mae_seconds",
    minimize: bool = True,
    current_active_model: Optional[str] = None,
    improvement_threshold: float = 0.05,
    notification_blocks: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Evaluate and compare all models in the registry.

    Args:
        metric: Metric to use for comparison (e.g., test_mae_seconds, test_rmse_seconds)
        minimize: Whether to minimize the metric (True for errors, False for R²)
        current_active_model: Current active model key for comparison
        improvement_threshold: Minimum improvement to recommend promotion (0.05 = 5%)
        notification_blocks: Prefect blocks for notifications

    Returns:
        Dictionary with evaluation results and promotion candidates
    """
    logger = get_run_logger()
    notification_blocks = notification_blocks or []

    logger.info(
        "Starting evaluation flow: metric=%s, minimize=%s",
        metric, minimize
    )

    # Load models
    models_info = load_registry_models_task()

    if models_info["n_models"] == 0:
        logger.warning("No models found in registry")
        return {"success": False, "error": "No models in registry"}

    # Compare global models
    global_comparison = compare_global_models_task(
        models_data=models_info["models"],
        metric=metric,
        minimize=minimize,
    )

    # Compare route models
    route_comparisons = compare_route_models_task(
        models_data=models_info["models"],
        metric=metric,
        minimize=minimize,
    )

    # Identify promotion candidates
    candidates = identify_promotion_candidates_task(
        global_comparison=global_comparison,
        route_comparisons=route_comparisons,
        current_active_model=current_active_model,
        improvement_threshold=improvement_threshold,
    )

    # Create report
    report = create_evaluation_report_task(
        models_info=models_info,
        global_comparison=global_comparison,
        route_comparisons=route_comparisons,
        candidates=candidates,
        metric=metric,
    )

    # Send notification
    if notification_blocks and candidates:
        _notify_blocks(
            notification_blocks,
            f"Model evaluation complete: {len(candidates)} promotion candidates identified",
            logger,
        )

    logger.info("Evaluation flow complete")

    return {
        "success": True,
        "n_models": models_info["n_models"],
        "best_global_model": global_comparison.get("best_model"),
        "route_best_models": route_comparisons,
        "promotion_candidates": candidates,
    }


if __name__ == "__main__":
    # Test run
    result = evaluation_flow(
        metric="test_mae_seconds",
        minimize=True,
    )
    print(result)
