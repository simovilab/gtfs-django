"""
Prefect flow for promoting models to production.

Updates the active model configuration in Prefect blocks and/or Redis,
and optionally triggers runtime reload.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure repo modules are importable
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
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


@task(name="validate-model-exists")
def validate_model_exists_task(model_key: str) -> Dict[str, Any]:
    """Validate that the model exists in the registry."""
    logger = get_run_logger()
    registry = get_registry()

    if model_key not in registry.registry:
        raise ValueError(f"Model '{model_key}' not found in registry")

    metadata = registry.load_metadata(model_key)
    logger.info(
        "Model validated: %s (type=%s, dataset=%s)",
        model_key,
        metadata.get("model_type"),
        metadata.get("dataset"),
    )

    return {
        "model_key": model_key,
        "model_type": metadata.get("model_type"),
        "dataset": metadata.get("dataset"),
        "saved_at": metadata.get("saved_at"),
        "metrics": metadata.get("metrics", {}),
    }


@task(name="update-runtime-block")
def update_runtime_block_task(
    model_key: str,
    runtime_block_name: str,
) -> bool:
    """Update the EtaRuntimeSettings block with the new model key."""
    logger = get_run_logger()

    try:
        # Import here to avoid circular dependency
        sys.path.insert(0, str(REPO_ROOT / "prefect"))
        from runtime_config import EtaRuntimeSettings

        # Load existing block
        block = EtaRuntimeSettings.load(runtime_block_name)
        old_model_key = block.model_key

        # Update model key
        block.model_key = model_key
        block.save(name=runtime_block_name, overwrite=True)

        logger.info(
            "Updated runtime block %s: %s -> %s",
            runtime_block_name, old_model_key, model_key
        )
        return True

    except Exception as exc:
        logger.error("Failed to update runtime block: %s", exc)
        raise


@task(name="update-redis-config")
def update_redis_config_task(
    model_key: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    redis_password: Optional[str],
    config_key: str = "eta:active_model",
) -> bool:
    """Update the active model configuration in Redis."""
    logger = get_run_logger()
    import redis

    try:
        client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True,
        )

        # Store with metadata
        config = {
            "model_key": model_key,
            "promoted_at": datetime.now().isoformat(),
            "source": "prefect-promotion-flow",
        }
        client.set(config_key, json.dumps(config))

        logger.info("Updated Redis config %s with model %s", config_key, model_key)
        return True

    except Exception as exc:
        logger.error("Failed to update Redis config: %s", exc)
        raise


@task(name="warm-model-cache")
def warm_model_cache_task(model_key: str) -> bool:
    """Pre-load the model into memory to warm the cache."""
    logger = get_run_logger()
    registry = get_registry()

    try:
        # Just load the model to warm any caches
        model = registry.load_model(model_key)
        logger.info("Model %s loaded for cache warming", model_key)
        return True
    except Exception as exc:
        logger.warning("Failed to warm model cache: %s", exc)
        return False


@task(name="create-promotion-lineage")
def create_promotion_lineage_task(
    model_info: Dict[str, Any],
    previous_model: Optional[str],
    promotion_reason: Optional[str],
) -> str:
    """Create a lineage artifact for audit purposes."""
    logger = get_run_logger()

    metrics = model_info.get("metrics", {})
    lines = [
        "# Model Promotion Lineage",
        "",
        f"**Promotion Timestamp:** {datetime.now().isoformat()}",
        "",
        "## Promoted Model",
        "",
        f"- **Model Key:** `{model_info['model_key']}`",
        f"- **Model Type:** {model_info.get('model_type', 'unknown')}",
        f"- **Dataset:** {model_info.get('dataset', 'unknown')}",
        f"- **Trained At:** {model_info.get('saved_at', 'unknown')}",
        "",
        "## Performance Metrics",
        "",
        f"- **MAE (seconds):** {metrics.get('test_mae_seconds', 'N/A')}",
        f"- **MAE (minutes):** {metrics.get('test_mae_minutes', 'N/A')}",
        f"- **RMSE (seconds):** {metrics.get('test_rmse_seconds', 'N/A')}",
        f"- **R²:** {metrics.get('test_r2', 'N/A')}",
        "",
        "## Promotion Context",
        "",
        f"- **Previous Model:** {previous_model or 'None'}",
        f"- **Promotion Reason:** {promotion_reason or 'Manual promotion'}",
    ]

    lineage = "\n".join(lines)

    create_markdown_artifact(
        key=f"promotion-lineage-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        markdown=lineage,
        description=f"Model promotion lineage for {model_info['model_key']}",
    )

    logger.info("Promotion lineage artifact created")
    return lineage


@flow(name="model-promotion")
def promotion_flow(
    model_key: str,
    update_runtime_block: bool = True,
    runtime_block_name: str = "eta-runtime-settings/default",
    update_redis: bool = False,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_db: int = 0,
    redis_password: Optional[str] = None,
    redis_config_key: str = "eta:active_model",
    warm_cache: bool = True,
    promotion_reason: Optional[str] = None,
    notification_blocks: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Promote a model to be the active model for ETA predictions.

    Args:
        model_key: The model key to promote
        update_runtime_block: Whether to update the Prefect runtime block
        runtime_block_name: Name of the EtaRuntimeSettings block to update
        update_redis: Whether to update the active model config in Redis
        redis_*: Redis connection parameters
        redis_config_key: Redis key for storing active model config
        warm_cache: Whether to pre-load the model to warm caches
        promotion_reason: Optional reason for the promotion (for audit)
        notification_blocks: Prefect blocks for notifications

    Returns:
        Dictionary with promotion results
    """
    logger = get_run_logger()
    notification_blocks = notification_blocks or []

    logger.info("Starting promotion flow for model: %s", model_key)

    # Validate model exists
    model_info = validate_model_exists_task(model_key)

    # Get current active model for lineage
    previous_model = None
    if update_runtime_block:
        try:
            sys.path.insert(0, str(REPO_ROOT / "prefect"))
            from runtime_config import EtaRuntimeSettings
            block = EtaRuntimeSettings.load(runtime_block_name)
            previous_model = block.model_key
        except Exception:
            pass

    # Update configurations
    if update_runtime_block:
        update_runtime_block_task(model_key, runtime_block_name)

    if update_redis:
        update_redis_config_task(
            model_key=model_key,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            redis_password=redis_password,
            config_key=redis_config_key,
        )

    # Warm cache
    cache_warmed = False
    if warm_cache:
        cache_warmed = warm_model_cache_task(model_key)

    # Create lineage
    lineage = create_promotion_lineage_task(
        model_info=model_info,
        previous_model=previous_model,
        promotion_reason=promotion_reason,
    )

    # Send notification
    if notification_blocks:
        _notify_blocks(
            notification_blocks,
            f"Model promoted: {model_key} (was: {previous_model or 'none'})",
            logger,
        )

    logger.info("Promotion flow complete for model: %s", model_key)

    return {
        "success": True,
        "model_key": model_key,
        "previous_model": previous_model,
        "runtime_block_updated": update_runtime_block,
        "redis_updated": update_redis,
        "cache_warmed": cache_warmed,
    }


if __name__ == "__main__":
    # Example usage (requires a valid model key)
    # promotion_flow(
    #     model_key="xgboost_...",
    #     update_runtime_block=True,
    #     runtime_block_name="eta-runtime-settings/dev",
    # )
    print("Run with: prefect deployment run 'model-promotion/model-promotion'")
