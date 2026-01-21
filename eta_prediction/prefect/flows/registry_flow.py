"""
Prefect flow for model registry health checks and maintenance.

Validates registry integrity, identifies issues, and handles
model pruning and archival.
"""

from __future__ import annotations

import json
import os
import shutil
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

from models.common.registry import get_registry, ModelRegistry


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


@task(name="check-registry-directory")
def check_registry_directory_task() -> Dict[str, Any]:
    """Check registry directory exists and is writable."""
    logger = get_run_logger()
    registry = get_registry()

    base_dir = registry.base_dir
    exists = base_dir.exists()
    writable = os.access(base_dir, os.W_OK) if exists else False

    # Count files
    pkl_files = list(base_dir.glob("*.pkl")) if exists else []
    json_files = list(base_dir.glob("*_meta.json")) if exists else []

    logger.info(
        "Registry directory: %s (exists=%s, writable=%s, pkl=%d, json=%d)",
        base_dir, exists, writable, len(pkl_files), len(json_files)
    )

    return {
        "base_dir": str(base_dir),
        "exists": exists,
        "writable": writable,
        "n_pkl_files": len(pkl_files),
        "n_json_files": len(json_files),
    }


@task(name="validate-registry-entries")
def validate_registry_entries_task() -> Dict[str, Any]:
    """Validate each registry entry has matching files."""
    logger = get_run_logger()
    registry = get_registry()

    valid_entries = []
    missing_pkl = []
    missing_meta = []
    invalid_meta = []
    orphan_files = []

    # Check each registry entry
    for model_key, entry in registry.registry.items():
        model_path = Path(entry.get("model_path", ""))
        meta_path = Path(entry.get("meta_path", ""))

        # Handle relative paths
        if not model_path.is_absolute():
            model_path = registry.base_dir / model_path.name
        if not meta_path.is_absolute():
            meta_path = registry.base_dir / meta_path.name

        has_pkl = model_path.exists()
        has_meta = meta_path.exists()

        if not has_pkl:
            missing_pkl.append(model_key)
            logger.warning("Missing pkl file for %s: %s", model_key, model_path)

        if not has_meta:
            missing_meta.append(model_key)
            logger.warning("Missing meta file for %s: %s", model_key, meta_path)

        # Validate metadata schema
        if has_meta:
            try:
                with open(meta_path) as f:
                    meta = json.load(f)

                # Check required fields
                required_fields = ["model_key", "model_type", "saved_at"]
                missing_fields = [f for f in required_fields if f not in meta]
                if missing_fields:
                    invalid_meta.append({
                        "model_key": model_key,
                        "missing_fields": missing_fields,
                    })
            except json.JSONDecodeError as e:
                invalid_meta.append({
                    "model_key": model_key,
                    "error": f"Invalid JSON: {e}",
                })

        if has_pkl and has_meta:
            valid_entries.append(model_key)

    # Check for orphan files (files without registry entries)
    all_pkl = set(p.stem for p in registry.base_dir.glob("*.pkl"))
    registered_keys = set(registry.registry.keys())
    orphan_keys = all_pkl - registered_keys
    orphan_files = list(orphan_keys)

    if orphan_files:
        logger.warning("Found %d orphan files: %s", len(orphan_files), orphan_files[:5])

    logger.info(
        "Registry validation: valid=%d, missing_pkl=%d, missing_meta=%d, invalid=%d, orphans=%d",
        len(valid_entries), len(missing_pkl), len(missing_meta),
        len(invalid_meta), len(orphan_files)
    )

    return {
        "valid_entries": valid_entries,
        "missing_pkl": missing_pkl,
        "missing_meta": missing_meta,
        "invalid_meta": invalid_meta,
        "orphan_files": orphan_files,
        "n_valid": len(valid_entries),
        "n_errors": len(missing_pkl) + len(missing_meta) + len(invalid_meta),
    }


@task(name="identify-stale-models")
def identify_stale_models_task(
    max_age_days: int = 90,
    keep_best_per_type: bool = True,
) -> List[Dict[str, Any]]:
    """Identify models that are candidates for archival."""
    logger = get_run_logger()
    registry = get_registry()

    cutoff_date = datetime.now() - timedelta(days=max_age_days)
    stale_models = []
    best_models = set()

    # If keeping best per type, identify them first
    if keep_best_per_type:
        models_df = registry.list_models()
        for model_type in models_df["model_type"].unique():
            best_key = registry.get_best_model(
                model_type=model_type,
                route_id="global",
                metric="test_mae_seconds",
            )
            if best_key:
                best_models.add(best_key)

    # Check each model
    for model_key in registry.registry.keys():
        try:
            metadata = registry.load_metadata(model_key)
            saved_at_str = metadata.get("saved_at")
            if not saved_at_str:
                continue

            saved_at = datetime.fromisoformat(saved_at_str.replace("Z", "+00:00"))
            saved_at = saved_at.replace(tzinfo=None)  # Make naive for comparison

            if saved_at < cutoff_date:
                # Check if it's a best model
                if keep_best_per_type and model_key in best_models:
                    logger.info("Keeping best model despite age: %s", model_key)
                    continue

                stale_models.append({
                    "model_key": model_key,
                    "model_type": metadata.get("model_type"),
                    "saved_at": saved_at_str,
                    "age_days": (datetime.now() - saved_at).days,
                })

        except Exception as exc:
            logger.warning("Failed to check model %s: %s", model_key, exc)

    logger.info("Identified %d stale models (older than %d days)", len(stale_models), max_age_days)
    return stale_models


@task(name="archive-models")
def archive_models_task(
    model_keys: List[str],
    archive_dir: str,
    delete_after_archive: bool = False,
) -> Dict[str, Any]:
    """Archive models to a specified directory."""
    logger = get_run_logger()
    registry = get_registry()

    archive_path = Path(archive_dir)
    archive_path.mkdir(parents=True, exist_ok=True)

    archived = []
    failed = []

    for model_key in model_keys:
        try:
            entry = registry.registry.get(model_key)
            if not entry:
                failed.append({"model_key": model_key, "error": "Not in registry"})
                continue

            model_path = Path(entry.get("model_path", ""))
            meta_path = Path(entry.get("meta_path", ""))

            if not model_path.is_absolute():
                model_path = registry.base_dir / model_path.name
            if not meta_path.is_absolute():
                meta_path = registry.base_dir / meta_path.name

            # Copy to archive
            if model_path.exists():
                shutil.copy2(model_path, archive_path / model_path.name)
            if meta_path.exists():
                shutil.copy2(meta_path, archive_path / meta_path.name)

            archived.append(model_key)

            # Optionally delete
            if delete_after_archive:
                registry.delete_model(model_key)
                logger.info("Archived and deleted: %s", model_key)
            else:
                logger.info("Archived (kept original): %s", model_key)

        except Exception as exc:
            failed.append({"model_key": model_key, "error": str(exc)})
            logger.error("Failed to archive %s: %s", model_key, exc)

    return {
        "archived": archived,
        "failed": failed,
        "archive_dir": str(archive_path),
    }


@task(name="create-health-report")
def create_health_report_task(
    dir_check: Dict[str, Any],
    validation: Dict[str, Any],
    stale_models: List[Dict[str, Any]],
) -> str:
    """Create a health report artifact."""
    logger = get_run_logger()

    lines = [
        "# Model Registry Health Report",
        "",
        f"**Timestamp:** {datetime.now().isoformat()}",
        "",
        "## Directory Status",
        "",
        f"- **Path:** `{dir_check['base_dir']}`",
        f"- **Exists:** {dir_check['exists']}",
        f"- **Writable:** {dir_check['writable']}",
        f"- **PKL Files:** {dir_check['n_pkl_files']}",
        f"- **Meta Files:** {dir_check['n_json_files']}",
        "",
        "## Validation Results",
        "",
        f"- **Valid Entries:** {validation['n_valid']}",
        f"- **Errors Found:** {validation['n_errors']}",
    ]

    if validation["missing_pkl"]:
        lines.extend([
            "",
            "### Missing PKL Files",
            "",
        ])
        for key in validation["missing_pkl"][:10]:
            lines.append(f"- `{key}`")
        if len(validation["missing_pkl"]) > 10:
            lines.append(f"- ... and {len(validation['missing_pkl']) - 10} more")

    if validation["missing_meta"]:
        lines.extend([
            "",
            "### Missing Meta Files",
            "",
        ])
        for key in validation["missing_meta"][:10]:
            lines.append(f"- `{key}`")

    if validation["invalid_meta"]:
        lines.extend([
            "",
            "### Invalid Metadata",
            "",
        ])
        for item in validation["invalid_meta"][:10]:
            lines.append(f"- `{item['model_key']}`: {item.get('error') or item.get('missing_fields')}")

    if validation["orphan_files"]:
        lines.extend([
            "",
            "### Orphan Files (no registry entry)",
            "",
        ])
        for key in validation["orphan_files"][:10]:
            lines.append(f"- `{key}`")

    if stale_models:
        lines.extend([
            "",
            "## Stale Models (candidates for archival)",
            "",
            "| Model | Type | Age (days) |",
            "|-------|------|------------|",
        ])
        for model in stale_models[:20]:
            lines.append(f"| `{model['model_key'][:30]}...` | {model['model_type']} | {model['age_days']} |")

    # Summary
    has_issues = validation["n_errors"] > 0 or validation["orphan_files"]
    status = "WARNING" if has_issues else "HEALTHY"

    lines.extend([
        "",
        "---",
        "",
        f"**Overall Status:** {status}",
    ])

    report = "\n".join(lines)

    create_markdown_artifact(
        key="registry-health-report",
        markdown=report,
        description="Model registry health check report",
    )

    logger.info("Health report artifact created (status: %s)", status)
    return report


@flow(name="registry-health-check")
def registry_health_flow(
    check_stale: bool = True,
    max_age_days: int = 90,
    archive_stale: bool = False,
    archive_dir: Optional[str] = None,
    delete_after_archive: bool = False,
    fail_on_errors: bool = False,
    notification_blocks: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Perform health check on the model registry.

    Args:
        check_stale: Whether to identify stale models
        max_age_days: Age threshold for stale model identification
        archive_stale: Whether to archive stale models
        archive_dir: Directory for archived models
        delete_after_archive: Whether to delete models after archiving
        fail_on_errors: Whether to raise exception on validation errors
        notification_blocks: Prefect blocks for notifications

    Returns:
        Dictionary with health check results
    """
    logger = get_run_logger()
    notification_blocks = notification_blocks or []

    logger.info("Starting registry health check")

    # Check directory
    dir_check = check_registry_directory_task()

    # Validate entries
    validation = validate_registry_entries_task()

    # Check stale models
    stale_models = []
    if check_stale:
        stale_models = identify_stale_models_task(
            max_age_days=max_age_days,
            keep_best_per_type=True,
        )

    # Archive if requested
    archive_result = None
    if archive_stale and stale_models and archive_dir:
        archive_result = archive_models_task(
            model_keys=[m["model_key"] for m in stale_models],
            archive_dir=archive_dir,
            delete_after_archive=delete_after_archive,
        )

    # Create report
    report = create_health_report_task(dir_check, validation, stale_models)

    # Send notification if there are issues
    has_errors = validation["n_errors"] > 0
    if notification_blocks and has_errors:
        _notify_blocks(
            notification_blocks,
            f"Registry health check found {validation['n_errors']} errors",
            logger,
        )

    # Optionally fail on errors
    if fail_on_errors and has_errors:
        raise RuntimeError(f"Registry validation found {validation['n_errors']} errors")

    logger.info("Registry health check complete")

    return {
        "success": True,
        "dir_check": dir_check,
        "validation": validation,
        "stale_models": stale_models,
        "archive_result": archive_result,
        "has_errors": has_errors,
    }


if __name__ == "__main__":
    # Test run
    result = registry_health_flow(
        check_stale=True,
        max_age_days=90,
        archive_stale=False,
    )
    print(f"Health check complete: {result['validation']['n_valid']} valid, {result['validation']['n_errors']} errors")
