"""
Centralized configuration and path management for the ETA prediction system.

This module eliminates scattered sys.path manipulation and provides a single source
of truth for all project paths and configuration.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


def _find_project_root() -> Path:
    """
    Find the project root by looking for pyproject.toml or .git directory.
    Falls back to the parent of the 'core' module directory.
    """
    # Start from this file's directory
    current = Path(__file__).resolve().parent

    # Walk up looking for markers
    for parent in [current] + list(current.parents):
        if (parent / "pyproject.toml").exists():
            return parent
        if (parent / ".git").exists():
            return parent
        # Stop at filesystem root
        if parent == parent.parent:
            break

    # Fallback: assume core/ is directly under project root
    return current.parent


@dataclass
class ProjectConfig:
    """
    Centralized project configuration.

    Usage:
        from core.config import get_config

        config = get_config()
        print(config.models_dir)
        print(config.model_registry_dir)
    """

    # Root paths
    project_root: Path = field(default_factory=_find_project_root)

    # Environment overrides (populated from env vars)
    _model_registry_override: Optional[str] = field(default=None, repr=False)
    _timezone_override: Optional[str] = field(default=None, repr=False)

    def __post_init__(self):
        """Load environment variable overrides."""
        self._model_registry_override = os.environ.get("MODEL_REGISTRY_DIR")
        self._timezone_override = os.environ.get("ETA_TIMEZONE")

        # Ensure project root is on sys.path for imports
        root_str = str(self.project_root)
        if root_str not in sys.path:
            sys.path.insert(0, root_str)

    # =========================================================================
    # Directory Paths
    # =========================================================================

    @property
    def core_dir(self) -> Path:
        """Path to core/ module."""
        return self.project_root / "core"

    @property
    def models_dir(self) -> Path:
        """Path to models/ directory."""
        return self.project_root / "models"

    @property
    def model_registry_dir(self) -> Path:
        """
        Path to trained model storage.

        Respects MODEL_REGISTRY_DIR environment variable if set.
        """
        if self._model_registry_override:
            return Path(self._model_registry_override).expanduser().resolve()
        return self.models_dir / "trained"

    @property
    def feature_engineering_dir(self) -> Path:
        """Path to feature_engineering/ module."""
        return self.project_root / "feature_engineering"

    @property
    def eta_service_dir(self) -> Path:
        """Path to eta_service/ module."""
        return self.project_root / "eta_service"

    @property
    def datasets_dir(self) -> Path:
        """Path to datasets/ directory."""
        return self.project_root / "datasets"

    @property
    def prefect_dir(self) -> Path:
        """Path to prefect/ directory."""
        return self.project_root / "prefect"

    @property
    def bytewax_dir(self) -> Path:
        """Path to bytewax/ directory."""
        return self.project_root / "bytewax"

    # =========================================================================
    # Default Values (eliminates magic numbers)
    # =========================================================================

    @property
    def default_timezone(self) -> str:
        """Default timezone for temporal features."""
        return self._timezone_override or "America/Costa_Rica"

    @property
    def default_region(self) -> str:
        """Default region for holiday calendar."""
        return "CR"

    # Redis defaults
    redis_default_host: str = "localhost"
    redis_default_port: int = 6379
    redis_default_db: int = 0

    # Cache settings
    predictions_ttl_seconds: int = 300
    route_stops_cache_size: int = 100
    shape_cache_size: int = 100

    # Distance thresholds (meters)
    min_distance_threshold_m: float = 10.0

    # Model defaults
    default_model_type: str = "xgboost"
    default_metric: str = "test_mae_seconds"

    # Weather defaults (placeholder values)
    default_temperature_c: float = 25.0
    default_precipitation_mm: float = 0.0
    default_wind_speed_kmh: Optional[float] = None

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def ensure_dirs_exist(self) -> None:
        """Create essential directories if they don't exist."""
        self.model_registry_dir.mkdir(parents=True, exist_ok=True)
        self.datasets_dir.mkdir(parents=True, exist_ok=True)

    def set_model_registry_dir(self, path: str) -> None:
        """
        Override the model registry directory at runtime.

        Also updates the environment variable for child processes.
        """
        resolved = str(Path(path).expanduser().resolve())
        self._model_registry_override = resolved
        os.environ["MODEL_REGISTRY_DIR"] = resolved

    def get_model_path(self, model_key: str) -> Path:
        """Get the full path for a model artifact."""
        return self.model_registry_dir / f"{model_key}.pkl"

    def get_model_metadata_path(self, model_key: str) -> Path:
        """Get the full path for a model's metadata file."""
        return self.model_registry_dir / f"{model_key}_meta.json"


# Global config instance (lazy initialization)
_config: Optional[ProjectConfig] = None


def get_config() -> ProjectConfig:
    """
    Get or create the global configuration instance.

    Usage:
        from core.config import get_config

        config = get_config()
        print(f"Project root: {config.project_root}")
        print(f"Model registry: {config.model_registry_dir}")
    """
    global _config
    if _config is None:
        _config = ProjectConfig()
    return _config


def reset_config() -> None:
    """Reset the global config (useful for testing)."""
    global _config
    _config = None
