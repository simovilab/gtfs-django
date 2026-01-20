"""
Core utilities for the ETA prediction system.

This module provides centralized configuration, logging, validation, and error handling
to ensure consistency across all components (eta_service, models, prefect, bytewax).
"""

from core.config import get_config, ProjectConfig
from core.logging import get_logger, LogLevel
from core.exceptions import (
    ETAError,
    ConfigurationError,
    ValidationError,
    ModelError,
    ModelNotFoundError,
    ModelLoadError,
    PredictionError,
    DataError,
    RedisError,
)
from core.validation import (
    validate_vehicle_position,
    validate_stop,
    validate_stops_list,
    VehiclePosition,
    Stop,
)

__all__ = [
    # Config
    "get_config",
    "ProjectConfig",
    # Logging
    "get_logger",
    "LogLevel",
    # Exceptions
    "ETAError",
    "ConfigurationError",
    "ValidationError",
    "ModelError",
    "ModelNotFoundError",
    "ModelLoadError",
    "PredictionError",
    "DataError",
    "RedisError",
    # Validation
    "validate_vehicle_position",
    "validate_stop",
    "validate_stops_list",
    "VehiclePosition",
    "Stop",
]
