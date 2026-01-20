"""
Custom exceptions for the ETA prediction system.

Provides a consistent exception hierarchy for error handling across all components.
All exceptions include context information for debugging.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class ETAError(Exception):
    """
    Base exception for all ETA system errors.

    Attributes:
        message: Human-readable error message
        code: Machine-readable error code for programmatic handling
        context: Additional context about the error
    """

    code: str = "ETA_ERROR"

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        **context: Any,
    ):
        self.message = message
        if code:
            self.code = code
        self.context = context
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the full error message with context."""
        msg = f"[{self.code}] {self.message}"
        if self.context:
            ctx_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            msg = f"{msg} ({ctx_str})"
        return msg

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to a dictionary for JSON serialization."""
        return {
            "error": self.code,
            "message": self.message,
            "context": self.context,
        }


# =============================================================================
# Configuration Errors
# =============================================================================


class ConfigurationError(ETAError):
    """Raised when there's a configuration problem."""

    code = "CONFIG_ERROR"


class MissingConfigError(ConfigurationError):
    """Raised when a required configuration value is missing."""

    code = "MISSING_CONFIG"

    def __init__(self, config_key: str, **context: Any):
        super().__init__(
            f"Missing required configuration: {config_key}",
            config_key=config_key,
            **context,
        )


# =============================================================================
# Validation Errors
# =============================================================================


class ValidationError(ETAError):
    """Raised when input validation fails."""

    code = "VALIDATION_ERROR"


class InvalidVehiclePositionError(ValidationError):
    """Raised when a vehicle position payload is invalid."""

    code = "INVALID_VEHICLE_POSITION"

    def __init__(self, reason: str, vehicle_id: Optional[str] = None, **context: Any):
        super().__init__(
            f"Invalid vehicle position: {reason}",
            vehicle_id=vehicle_id,
            **context,
        )


class InvalidStopError(ValidationError):
    """Raised when a stop payload is invalid."""

    code = "INVALID_STOP"

    def __init__(self, reason: str, stop_id: Optional[str] = None, **context: Any):
        super().__init__(
            f"Invalid stop data: {reason}",
            stop_id=stop_id,
            **context,
        )


class InvalidRouteError(ValidationError):
    """Raised when route data is invalid."""

    code = "INVALID_ROUTE"

    def __init__(self, reason: str, route_id: Optional[str] = None, **context: Any):
        super().__init__(
            f"Invalid route data: {reason}",
            route_id=route_id,
            **context,
        )


# =============================================================================
# Model Errors
# =============================================================================


class ModelError(ETAError):
    """Base exception for model-related errors."""

    code = "MODEL_ERROR"


class ModelNotFoundError(ModelError):
    """Raised when a requested model doesn't exist."""

    code = "MODEL_NOT_FOUND"

    def __init__(
        self,
        model_key: Optional[str] = None,
        model_type: Optional[str] = None,
        route_id: Optional[str] = None,
        **context: Any,
    ):
        msg = "Model not found"
        if model_key:
            msg = f"Model not found: {model_key}"
        elif model_type:
            route_info = f" for route {route_id}" if route_id else ""
            msg = f"No {model_type} model found{route_info}"

        super().__init__(
            msg,
            model_key=model_key,
            model_type=model_type,
            route_id=route_id,
            **context,
        )


class ModelLoadError(ModelError):
    """Raised when a model fails to load."""

    code = "MODEL_LOAD_ERROR"

    def __init__(self, model_key: str, reason: str, **context: Any):
        super().__init__(
            f"Failed to load model {model_key}: {reason}",
            model_key=model_key,
            **context,
        )


class ModelMetadataError(ModelError):
    """Raised when model metadata is invalid or missing."""

    code = "MODEL_METADATA_ERROR"

    def __init__(self, model_key: str, reason: str, **context: Any):
        super().__init__(
            f"Model metadata error for {model_key}: {reason}",
            model_key=model_key,
            **context,
        )


# =============================================================================
# Prediction Errors
# =============================================================================


class PredictionError(ETAError):
    """Raised when a prediction fails."""

    code = "PREDICTION_ERROR"

    def __init__(
        self,
        reason: str,
        vehicle_id: Optional[str] = None,
        model_key: Optional[str] = None,
        **context: Any,
    ):
        super().__init__(
            f"Prediction failed: {reason}",
            vehicle_id=vehicle_id,
            model_key=model_key,
            **context,
        )


class FeatureExtractionError(PredictionError):
    """Raised when feature extraction fails."""

    code = "FEATURE_EXTRACTION_ERROR"

    def __init__(self, reason: str, **context: Any):
        # Don't call super().__init__ with PredictionError's signature
        ETAError.__init__(self, f"Feature extraction failed: {reason}", **context)


# =============================================================================
# Data Errors
# =============================================================================


class DataError(ETAError):
    """Base exception for data-related errors."""

    code = "DATA_ERROR"


class DatasetError(DataError):
    """Raised when there's a problem with a dataset."""

    code = "DATASET_ERROR"

    def __init__(self, reason: str, dataset_path: Optional[str] = None, **context: Any):
        super().__init__(
            f"Dataset error: {reason}",
            dataset_path=dataset_path,
            **context,
        )


class MissingDataError(DataError):
    """Raised when required data is missing."""

    code = "MISSING_DATA"

    def __init__(self, data_type: str, identifier: Optional[str] = None, **context: Any):
        msg = f"Missing {data_type}"
        if identifier:
            msg = f"{msg}: {identifier}"
        super().__init__(msg, data_type=data_type, identifier=identifier, **context)


# =============================================================================
# Redis Errors
# =============================================================================


class RedisError(ETAError):
    """Base exception for Redis-related errors."""

    code = "REDIS_ERROR"


class RedisConnectionError(RedisError):
    """Raised when Redis connection fails."""

    code = "REDIS_CONNECTION_ERROR"

    def __init__(self, host: str, port: int, reason: str, **context: Any):
        super().__init__(
            f"Failed to connect to Redis at {host}:{port}: {reason}",
            host=host,
            port=port,
            **context,
        )


class RedisKeyError(RedisError):
    """Raised when a Redis key operation fails."""

    code = "REDIS_KEY_ERROR"

    def __init__(self, key: str, operation: str, reason: str, **context: Any):
        super().__init__(
            f"Redis {operation} failed for key {key}: {reason}",
            key=key,
            operation=operation,
            **context,
        )
