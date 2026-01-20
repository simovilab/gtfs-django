"""
Structured logging utility for the ETA prediction system.

Provides consistent logging across all components with support for:
- Multiple log levels (DEBUG, INFO, WARNING, ERROR)
- Structured fields for log parsing
- Component-based prefixes
- Integration with Prefect logging when available
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


class LogLevel(Enum):
    """Log levels for the ETA system."""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR


class StructuredFormatter(logging.Formatter):
    """
    Formatter that outputs structured JSON for production or readable text for development.
    """

    def __init__(self, json_output: bool = False, include_timestamp: bool = True):
        super().__init__()
        self.json_output = json_output
        self.include_timestamp = include_timestamp

    def format(self, record: logging.LogRecord) -> str:
        # Extract structured fields if present
        extra_fields: Dict[str, Any] = {}
        for key in list(vars(record).keys()):
            if key.startswith("_eta_"):
                extra_fields[key[5:]] = getattr(record, key)

        if self.json_output:
            log_dict = {
                "level": record.levelname,
                "message": record.getMessage(),
                "component": getattr(record, "_eta_component", record.name),
            }
            if self.include_timestamp:
                log_dict["timestamp"] = datetime.now(timezone.utc).isoformat()
            if extra_fields:
                log_dict["fields"] = extra_fields
            if record.exc_info:
                log_dict["exception"] = self.formatException(record.exc_info)
            return json.dumps(log_dict)
        else:
            # Human-readable format
            timestamp = ""
            if self.include_timestamp:
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S ")

            component = getattr(record, "_eta_component", record.name)
            level = record.levelname[0]  # D, I, W, E

            msg = f"{timestamp}[{level}] [{component}] {record.getMessage()}"

            # Append extra fields if any
            if extra_fields:
                fields_str = " ".join(f"{k}={v}" for k, v in extra_fields.items())
                msg = f"{msg} | {fields_str}"

            if record.exc_info:
                msg = f"{msg}\n{self.formatException(record.exc_info)}"

            return msg


@dataclass
class ETALogger:
    """
    Structured logger for ETA system components.

    Usage:
        from core.logging import get_logger

        logger = get_logger("estimator")
        logger.info("Processing vehicle", vehicle_id="V123", route="171")
        logger.error("Model not found", model_key="xgb_global")
    """

    component: str
    level: LogLevel = LogLevel.INFO
    json_output: bool = False
    _logger: logging.Logger = field(init=False, repr=False)
    _prefect_logger: Optional[Any] = field(init=False, repr=False, default=None)

    def __post_init__(self):
        """Initialize the underlying Python logger."""
        self._logger = logging.getLogger(f"eta.{self.component}")
        self._logger.setLevel(self.level.value)

        # Remove existing handlers to avoid duplicates
        self._logger.handlers.clear()

        # Add console handler with our formatter
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(self.level.value)
        handler.setFormatter(StructuredFormatter(
            json_output=self.json_output,
            include_timestamp=True
        ))
        self._logger.addHandler(handler)

        # Don't propagate to root logger
        self._logger.propagate = False

        # Try to get Prefect logger if we're in a Prefect context
        self._try_get_prefect_logger()

    def _try_get_prefect_logger(self) -> None:
        """Attempt to get Prefect's logger if running in a Prefect context."""
        try:
            from prefect import get_run_logger
            self._prefect_logger = get_run_logger()
        except Exception:
            self._prefect_logger = None

    def _log(self, level: int, msg: str, **fields: Any) -> None:
        """Internal logging method that handles both standard and Prefect logging."""
        # Create extra dict with prefixed keys
        extra = {f"_eta_{k}": v for k, v in fields.items()}
        extra["_eta_component"] = self.component

        self._logger.log(level, msg, extra=extra)

        # Also log to Prefect if available (without structured fields since Prefect
        # has its own formatting)
        if self._prefect_logger:
            prefect_msg = msg
            if fields:
                fields_str = " ".join(f"{k}={v}" for k, v in fields.items())
                prefect_msg = f"[{self.component}] {msg} | {fields_str}"
            else:
                prefect_msg = f"[{self.component}] {msg}"

            if level == logging.DEBUG:
                self._prefect_logger.debug(prefect_msg)
            elif level == logging.INFO:
                self._prefect_logger.info(prefect_msg)
            elif level == logging.WARNING:
                self._prefect_logger.warning(prefect_msg)
            elif level == logging.ERROR:
                self._prefect_logger.error(prefect_msg)

    def debug(self, msg: str, **fields: Any) -> None:
        """Log a debug message."""
        self._log(logging.DEBUG, msg, **fields)

    def info(self, msg: str, **fields: Any) -> None:
        """Log an info message."""
        self._log(logging.INFO, msg, **fields)

    def warning(self, msg: str, **fields: Any) -> None:
        """Log a warning message."""
        self._log(logging.WARNING, msg, **fields)

    def warn(self, msg: str, **fields: Any) -> None:
        """Alias for warning()."""
        self.warning(msg, **fields)

    def error(self, msg: str, exc_info: bool = False, **fields: Any) -> None:
        """Log an error message."""
        extra = {f"_eta_{k}": v for k, v in fields.items()}
        extra["_eta_component"] = self.component
        self._logger.error(msg, extra=extra, exc_info=exc_info)

        if self._prefect_logger:
            prefect_msg = f"[{self.component}] {msg}"
            if fields:
                fields_str = " ".join(f"{k}={v}" for k, v in fields.items())
                prefect_msg = f"{prefect_msg} | {fields_str}"
            self._prefect_logger.error(prefect_msg)

    def set_level(self, level: LogLevel) -> None:
        """Change the log level at runtime."""
        self.level = level
        self._logger.setLevel(level.value)
        for handler in self._logger.handlers:
            handler.setLevel(level.value)


# Logger cache to avoid creating duplicate loggers
_loggers: Dict[str, ETALogger] = {}


def get_logger(
    component: str,
    level: Optional[LogLevel] = None,
    json_output: bool = False,
) -> ETALogger:
    """
    Get or create a logger for the specified component.

    Args:
        component: Name of the component (e.g., "estimator", "registry", "prefect")
        level: Log level (defaults to INFO, or DEBUG if ETA_DEBUG env var is set)
        json_output: If True, output logs as JSON (useful for production)

    Returns:
        ETALogger instance

    Usage:
        from core.logging import get_logger

        logger = get_logger("estimator")
        logger.info("Starting prediction", vehicle_id="V123")
        logger.warning("Missing route stops", route_id="171")
        logger.error("Model failed to load", model_key="xgb_global", exc_info=True)
    """
    import os

    # Determine default level
    if level is None:
        if os.environ.get("ETA_DEBUG", "").lower() in ("1", "true", "yes"):
            level = LogLevel.DEBUG
        else:
            level = LogLevel.INFO

    # Check for JSON output env var
    if os.environ.get("ETA_LOG_JSON", "").lower() in ("1", "true", "yes"):
        json_output = True

    cache_key = f"{component}:{level.name}:{json_output}"

    if cache_key not in _loggers:
        _loggers[cache_key] = ETALogger(
            component=component,
            level=level,
            json_output=json_output,
        )

    return _loggers[cache_key]


def configure_root_logging(level: LogLevel = LogLevel.INFO, json_output: bool = False) -> None:
    """
    Configure the root logger for the entire application.

    Call this once at application startup if you want consistent formatting
    for all loggers (including third-party libraries).
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level.value)

    # Remove existing handlers
    root_logger.handlers.clear()

    # Add our formatter
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level.value)
    handler.setFormatter(StructuredFormatter(json_output=json_output))
    root_logger.addHandler(handler)
