"""
Tests for core utilities: config, logging, validation, and exceptions.

Run with: pytest core/tests/test_core.py -v
"""

import os
import pytest
from datetime import datetime, timezone


class TestConfig:
    """Tests for core.config module."""

    def test_get_config_returns_singleton(self):
        """Config should return the same instance."""
        from core.config import get_config, reset_config

        reset_config()  # Start fresh
        config1 = get_config()
        config2 = get_config()
        assert config1 is config2

    def test_config_project_root_exists(self):
        """Project root should be a valid directory."""
        from core.config import get_config

        config = get_config()
        assert config.project_root.exists()
        assert config.project_root.is_dir()

    def test_config_model_registry_dir(self):
        """Model registry dir should be configurable via env var."""
        from core.config import get_config, reset_config

        # Test default
        reset_config()
        if "MODEL_REGISTRY_DIR" in os.environ:
            del os.environ["MODEL_REGISTRY_DIR"]
        config = get_config()
        assert "trained" in str(config.model_registry_dir)

    def test_config_default_values(self):
        """Config should have sensible defaults."""
        from core.config import get_config

        config = get_config()
        assert config.default_timezone == "America/Costa_Rica"
        assert config.default_region == "CR"
        assert config.redis_default_port == 6379
        assert config.predictions_ttl_seconds == 300

    def test_config_paths(self):
        """Config should provide valid paths."""
        from core.config import get_config

        config = get_config()
        assert config.models_dir == config.project_root / "models"
        assert config.feature_engineering_dir == config.project_root / "feature_engineering"
        assert config.eta_service_dir == config.project_root / "eta_service"


class TestLogging:
    """Tests for core.logging module."""

    def test_get_logger_returns_logger(self):
        """get_logger should return an ETALogger instance."""
        from core.logging import get_logger, ETALogger

        logger = get_logger("test")
        assert isinstance(logger, ETALogger)

    def test_logger_levels(self):
        """Logger should support all standard levels."""
        from core.logging import get_logger

        logger = get_logger("test_levels")
        # These should not raise
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")

    def test_logger_with_fields(self):
        """Logger should accept structured fields."""
        from core.logging import get_logger

        logger = get_logger("test_fields")
        # This should not raise
        logger.info("Test message", vehicle_id="V123", route="171")

    def test_logger_caching(self):
        """Loggers should be cached by component name."""
        from core.logging import get_logger

        logger1 = get_logger("cached_test")
        logger2 = get_logger("cached_test")
        # Same component should return same logger (cached)
        assert logger1 is logger2


class TestExceptions:
    """Tests for core.exceptions module."""

    def test_base_exception(self):
        """ETAError should format messages correctly."""
        from core.exceptions import ETAError

        err = ETAError("Test error", code="TEST_CODE", key1="val1")
        assert "TEST_CODE" in str(err)
        assert "Test error" in str(err)
        assert "key1=val1" in str(err)

    def test_exception_to_dict(self):
        """Exceptions should convert to dict for JSON serialization."""
        from core.exceptions import ETAError

        err = ETAError("Test", foo="bar")
        d = err.to_dict()
        assert d["message"] == "Test"
        assert d["context"]["foo"] == "bar"

    def test_model_not_found_error(self):
        """ModelNotFoundError should include model details."""
        from core.exceptions import ModelNotFoundError

        err = ModelNotFoundError(model_key="xgb_global")
        assert "xgb_global" in str(err)
        assert "MODEL_NOT_FOUND" in str(err)

    def test_validation_error_hierarchy(self):
        """Validation errors should inherit from ETAError."""
        from core.exceptions import (
            ETAError,
            ValidationError,
            InvalidVehiclePositionError,
            InvalidStopError,
        )

        assert issubclass(ValidationError, ETAError)
        assert issubclass(InvalidVehiclePositionError, ValidationError)
        assert issubclass(InvalidStopError, ValidationError)


class TestValidation:
    """Tests for core.validation module."""

    def test_validate_vehicle_position_valid(self):
        """Valid vehicle position should pass validation."""
        from core.validation import validate_vehicle_position, VehiclePosition

        data = {
            "vehicle_id": "V123",
            "lat": 9.93,
            "lon": -84.08,
            "timestamp": "2024-01-15T10:00:00Z",
            "route": "171",
        }
        result = validate_vehicle_position(data)
        assert isinstance(result, VehiclePosition)
        assert result.vehicle_id == "V123"
        assert result.lat == 9.93
        assert result.lon == -84.08
        assert result.route_id == "171"

    def test_validate_vehicle_position_missing_id(self):
        """Missing vehicle_id should raise error."""
        from core.validation import validate_vehicle_position
        from core.exceptions import InvalidVehiclePositionError

        data = {
            "lat": 9.93,
            "lon": -84.08,
            "timestamp": "2024-01-15T10:00:00Z",
            "route": "171",
        }
        with pytest.raises(InvalidVehiclePositionError) as exc_info:
            validate_vehicle_position(data)
        assert "vehicle_id" in str(exc_info.value).lower()

    def test_validate_vehicle_position_invalid_lat(self):
        """Invalid latitude should raise error."""
        from core.validation import validate_vehicle_position
        from core.exceptions import InvalidVehiclePositionError

        data = {
            "vehicle_id": "V123",
            "lat": 91.0,  # Invalid: > 90
            "lon": -84.08,
            "timestamp": "2024-01-15T10:00:00Z",
            "route": "171",
        }
        with pytest.raises(InvalidVehiclePositionError) as exc_info:
            validate_vehicle_position(data)
        assert "latitude" in str(exc_info.value).lower() or "lat" in str(exc_info.value).lower()

    def test_validate_vehicle_position_alternate_field_names(self):
        """Should accept alternate field names."""
        from core.validation import validate_vehicle_position

        # Using alternate field names
        data = {
            "id": "V456",
            "latitude": 9.93,
            "longitude": -84.08,
            "timestamp": "2024-01-15T10:00:00Z",
            "routeId": "172",
        }
        result = validate_vehicle_position(data)
        assert result.vehicle_id == "V456"
        assert result.route_id == "172"

    def test_validate_stop_valid(self):
        """Valid stop should pass validation."""
        from core.validation import validate_stop, Stop

        data = {
            "stop_id": "S100",
            "lat": 9.94,
            "lon": -84.07,
            "stop_sequence": 5,
        }
        result = validate_stop(data)
        assert isinstance(result, Stop)
        assert result.stop_id == "S100"
        assert result.stop_sequence == 5

    def test_validate_stop_missing_coordinates(self):
        """Missing coordinates should raise error."""
        from core.validation import validate_stop
        from core.exceptions import InvalidStopError

        data = {
            "stop_id": "S100",
            "stop_sequence": 5,
        }
        with pytest.raises(InvalidStopError):
            validate_stop(data)

    def test_validate_stops_list(self):
        """Should validate a list of stops."""
        from core.validation import validate_stops_list

        data = [
            {"stop_id": "S1", "lat": 9.93, "lon": -84.08},
            {"stop_id": "S2", "lat": 9.94, "lon": -84.07},
        ]
        result = validate_stops_list(data)
        assert len(result) == 2
        assert result[0].stop_id == "S1"
        assert result[0].stop_sequence == 1  # Auto-assigned
        assert result[1].stop_sequence == 2

    def test_validate_stops_list_from_dict(self):
        """Should handle dict with 'stops' key."""
        from core.validation import validate_stops_list

        data = {
            "stops": [
                {"stop_id": "S1", "lat": 9.93, "lon": -84.08},
            ]
        }
        result = validate_stops_list(data)
        assert len(result) == 1

    def test_is_valid_vehicle_position_helper(self):
        """is_valid_vehicle_position should return bool."""
        from core.validation import is_valid_vehicle_position

        valid_data = {
            "vehicle_id": "V1",
            "lat": 9.93,
            "lon": -84.08,
            "timestamp": "2024-01-15T10:00:00Z",
            "route": "171",
        }
        invalid_data = {"vehicle_id": "V1"}

        assert is_valid_vehicle_position(valid_data) is True
        assert is_valid_vehicle_position(invalid_data) is False

    def test_to_dict_compatibility(self):
        """Validated objects should convert back to dicts."""
        from core.validation import validate_vehicle_position, validate_stop

        vp_data = {
            "vehicle_id": "V1",
            "lat": 9.93,
            "lon": -84.08,
            "timestamp": "2024-01-15T10:00:00Z",
            "route": "171",
        }
        vp = validate_vehicle_position(vp_data)
        vp_dict = vp.to_dict()
        assert vp_dict["vehicle_id"] == "V1"
        assert vp_dict["lat"] == 9.93

        stop_data = {"stop_id": "S1", "lat": 9.93, "lon": -84.08}
        stop = validate_stop(stop_data)
        stop_dict = stop.to_dict()
        assert stop_dict["stop_id"] == "S1"


class TestIntegration:
    """Integration tests for core modules working together."""

    def test_logging_with_validation_error(self):
        """Logging should work with validation errors."""
        from core.logging import get_logger
        from core.validation import validate_vehicle_position
        from core.exceptions import InvalidVehiclePositionError

        logger = get_logger("integration_test")

        try:
            validate_vehicle_position({})
        except InvalidVehiclePositionError as e:
            logger.error("Validation failed", error=str(e), code=e.code)
            # Should not raise

    def test_config_paths_importable(self):
        """Modules should be importable after config initialization."""
        from core.config import get_config

        config = get_config()

        # Verify the path is set up correctly
        import sys
        assert str(config.project_root) in sys.path

        # These imports should work because config sets up sys.path
        from feature_engineering.temporal import extract_temporal_features

        assert callable(extract_temporal_features)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
