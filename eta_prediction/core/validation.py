"""
Input validation utilities for the ETA prediction system.

Provides validation functions and data classes for:
- Vehicle position payloads (from Redis/MQTT)
- Stop definitions
- Route data

All validation happens at system boundaries to catch errors early.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from core.exceptions import (
    InvalidVehiclePositionError,
    InvalidStopError,
    InvalidRouteError,
    ValidationError,
)


# =============================================================================
# Coordinate Validation
# =============================================================================


def _is_valid_latitude(lat: float) -> bool:
    """Check if latitude is within valid range."""
    return -90.0 <= lat <= 90.0


def _is_valid_longitude(lon: float) -> bool:
    """Check if longitude is within valid range."""
    return -180.0 <= lon <= 180.0


def _coerce_float(value: Any, field_name: str) -> Optional[float]:
    """
    Safely coerce a value to float.

    Returns None if the value cannot be converted.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any, field_name: str) -> Optional[int]:
    """Safely coerce a value to int."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_str(value: Any) -> Optional[str]:
    """Safely coerce a value to string."""
    if value is None:
        return None
    return str(value)


# =============================================================================
# Vehicle Position
# =============================================================================


@dataclass
class VehiclePosition:
    """
    Validated vehicle position data.

    This dataclass represents a cleaned and validated vehicle position
    that can be safely used for ETA predictions.
    """

    vehicle_id: str
    lat: float
    lon: float
    timestamp: str
    route_id: str
    speed: float = 0.0
    trip_id: Optional[str] = None
    bearing: Optional[float] = None
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for compatibility with existing code."""
        return {
            "vehicle_id": self.vehicle_id,
            "lat": self.lat,
            "lon": self.lon,
            "timestamp": self.timestamp,
            "route": self.route_id,
            "route_id": self.route_id,
            "speed": self.speed,
            "trip_id": self.trip_id,
            "bearing": self.bearing,
        }


def validate_vehicle_position(
    data: Dict[str, Any],
    strict: bool = False,
) -> VehiclePosition:
    """
    Validate and normalize a vehicle position payload.

    Args:
        data: Raw vehicle position dictionary (from Redis/MQTT)
        strict: If True, raise on any validation issue. If False, try to
                extract what we can and only fail on critical missing fields.

    Returns:
        VehiclePosition with validated data

    Raises:
        InvalidVehiclePositionError: If validation fails

    Example:
        >>> from core.validation import validate_vehicle_position
        >>> raw = {"id": "V123", "lat": 9.93, "lon": -84.08, "timestamp": "2024-01-15T10:00:00Z", "route": "171"}
        >>> vp = validate_vehicle_position(raw)
        >>> print(vp.vehicle_id, vp.lat, vp.lon)
    """
    if not isinstance(data, dict):
        raise InvalidVehiclePositionError("Input must be a dictionary")

    # Extract vehicle_id (multiple possible field names)
    vehicle_id = (
        data.get("vehicle_id")
        or data.get("vehicleId")
        or data.get("id")
        or (data.get("vehicle") or {}).get("id")
    )
    if not vehicle_id:
        raise InvalidVehiclePositionError("Missing vehicle_id")
    vehicle_id = str(vehicle_id)

    # Extract and validate coordinates
    lat = _coerce_float(data.get("lat") or data.get("latitude"), "lat")
    lon = _coerce_float(data.get("lon") or data.get("longitude") or data.get("lng"), "lon")

    if lat is None:
        raise InvalidVehiclePositionError("Missing or invalid latitude", vehicle_id=vehicle_id)
    if lon is None:
        raise InvalidVehiclePositionError("Missing or invalid longitude", vehicle_id=vehicle_id)

    if not _is_valid_latitude(lat):
        raise InvalidVehiclePositionError(
            f"Latitude {lat} out of range [-90, 90]",
            vehicle_id=vehicle_id,
            lat=lat,
        )
    if not _is_valid_longitude(lon):
        raise InvalidVehiclePositionError(
            f"Longitude {lon} out of range [-180, 180]",
            vehicle_id=vehicle_id,
            lon=lon,
        )

    # Extract timestamp
    timestamp = data.get("timestamp")
    if not timestamp:
        raise InvalidVehiclePositionError("Missing timestamp", vehicle_id=vehicle_id)
    timestamp = str(timestamp)

    # Validate timestamp format (basic check)
    if strict:
        try:
            # Try to parse ISO format
            ts = timestamp.replace("Z", "+00:00")
            datetime.fromisoformat(ts)
        except ValueError as e:
            raise InvalidVehiclePositionError(
                f"Invalid timestamp format: {e}",
                vehicle_id=vehicle_id,
                timestamp=timestamp,
            )

    # Extract route_id (multiple possible field names)
    route_id = (
        data.get("route_id")
        or data.get("route")
        or data.get("routeId")
        or data.get("route_short_name")
    )
    if not route_id:
        raise InvalidVehiclePositionError("Missing route_id", vehicle_id=vehicle_id)
    route_id = str(route_id)

    # Extract optional fields
    speed = _coerce_float(data.get("speed"), "speed") or 0.0
    if speed < 0:
        speed = 0.0  # Clamp negative speeds

    trip_id = _coerce_str(data.get("trip_id") or data.get("trip") or data.get("tripId"))
    bearing = _coerce_float(data.get("bearing") or data.get("heading"), "bearing")

    return VehiclePosition(
        vehicle_id=vehicle_id,
        lat=lat,
        lon=lon,
        timestamp=timestamp,
        route_id=route_id,
        speed=speed,
        trip_id=trip_id,
        bearing=bearing,
        raw_data=data,
    )


# =============================================================================
# Stop
# =============================================================================


@dataclass
class Stop:
    """
    Validated stop data.

    Represents a transit stop with validated coordinates and sequence information.
    """

    stop_id: str
    lat: float
    lon: float
    stop_sequence: int
    stop_name: Optional[str] = None
    total_stop_sequence: Optional[int] = None
    raw_data: Dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for compatibility with existing code."""
        result = {
            "stop_id": self.stop_id,
            "lat": self.lat,
            "lon": self.lon,
            "stop_sequence": self.stop_sequence,
        }
        if self.stop_name:
            result["stop_name"] = self.stop_name
        if self.total_stop_sequence:
            result["total_stop_sequence"] = self.total_stop_sequence
        return result


def validate_stop(
    data: Dict[str, Any],
    index: Optional[int] = None,
    total_stops: Optional[int] = None,
) -> Stop:
    """
    Validate and normalize a stop payload.

    Args:
        data: Raw stop dictionary
        index: Optional index in the stops list (1-based, used as fallback for stop_sequence)
        total_stops: Optional total number of stops (used as fallback for total_stop_sequence)

    Returns:
        Stop with validated data

    Raises:
        InvalidStopError: If validation fails
    """
    if not isinstance(data, dict):
        raise InvalidStopError("Input must be a dictionary")

    # Extract stop_id
    stop_id = data.get("stop_id") or data.get("stopId") or data.get("id")
    if not stop_id:
        raise InvalidStopError("Missing stop_id")
    stop_id = str(stop_id)

    # Extract and validate coordinates
    lat = _coerce_float(data.get("lat") or data.get("latitude") or data.get("stop_lat"), "lat")
    lon = _coerce_float(
        data.get("lon") or data.get("longitude") or data.get("lng") or data.get("stop_lon"),
        "lon",
    )

    if lat is None:
        raise InvalidStopError("Missing or invalid latitude", stop_id=stop_id)
    if lon is None:
        raise InvalidStopError("Missing or invalid longitude", stop_id=stop_id)

    if not _is_valid_latitude(lat):
        raise InvalidStopError(f"Latitude {lat} out of range", stop_id=stop_id, lat=lat)
    if not _is_valid_longitude(lon):
        raise InvalidStopError(f"Longitude {lon} out of range", stop_id=stop_id, lon=lon)

    # Extract stop_sequence (multiple fallbacks)
    stop_sequence = _coerce_int(
        data.get("stop_sequence") or data.get("sequence") or data.get("stop_order"),
        "stop_sequence",
    )
    if stop_sequence is None:
        stop_sequence = index if index is not None else 1

    # Extract optional fields
    stop_name = _coerce_str(data.get("stop_name") or data.get("name"))

    total_stop_sequence = _coerce_int(
        data.get("total_stop_sequence") or data.get("total_stops"),
        "total_stop_sequence",
    )
    if total_stop_sequence is None:
        total_stop_sequence = total_stops

    return Stop(
        stop_id=stop_id,
        lat=lat,
        lon=lon,
        stop_sequence=stop_sequence,
        stop_name=stop_name,
        total_stop_sequence=total_stop_sequence,
        raw_data=data,
    )


def validate_stops_list(
    data: Any,
    min_stops: int = 0,
) -> List[Stop]:
    """
    Validate and normalize a list of stops.

    Handles both raw lists and dicts with a 'stops' key.

    Args:
        data: Raw stops data (list or dict with 'stops' key)
        min_stops: Minimum number of stops required (0 = no minimum)

    Returns:
        List of validated Stop objects

    Raises:
        InvalidStopError: If validation fails
    """
    # Handle dict wrapper
    if isinstance(data, dict):
        if "stops" in data:
            stops_raw = data["stops"]
        else:
            raise InvalidStopError("Dictionary must contain 'stops' key")
    elif isinstance(data, list):
        stops_raw = data
    else:
        raise InvalidStopError("Input must be a list or dict with 'stops' key")

    if not isinstance(stops_raw, list):
        raise InvalidStopError("Stops data must be a list")

    total_stops = len(stops_raw)

    if total_stops < min_stops:
        raise InvalidStopError(
            f"Expected at least {min_stops} stops, got {total_stops}",
            expected=min_stops,
            actual=total_stops,
        )

    validated_stops: List[Stop] = []
    errors: List[str] = []

    for idx, stop_data in enumerate(stops_raw, start=1):
        try:
            stop = validate_stop(stop_data, index=idx, total_stops=total_stops)
            validated_stops.append(stop)
        except InvalidStopError as e:
            errors.append(f"Stop {idx}: {e.message}")

    if errors and len(errors) == total_stops:
        # All stops failed validation
        raise InvalidStopError(f"All stops failed validation: {'; '.join(errors[:3])}")

    return validated_stops


# =============================================================================
# Utility Functions
# =============================================================================


def validate_coordinates(lat: Any, lon: Any) -> Tuple[float, float]:
    """
    Validate a latitude/longitude pair.

    Args:
        lat: Latitude value
        lon: Longitude value

    Returns:
        Tuple of (lat, lon) as floats

    Raises:
        ValidationError: If coordinates are invalid
    """
    lat_f = _coerce_float(lat, "lat")
    lon_f = _coerce_float(lon, "lon")

    if lat_f is None:
        raise ValidationError(f"Invalid latitude: {lat}")
    if lon_f is None:
        raise ValidationError(f"Invalid longitude: {lon}")

    if not _is_valid_latitude(lat_f):
        raise ValidationError(f"Latitude {lat_f} out of range [-90, 90]")
    if not _is_valid_longitude(lon_f):
        raise ValidationError(f"Longitude {lon_f} out of range [-180, 180]")

    return lat_f, lon_f


def is_valid_vehicle_position(data: Dict[str, Any]) -> bool:
    """
    Check if a vehicle position payload is valid without raising exceptions.

    Returns True if valid, False otherwise.
    """
    try:
        validate_vehicle_position(data)
        return True
    except (InvalidVehiclePositionError, ValidationError):
        return False


def is_valid_stop(data: Dict[str, Any]) -> bool:
    """
    Check if a stop payload is valid without raising exceptions.

    Returns True if valid, False otherwise.
    """
    try:
        validate_stop(data)
        return True
    except (InvalidStopError, ValidationError):
        return False
