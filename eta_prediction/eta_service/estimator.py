"""
ETA Service - Enhanced Implementation
Estimates stop arrival times from vehicle positions with route-specific model support.
"""

from datetime import datetime, timezone
import math

# Use centralized configuration (eliminates sys.path manipulation)
from core.config import get_config
from core.logging import get_logger
from core.exceptions import (
    ModelNotFoundError,
    ModelLoadError,
    PredictionError,
)
from core.validation import (
    validate_vehicle_position,
    validate_stops_list,
)
from core.exceptions import InvalidVehiclePositionError, InvalidStopError

# Initialize config to ensure paths are set up
_config = get_config()
_logger = get_logger("estimator")

from feature_engineering.temporal import extract_temporal_features
from models.common.registry import get_registry


def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in meters."""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c


def _progress_features(vehicle_position, stop, next_stop, total_segments_hint):
    """Approximate distance/progress metrics without requiring shapes."""
    vp_lat = vehicle_position["lat"]
    vp_lon = vehicle_position["lon"]
    stop_lat = stop["lat"]
    stop_lon = stop["lon"]

    distance_to_stop = haversine_distance(vp_lat, vp_lon, stop_lat, stop_lon)

    progress_on_segment = 0.0
    if next_stop:
        next_lat = next_stop["lat"]
        next_lon = next_stop["lon"]
        segment_length = haversine_distance(stop_lat, stop_lon, next_lat, next_lon)
        if segment_length == 0.0:
            distance_to_next = 0.0
        else:
            distance_to_next = haversine_distance(vp_lat, vp_lon, next_lat, next_lon)
            progress_on_segment = max(0.0, min(1.0, 1.0 - (distance_to_next / segment_length)))
    else:
        distance_to_next = None

    stop_seq = (
        stop.get("stop_sequence")
        or stop.get("sequence")
        or stop.get("stop_order")
        or 1
    )
    total_segments = (
        stop.get("total_stop_sequence")
        or total_segments_hint
        or stop_seq
    )
    completed = max(float(stop_seq) - 1.0, 0.0)
    denom = max(float(total_segments), 1.0)
    progress_ratio = max(0.0, min(1.0, (completed + progress_on_segment) / denom))

    return distance_to_stop, progress_on_segment, progress_ratio


def _predict_with_model(model_key, model_type, features, distance_m):
    """
    Call the appropriate predict_eta function based on model type.
    """

    if model_type == 'historical_mean':
        from models.historical_mean.predict import predict_eta
        return predict_eta(
            model_key=model_key,
            route_id=features.get('route_id', 'unknown'),
            stop_sequence=features.get('stop_sequence', 0),
            hour=features.get('hour', 0),
            day_of_week=features.get('day_of_week', 0),
            is_peak_hour=features.get('is_peak_hour', False)
        )

    elif model_type == 'ewma':
        from models.ewma.predict import predict_eta
        return predict_eta(
            model_key=model_key,
            route_id=features.get('route_id', 'unknown'),
            stop_sequence=features.get('stop_sequence', 0),
            hour=features.get('hour', 0)
        )

    elif model_type == 'polyreg_distance':
        from models.polyreg_distance.predict import predict_eta
        return predict_eta(
            model_key=model_key,
            distance_to_stop=distance_m
        )

    elif model_type == 'polyreg_time':
        from models.polyreg_time.predict import predict_eta
        return predict_eta(
            model_key=model_key,
            distance_to_stop=distance_m,
            progress_on_segment=features.get('progress_on_segment'),
            progress_ratio=features.get('progress_ratio'),
            hour=features.get('hour', 0),
            day_of_week=features.get('day_of_week', 0),
            is_peak_hour=features.get('is_peak_hour', False),
            is_weekend=features.get('is_weekend', False),
            is_holiday=features.get('is_holiday', False),
            temperature_c=features.get('temperature_c', 25.0),
            precipitation_mm=features.get('precipitation_mm', 0.0),
            wind_speed_kmh=features.get('wind_speed_kmh')
        )

    # ✅ NEW XGBOOST BRANCH (consistent with xgb/predict.py)
    elif model_type == 'xgboost':
        from models.xgb.predict import predict_eta

        return predict_eta(
            model_key=model_key,
            distance_to_stop=distance_m,
            progress_on_segment=features.get('progress_on_segment'),
            progress_ratio=features.get('progress_ratio'),
            hour=features.get('hour', 0),
            day_of_week=features.get('day_of_week', 0),
            is_peak_hour=features.get('is_peak_hour', False),
            is_weekend=features.get('is_weekend', False),
            is_holiday=features.get('is_holiday', False),
            temperature_c=features.get('temperature_c', 25.0),
            precipitation_mm=features.get('precipitation_mm', 0.0),
            wind_speed_kmh=features.get('wind_speed_kmh', None)
        )

    else:
        raise ValueError(f"Unknown model type: {model_type}")


def estimate_stop_times(
    vehicle_position: dict,
    upcoming_stops: list[dict],
    route_id: str = None,
    trip_id: str = None,
    model_key: str = None,
    model_type: str = None,
    prefer_route_model: bool = True,
    max_stops: int = 3,
) -> dict:

    # Validate inputs
    if not vehicle_position or not upcoming_stops:
        return {
            'vehicle_id': vehicle_position.get('vehicle_id', 'unknown') if vehicle_position else 'unknown',
            'route_id': route_id,
            'trip_id': trip_id,
            'computed_at': datetime.now(timezone.utc).isoformat(),
            'model_key': None,
            'predictions': [],
            'error': 'Missing vehicle position or stops'
        }

    stops_to_predict = upcoming_stops[:max_stops]

    # Parse timestamp
    vp_timestamp_str = vehicle_position['timestamp']
    if vp_timestamp_str.endswith('Z'):
        vp_timestamp_str = vp_timestamp_str.replace('Z', '+00:00')
    vp_timestamp = datetime.fromisoformat(vp_timestamp_str)

    # Extract temporal features (using config for timezone)
    temporal_features = extract_temporal_features(
        vp_timestamp,
        tz=_config.default_timezone,
        region=_config.default_region
    )

    # Determine route
    if route_id is None:
        route_id = vehicle_position.get('route', 'unknown')

    # Load registry
    registry = get_registry()
    model_scope = 'unknown'

    # ============================================================
    #    SMART MODEL SELECTION
    # ============================================================
    if model_key is None:
        if prefer_route_model and route_id and route_id != 'unknown':
            model_key = registry.get_best_model(
                model_type=model_type,
                route_id=route_id,
                metric='test_mae_seconds'
            )

            if model_key:
                model_scope = 'route'
                _logger.debug("Using route-specific model", route_id=route_id, model_key=model_key)
            else:
                _logger.debug("No route model found, trying global", route_id=route_id)
                model_key = registry.get_best_model(
                    model_type=model_type,
                    route_id='global',
                    metric='test_mae_seconds'
                )
                model_scope = 'global'
        else:
            model_key = registry.get_best_model(
                model_type=model_type,
                route_id='global',
                metric='test_mae_seconds'
            )
            model_scope = 'global'

        # Last fallback
        if model_key is None:
            model_key = registry.get_best_model(model_type=model_type)

        if model_key is None:
            return {
                'vehicle_id': vehicle_position['vehicle_id'],
                'route_id': route_id,
                'trip_id': trip_id,
                'computed_at': datetime.now(timezone.utc).isoformat(),
                'model_key': None,
                'predictions': [],
                'error': 'No trained models found for model_type'
            }

    # Load metadata
    try:
        model_metadata = registry.load_metadata(model_key)
        actual_model_type = model_metadata.get('model_type', 'unknown')
        model_route_id = model_metadata.get('route_id')

        if model_route_id not in (None, 'global'):
            model_scope = 'route'
        elif model_scope == 'unknown':
            model_scope = 'global'

    except Exception as e:
        return {
            'vehicle_id': vehicle_position['vehicle_id'],
            'route_id': route_id,
            'trip_id': trip_id,
            'computed_at': datetime.now(timezone.utc).isoformat(),
            'model_key': model_key,
            'predictions': [],
            'error': f'Failed to load model metadata: {str(e)}'
        }

    # ============================================================
    #    PREDICT STOP ETAs
    # ============================================================
    predictions = []
    approx_total_segments = max(
        (
            stop.get('total_stop_sequence')
            or stop.get('stop_sequence')
            or stop.get('sequence')
            or 0
        )
        for stop in stops_to_predict
    ) if stops_to_predict else 0
    if approx_total_segments <= 0:
        approx_total_segments = max(len(stops_to_predict), 1)

    for idx, stop in enumerate(stops_to_predict):
        next_stop = stops_to_predict[idx + 1] if idx + 1 < len(stops_to_predict) else None

        distance_m, progress_on_segment, progress_ratio = _progress_features(
            vehicle_position,
            stop,
            next_stop,
            approx_total_segments,
        )

        stop_sequence_value = (
            stop.get('stop_sequence')
            or stop.get('sequence')
            or stop.get('stop_order')
            or idx + 1
        )

        # Build features
        features = {
            'route_id': route_id,
            'stop_sequence': stop_sequence_value,
            'distance_to_stop': distance_m,
            'progress_on_segment': progress_on_segment,
            'progress_ratio': progress_ratio,
            'hour': temporal_features['hour'],
            'day_of_week': temporal_features['day_of_week'],
            'is_weekend': temporal_features['is_weekend'],
            'is_holiday': temporal_features['is_holiday'],
            'is_peak_hour': temporal_features['is_peak_hour'],
            'temperature_c': _config.default_temperature_c,
            'precipitation_mm': _config.default_precipitation_mm,
            'wind_speed_kmh': _config.default_wind_speed_kmh,
        }

        try:
            result = _predict_with_model(model_key, actual_model_type, features, distance_m)

            eta_seconds = result.get('eta_seconds', 0.0)
            eta_minutes = eta_seconds / 60.0
            eta_formatted = result.get(
                'eta_formatted',
                f"{int(eta_minutes)}m {int(eta_seconds % 60)}s"
            )

            eta_ts = datetime.fromtimestamp(vp_timestamp.timestamp() + eta_seconds, tz=timezone.utc)

            predictions.append({
                'stop_id': stop['stop_id'],
                'stop_sequence': stop_sequence_value,
                'distance_to_stop_m': round(distance_m, 1),
                'eta_seconds': round(eta_seconds, 1),
                'eta_minutes': round(eta_minutes, 2),
                'eta_formatted': eta_formatted,
                'eta_timestamp': eta_ts.isoformat(),
            })

        except Exception as e:
            predictions.append({
                'stop_id': stop['stop_id'],
                'stop_sequence': stop_sequence_value,
                'distance_to_stop_m': round(distance_m, 1),
                'eta_seconds': None,
                'eta_minutes': None,
                'eta_formatted': None,
                'eta_timestamp': None,
                'error': str(e),
            })

    return {
        'vehicle_id': vehicle_position['vehicle_id'],
        'route_id': route_id,
        'trip_id': trip_id,
        'computed_at': datetime.now(timezone.utc).isoformat(),
        'model_key': model_key,
        'model_type': actual_model_type,
        'model_scope': model_scope,
        'predictions': predictions
    }
