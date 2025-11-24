"""
ETA Service - Enhanced Implementation
Estimates stop arrival times from vehicle positions with route-specific model support.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
import math

# Add parent directories to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "feature_engineering"))
sys.path.insert(0, str(project_root / "models"))

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
            hour=features.get('hour', 0),
            day_of_week=features.get('day_of_week', 0),
            is_peak_hour=features.get('is_peak_hour', False),
            is_weekend=features.get('is_weekend', False),
            is_holiday=features.get('is_holiday', False),
            headway_seconds=features.get('headway_seconds', 120.0),
            current_speed_kmh=features.get('speed_kmh', 0.0),
            temperature_c=features.get('temperature_c', 25.0),
            precipitation_mm=features.get('precipitation_mm', 0.0)
        )

    # ✅ NEW XGBOOST BRANCH (consistent with xgb/predict.py)
    elif model_type == 'xgboost':
        from models.xgb.predict import predict_eta

        return predict_eta(
            model_key=model_key,
            distance_to_stop=distance_m,
            hour=features.get('hour', 0),
            day_of_week=features.get('day_of_week', 0),
            is_peak_hour=features.get('is_peak_hour', False),
            is_weekend=features.get('is_weekend', False),
            is_holiday=features.get('is_holiday', False),
            headway_seconds=features.get('headway_seconds', 120.0),
            current_speed_kmh=features.get('speed_kmh', 0.0),
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

    # Extract temporal features
    temporal_features = extract_temporal_features(
        vp_timestamp,
        tz='America/Costa_Rica',
        region='CR'
    )

    # Convert speed
    speed_mps = vehicle_position.get('speed', 0.0)
    speed_kmh = speed_mps * 3.6

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
                print(f"[ETA Service] Using route-specific model for route {route_id}")
            else:
                print(f"[ETA Service] No route model for {route_id}. Trying global.")
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

    for stop in stops_to_predict:

        distance_m = haversine_distance(
            vehicle_position['lat'],
            vehicle_position['lon'],
            stop['lat'],
            stop['lon']
        )

        # Build features
        features = {
            'route_id': route_id,
            'stop_sequence': stop['stop_sequence'],
            'distance_to_stop': distance_m,
            'hour': temporal_features['hour'],
            'day_of_week': temporal_features['day_of_week'],
            'is_weekend': temporal_features['is_weekend'],
            'is_holiday': temporal_features['is_holiday'],
            'is_peak_hour': temporal_features['is_peak_hour'],
            'speed_kmh': speed_kmh,
            'speed_mps': speed_mps,
            'headway_seconds': 120.0,
            'temperature_c': 25.0,
            'precipitation_mm': 0.0,
            'wind_speed_kmh': None,     # Optional
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
                'stop_sequence': stop['stop_sequence'],
                'distance_to_stop_m': round(distance_m, 1),
                'eta_seconds': round(eta_seconds, 1),
                'eta_minutes': round(eta_minutes, 2),
                'eta_formatted': eta_formatted,
                'eta_timestamp': eta_ts.isoformat(),
            })

        except Exception as e:
            predictions.append({
                'stop_id': stop['stop_id'],
                'stop_sequence': stop['stop_sequence'],
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
