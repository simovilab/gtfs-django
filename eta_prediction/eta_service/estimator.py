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
    
    Each model type has its own signature requirements.
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
            headway_seconds=features.get('headway_seconds', 120.0),  # Default 2min
            current_speed_kmh=features.get('speed_kmh', 0.0),
            temperature_c=features.get('temperature_c', 25.0),  # Default Costa Rica temp
            precipitation_mm=features.get('precipitation_mm', 0.0)
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
    """
    Estimate arrival times for upcoming stops.
    
    Args:
        vehicle_position: {
            'vehicle_id': str,
            'lat': float,
            'lon': float,
            'speed': float,  # m/s (will convert to km/h)
            'heading': int,  # degrees
            'timestamp': str (ISO format)
        }
        upcoming_stops: [
            {
                'stop_id': str,
                'stop_sequence': int,
                'lat': float,
                'lon': float
            },
            ...
        ]
        route_id: Route identifier (optional, for route-specific models)
        trip_id: Trip identifier (optional, for context)
        model_key: Specific model to use (if None, uses best available)
        model_type: Filter models by type (e.g., 'polyreg_time')
        prefer_route_model: If True, prefer route-specific models over global
        max_stops: Maximum number of stops to predict (default: 3)
        
    Returns:
        {
            'vehicle_id': str,
            'route_id': str,
            'trip_id': str,
            'computed_at': str (ISO),
            'model_key': str,
            'model_type': str,
            'model_scope': str ('route' or 'global'),
            'predictions': [
                {
                    'stop_id': str,
                    'stop_sequence': int,
                    'distance_to_stop_m': float,
                    'eta_seconds': float,
                    'eta_minutes': float,
                    'eta_formatted': str,
                    'eta_timestamp': str (ISO),
                },
                ...
            ]
        }
    """
    
    # Validate inputs
    if not vehicle_position or not upcoming_stops:
        return {
            'vehicle_id': vehicle_position.get('vehicle_id', 'unknown'),
            'route_id': route_id,
            'trip_id': trip_id,
            'computed_at': datetime.now(timezone.utc).isoformat(),
            'model_key': None,
            'predictions': [],
            'error': 'Missing vehicle position or stops'
        }
    
    # Limit stops
    stops_to_predict = upcoming_stops[:max_stops]
    
    # Parse timestamp
    vp_timestamp_str = vehicle_position['timestamp']
    if vp_timestamp_str.endswith('Z'):
        vp_timestamp_str = vp_timestamp_str.replace('Z', '+00:00')
    vp_timestamp = datetime.fromisoformat(vp_timestamp_str)
    
    # Extract temporal features (reused for all stops)
    temporal_features = extract_temporal_features(
        vp_timestamp,
        tz='America/Costa_Rica',
        region='CR'
    )
    
    # Convert speed from m/s to km/h
    speed_mps = vehicle_position.get('speed', 0.0)
    speed_kmh = speed_mps * 3.6
    
    # Determine route_id if not provided
    if route_id is None:
        route_id = vehicle_position.get('route', 'unknown')
    
    # Load model with smart routing logic
    registry = get_registry()
    model_scope = 'unknown'
    
    if model_key is None:
        # Smart model selection
        if prefer_route_model and route_id and route_id != 'unknown':
            # Try route-specific model first
            model_key = registry.get_best_model(
                model_type=model_type,
                route_id=route_id,
                metric='test_mae_seconds'
            )
            
            if model_key:
                model_scope = 'route'
                print(f"[ETA Service] Using route-specific model for route {route_id}")
            else:
                # Fallback to global model
                print(f"[ETA Service] No route-specific model for route {route_id}, using global")
                model_key = registry.get_best_model(
                    model_type=model_type,
                    route_id='global',
                    metric='test_mae_seconds'
                )
                model_scope = 'global'
        else:
            # Use global model directly
            model_key = registry.get_best_model(
                model_type=model_type,
                route_id='global',
                metric='test_mae_seconds'
            )
            model_scope = 'global'
        
        if model_key is None:
            # Last resort: any model
            model_key = registry.get_best_model(
                model_type=model_type,
                metric='test_mae_seconds'
            )
            model_scope = 'any'
        
        if model_key is None:
            return {
                'vehicle_id': vehicle_position['vehicle_id'],
                'route_id': route_id,
                'trip_id': trip_id,
                'computed_at': datetime.now(timezone.utc).isoformat(),
                'model_key': None,
                'predictions': [],
                'error': 'No trained models found in registry'
            }
    else:
        # Model key explicitly provided - determine scope from metadata
        try:
            from models.common.keys import ModelKey
            parsed = ModelKey.parse(model_key)
            model_scope = parsed.get('scope', 'unknown')
        except:
            model_scope = 'explicit'
    
    try:
        model_metadata = registry.load_metadata(model_key)
        actual_model_type = model_metadata.get('model_type', 'unknown')
        model_route_id = model_metadata.get('route_id')
        
        # Update scope from metadata if available
        if model_route_id is not None:
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
    
    # Generate predictions for each stop
    predictions = []
    
    for stop in stops_to_predict:
        # Calculate distance to stop
        distance_m = haversine_distance(
            vehicle_position['lat'],
            vehicle_position['lon'],
            stop['lat'],
            stop['lon']
        )
        
        # Build feature dict with all available features
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
            # Defaults for optional features
            'headway_seconds': 120.0,  # 2 min default
            'temperature_c': 25.0,     # Reasonable Costa Rica temp
            'precipitation_mm': 0.0,   # Assume no rain
        }
        
        # Make prediction using the appropriate model interface
        try:
            result = _predict_with_model(model_key, actual_model_type, features, distance_m)
            
            # Extract ETA from result
            eta_seconds = result.get('eta_seconds', 0.0)
            eta_minutes = result.get('eta_minutes', eta_seconds / 60.0)
            eta_formatted = result.get('eta_formatted', f"{int(eta_minutes)}m {int(eta_seconds % 60)}s")
            
            # Calculate arrival timestamp
            eta_timestamp = vp_timestamp.timestamp() + eta_seconds
            eta_timestamp_iso = datetime.fromtimestamp(eta_timestamp, tz=timezone.utc).isoformat()
            
            predictions.append({
                'stop_id': stop['stop_id'],
                'stop_sequence': stop['stop_sequence'],
                'distance_to_stop_m': round(distance_m, 1),
                'eta_seconds': round(eta_seconds, 1),
                'eta_minutes': round(eta_minutes, 2),
                'eta_formatted': eta_formatted,
                'eta_timestamp': eta_timestamp_iso,
            })
        except Exception as e:
            # Skip this stop if prediction fails
            predictions.append({
                'stop_id': stop['stop_id'],
                'stop_sequence': stop['stop_sequence'],
                'distance_to_stop_m': round(distance_m, 1),
                'eta_seconds': None,
                'eta_minutes': None,
                'eta_formatted': None,
                'eta_timestamp': None,
                'error': str(e)
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


# Example usage / test
if __name__ == '__main__':
    # Mock vehicle position (matching your MQTT format)
    vehicle_position = {
        'vehicle_id': 'vehicle_42',
        'route': '1',
        'lat': 9.9281,
        'lon': -84.0907,
        'speed': 10.5,  # m/s
        'heading': 90,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    
    # Mock upcoming stops
    upcoming_stops = [
        {'stop_id': 'stop_001', 'stop_sequence': 5, 'lat': 9.9291, 'lon': -84.0897},
        {'stop_id': 'stop_002', 'stop_sequence': 6, 'lat': 9.9301, 'lon': -84.0887},
        {'stop_id': 'stop_003', 'stop_sequence': 7, 'lat': 9.9311, 'lon': -84.0877},
    ]
    
    # Get predictions with route-specific model
    result = estimate_stop_times(
        vehicle_position=vehicle_position,
        upcoming_stops=upcoming_stops,
        route_id='1',
        trip_id='trip_001',
        prefer_route_model=True,
        max_stops=3
    )
    
    # Pretty print
    print(f"\n{'='*70}")
    print(f"ETA Predictions for Vehicle: {result['vehicle_id']}")
    print(f"Route: {result['route_id']}, Trip: {result['trip_id']}")
    print(f"Model: {result.get('model_type', 'N/A')} ({result.get('model_scope', 'unknown')} scope)")
    print(f"Key: {result['model_key']}")
    print(f"{'='*70}\n")
    
    if result.get('error'):
        print(f"ERROR: {result['error']}")
    else:
        for pred in result['predictions']:
            if pred.get('error'):
                print(f"Stop {pred['stop_id']}: ERROR - {pred['error']}")
            else:
                print(f"Stop {pred['stop_id']} (seq {pred['stop_sequence']}):")
                print(f"  Distance: {pred['distance_to_stop_m']:.1f}m")
                print(f"  ETA: {pred['eta_formatted']} ({pred['eta_seconds']:.0f}s)")
                print(f"  Arrival: {pred['eta_timestamp']}")
            print()