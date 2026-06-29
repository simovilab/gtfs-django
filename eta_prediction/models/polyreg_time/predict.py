"""
Prediction interface for Polynomial Regression Time model.
"""

import pandas as pd
from typing import Dict, Optional
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from common.registry import get_registry
from common.utils import format_seconds


def predict_eta(model_key: str,
                distance_to_stop: float,
                distance_to_next_stop: Optional[float] = None,
                shape_distance_to_stop: Optional[float] = None,
                shape_progress: Optional[float] = None,
                cross_track_error: Optional[float] = None,
                progress_ratio: Optional[float] = None,
                stops_ahead: Optional[int] = None,
                current_speed_kmh: Optional[float] = None,
                bearing_to_stop: Optional[float] = None,
                bearing_diff: Optional[float] = None,
                is_at_stop: Optional[bool] = None,
                hour: Optional[int] = None,
                day_of_week: Optional[int] = None,
                is_peak_hour: Optional[bool] = None,
                is_weekend: Optional[bool] = None,
                is_holiday: Optional[bool] = None,
                hour_sin: Optional[float] = None,
                hour_cos: Optional[float] = None,
                dow_sin: Optional[float] = None,
                dow_cos: Optional[float] = None) -> Dict:
    """
    Predict ETA using polynomial regression time model.

    Args:
        model_key: Model identifier
        distance_to_stop: Haversine distance to target stop (meters)
        distance_to_next_stop: Distance to the immediate next stop (meters)
        shape_distance_to_stop: Along-shape distance to target stop (meters)
        shape_progress: 0-1 fraction along the route shape
        cross_track_error: Perpendicular distance off the shape (meters)
        progress_ratio: 0-1 fraction along the overall route
        stops_ahead: Number of stops between the vehicle and the target
        current_speed_kmh: Vehicle speed (km/h)
        bearing_to_stop: Initial bearing from vehicle to target stop (degrees)
        bearing_diff: |vehicle heading - bearing_to_stop| wrapped to 0-180
        is_at_stop: Whether the vehicle currently reports STOPPED_AT
        hour: Hour of day (0-23)
        day_of_week: Day of week (0=Monday)
        is_peak_hour: Peak hour flag
        is_weekend: Weekend flag
        is_holiday: Holiday flag
        hour_sin/hour_cos/dow_sin/dow_cos: Cyclical temporal encodings

    Returns:
        Dictionary with prediction and metadata
    """
    # Load model
    registry = get_registry()
    model = registry.load_model(model_key)
    metadata = registry.load_metadata(model_key)

    # Prepare input
    input_data = {'distance_to_stop': [distance_to_stop]}

    # Add optional features
    optional_features = {
        'distance_to_next_stop': distance_to_next_stop,
        'shape_distance_to_stop': shape_distance_to_stop,
        'shape_progress': shape_progress,
        'cross_track_error': cross_track_error,
        'progress_ratio': progress_ratio,
        'stops_ahead': stops_ahead,
        'current_speed_kmh': current_speed_kmh,
        'bearing_to_stop': bearing_to_stop,
        'bearing_diff': bearing_diff,
        'is_at_stop': is_at_stop,
        'hour': hour,
        'day_of_week': day_of_week,
        'is_peak_hour': is_peak_hour,
        'is_weekend': is_weekend,
        'is_holiday': is_holiday,
        'hour_sin': hour_sin,
        'hour_cos': hour_cos,
        'dow_sin': dow_sin,
        'dow_cos': dow_cos,
    }

    for key, value in optional_features.items():
        if value is not None:
            input_data[key] = [value]
    
    input_df = pd.DataFrame(input_data)
    
    # Predict
    eta_seconds = model.predict(input_df)[0]
    
    return {
        'eta_seconds': float(eta_seconds),
        'eta_minutes': float(eta_seconds / 60),
        'eta_formatted': format_seconds(eta_seconds),
        'model_key': model_key,
        'model_type': 'polyreg_time',
        'distance_to_stop_m': distance_to_stop,
        'features_used': list(input_data.keys())
    }


def batch_predict(model_key: str, input_df: pd.DataFrame) -> pd.DataFrame:
    """Batch prediction."""
    registry = get_registry()
    model = registry.load_model(model_key)
    
    result_df = input_df.copy()
    result_df['predicted_eta_seconds'] = model.predict(input_df)
    result_df['predicted_eta_minutes'] = result_df['predicted_eta_seconds'] / 60
    
    return result_df
