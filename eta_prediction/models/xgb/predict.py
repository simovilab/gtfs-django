"""
Prediction interface for XGBoost Time model.
"""

import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

sys.path.append(str(Path(__file__).parent.parent))

from common.registry import get_registry
from common.utils import format_seconds


def predict_eta(
    model_key: str,
    distance_to_stop: float,
    progress_on_segment: Optional[float] = None,
    progress_ratio: Optional[float] = None,
    hour: Optional[int] = None,
    day_of_week: Optional[int] = None,
    is_peak_hour: Optional[bool] = None,
    is_weekend: Optional[bool] = None,
    is_holiday: Optional[bool] = None,
    temperature_c: Optional[float] = None,
    precipitation_mm: Optional[float] = None,
    wind_speed_kmh: Optional[float] = None,
) -> Dict:
    """
    Predict ETA using XGBoost time model.
    
    Args:
        model_key: Model identifier
        distance_to_stop: Distance in meters
        progress_on_segment: 0-1 fraction of the current stop segment
        progress_ratio: 0-1 fraction along the entire route
        hour: Hour of day (0–23)
        day_of_week: Day of week (0=Monday)
        is_peak_hour: Peak hour flag
        is_weekend: Weekend flag
        is_holiday: Holiday flag
        temperature_c: Temperature (°C)
        precipitation_mm: Precipitation (mm)
        wind_speed_kmh: Wind speed (km/h)
        
    Returns:
        Dictionary with prediction and metadata.
    """
    # Load model & metadata
    registry = get_registry()
    model = registry.load_model(model_key)
    metadata = registry.load_metadata(model_key)

    # Prepare input
    input_data = {"distance_to_stop": [distance_to_stop]}

    # Add optional features (only if provided)
    optional_features = {
        "progress_on_segment": progress_on_segment,
        "progress_ratio": progress_ratio,
        "hour": hour,
        "day_of_week": day_of_week,
        "is_peak_hour": is_peak_hour,
        "is_weekend": is_weekend,
        "is_holiday": is_holiday,
        "temperature_c": temperature_c,
        "precipitation_mm": precipitation_mm,
        "wind_speed_kmh": wind_speed_kmh,
    }

    for key, value in optional_features.items():
        if value is not None:
            input_data[key] = [value]

    input_df = pd.DataFrame(input_data)

    # Predict
    eta_seconds = float(model.predict(input_df)[0])

    return {
        "eta_seconds": eta_seconds,
        "eta_minutes": eta_seconds / 60.0,
        "eta_formatted": format_seconds(eta_seconds),
        "model_key": model_key,
        "model_type": metadata.get("model_type", "xgboost"),
        "distance_to_stop_m": distance_to_stop,
        "features_used": list(input_data.keys()),
    }


def batch_predict(model_key: str, input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Batch prediction for XGBoost time model.
    
    Args:
        model_key: Model identifier
        input_df: DataFrame with required feature columns
        
    Returns:
        DataFrame with additional prediction columns:
        - predicted_eta_seconds
        - predicted_eta_minutes
    """
    registry = get_registry()
    model = registry.load_model(model_key)

    result_df = input_df.copy()
    result_df["predicted_eta_seconds"] = model.predict(input_df)
    result_df["predicted_eta_minutes"] = result_df["predicted_eta_seconds"] / 60.0

    return result_df
