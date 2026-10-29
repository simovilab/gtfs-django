"""
Prediction interface for Polynomial Regression Distance model.
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from common.registry import get_registry
from common.utils import format_seconds


def predict_eta(model_key: str,
                distance_to_stop: float,
                route_id: Optional[str] = None) -> Dict:
    """
    Predict ETA using polynomial regression distance model.
    
    Args:
        model_key: Model identifier in registry
        distance_to_stop: Distance to stop in meters
        route_id: Route ID (required for route-specific models)
        
    Returns:
        Dictionary with prediction and metadata
    """
    # Load model
    registry = get_registry()
    model = registry.load_model(model_key)
    metadata = registry.load_metadata(model_key)
    
    # Prepare input
    input_data = {'distance_to_stop': [distance_to_stop]}
    if model.route_specific:
        if route_id is None:
            raise ValueError("route_id required for route-specific model")
        input_data['route_id'] = [route_id]
    
    input_df = pd.DataFrame(input_data)
    
    # Predict
    eta_seconds = model.predict(input_df)[0]
    
    # Get coefficients for this route
    coefs = model.get_coefficients(route_id if model.route_specific else None)
    
    return {
        'eta_seconds': float(eta_seconds),
        'eta_minutes': float(eta_seconds / 60),
        'eta_formatted': format_seconds(eta_seconds),
        'model_key': model_key,
        'model_type': 'polyreg_distance',
        'distance_to_stop_m': distance_to_stop,
        'route_specific': metadata.get('route_specific', False),
        'degree': metadata.get('degree'),
        'coefficients': coefs
    }


def batch_predict(model_key: str, input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Batch prediction for multiple inputs.
    
    Args:
        model_key: Model identifier in registry
        input_df: DataFrame with distance_to_stop (and route_id if needed)
        
    Returns:
        DataFrame with predictions added
    """
    registry = get_registry()
    model = registry.load_model(model_key)
    
    result_df = input_df.copy()
    result_df['predicted_eta_seconds'] = model.predict(input_df)
    result_df['predicted_eta_minutes'] = result_df['predicted_eta_seconds'] / 60
    
    return result_df


if __name__ == "__main__":
    # Example usage
    
    # Single prediction
    result = predict_eta(
        model_key="polyreg_distance_sample_dataset_distance_20250126_143022_degree=2",
        distance_to_stop=1500.0,  # 1.5 km
        route_id="1"
    )
    
    print("Single Prediction:")
    print(f"  Distance: {result['distance_to_stop_m']} meters")
    print(f"  ETA: {result['eta_formatted']}")
    print(f"  Degree: {result['degree']}")
    
    # Batch prediction
    test_data = pd.DataFrame({
        'route_id': ['1', '1', '2'],
        'distance_to_stop': [500.0, 1500.0, 3000.0]
    })
    
    predictions = batch_predict(
        model_key="polyreg_distance_sample_dataset_distance_20250126_143022_degree=2",
        input_df=test_data
    )
    
    print("\nBatch Predictions:")
    print(predictions[['route_id', 'distance_to_stop', 'predicted_eta_minutes']])