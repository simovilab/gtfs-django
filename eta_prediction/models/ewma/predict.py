"""
Prediction interface for EWMA model.
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
                route_id: str,
                stop_sequence: int,
                hour: Optional[int] = None) -> Dict:
    """
    Predict ETA using EWMA model.
    
    Args:
        model_key: Model identifier
        route_id: Route ID
        stop_sequence: Stop sequence number
        hour: Hour of day (if model uses hourly grouping)
        
    Returns:
        Dictionary with prediction and metadata
    """
    # Load model
    registry = get_registry()
    model = registry.load_model(model_key)
    metadata = registry.load_metadata(model_key)
    
    # Prepare input
    input_data = {
        'route_id': [route_id],
        'stop_sequence': [stop_sequence]
    }
    
    if hour is not None and 'hour' in model.group_by:
        input_data['hour'] = [hour]
    
    input_df = pd.DataFrame(input_data)
    
    # Predict
    eta_seconds = model.predict(input_df)[0]
    
    # Check if EWMA value exists
    key = tuple(input_df.iloc[0][col] for col in model.group_by)
    has_ewma = key in model.ewma_values
    n_observations = model.observation_counts.get(key, 0) if has_ewma else 0
    
    return {
        'eta_seconds': float(eta_seconds),
        'eta_minutes': float(eta_seconds / 60),
        'eta_formatted': format_seconds(eta_seconds),
        'model_key': model_key,
        'model_type': 'ewma',
        'alpha': metadata.get('alpha'),
        'has_ewma_value': has_ewma,
        'n_observations': n_observations,
        'using_global_mean': not has_ewma or n_observations < model.min_observations
    }


def predict_and_update(model_key: str,
                      route_id: str,
                      stop_sequence: int,
                      observed_eta: float,
                      hour: Optional[int] = None,
                      save_updated: bool = False) -> Dict:
    """
    Predict ETA and update model with observed value (online learning).
    
    Args:
        model_key: Model identifier
        route_id: Route ID
        stop_sequence: Stop sequence
        observed_eta: Actual observed ETA in seconds
        hour: Hour of day
        save_updated: Whether to save updated model back to registry
        
    Returns:
        Dictionary with prediction, error, and updated EWMA
    """
    # Get prediction first
    result = predict_eta(model_key, route_id, stop_sequence, hour)
    prediction = result['eta_seconds']
    
    # Load model for update
    registry = get_registry()
    model = registry.load_model(model_key)
    
    # Prepare input for update
    input_data = {
        'route_id': [route_id],
        'stop_sequence': [stop_sequence]
    }
    if hour is not None and 'hour' in model.group_by:
        input_data['hour'] = [hour]
    
    input_df = pd.DataFrame(input_data)
    
    # Update model
    model.update(input_df, np.array([observed_eta]))
    
    # Get new EWMA value
    key = tuple(input_df.iloc[0][col] for col in model.group_by)
    new_ewma = model.ewma_values.get(key)
    
    # Save if requested
    if save_updated:
        metadata = registry.load_metadata(model_key)
        registry.save_model(model_key, model, metadata, overwrite=True)
    
    return {
        **result,
        'observed_eta_seconds': observed_eta,
        'error_seconds': observed_eta - prediction,
        'updated_ewma_seconds': new_ewma,
        'model_updated': True
    }


def batch_predict(model_key: str, input_df: pd.DataFrame) -> pd.DataFrame:
    """Batch prediction."""
    registry = get_registry()
    model = registry.load_model(model_key)
    
    result_df = input_df.copy()
    result_df['predicted_eta_seconds'] = model.predict(input_df)
    result_df['predicted_eta_minutes'] = result_df['predicted_eta_seconds'] / 60
    
    return result_df


if __name__ == "__main__":
    # Example: predict and update
    result = predict_and_update(
        model_key="ewma_sample_dataset_temporal-route_20250126_143022_alpha=0_3",
        route_id="1",
        stop_sequence=5,
        observed_eta=180.0,  # 3 minutes
        hour=8
    )
    
    print("Prediction and Update:")
    print(f"  Predicted: {result['eta_formatted']}")
    print(f"  Observed: {format_seconds(result['observed_eta_seconds'])}")
    print(f"  Error: {result['error_seconds']:.1f} seconds")
    print(f"  Updated EWMA: {result['updated_ewma_seconds']/60:.2f} minutes")