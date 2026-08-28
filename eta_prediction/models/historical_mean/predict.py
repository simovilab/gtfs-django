"""
Prediction interface for Historical Mean model.
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
                hour: int,
                day_of_week: Optional[int] = None,
                is_peak_hour: Optional[bool] = None) -> Dict:
    """
    Predict ETA using historical mean model.
    
    Args:
        model_key: Model identifier in registry
        route_id: Route ID
        stop_sequence: Stop sequence number
        hour: Hour of day (0-23)
        day_of_week: Day of week (0=Monday, optional)
        is_peak_hour: Peak hour flag (optional)
        
    Returns:
        Dictionary with prediction and metadata
    """
    # Load model
    registry = get_registry()
    model = registry.load_model(model_key)
    metadata = registry.load_metadata(model_key)
    
    # Prepare input dataframe
    input_data = {
        'route_id': [route_id],
        'stop_sequence': [stop_sequence],
        'hour': [hour]
    }
    
    if day_of_week is not None and 'day_of_week' in model.group_by:
        input_data['day_of_week'] = [day_of_week]
    
    if is_peak_hour is not None and 'is_peak_hour' in model.group_by:
        input_data['is_peak_hour'] = [is_peak_hour]
    
    input_df = pd.DataFrame(input_data)
    
    # Predict
    eta_seconds = model.predict(input_df)[0]
    
    # Check if prediction came from historical data or fallback
    coverage = model.get_coverage(input_df)
    has_historical_data = coverage > 0
    
    return {
        'eta_seconds': float(eta_seconds),
        'eta_minutes': float(eta_seconds / 60),
        'eta_formatted': format_seconds(eta_seconds),
        'model_key': model_key,
        'model_type': 'historical_mean',
        'has_historical_data': has_historical_data,
        'global_mean_eta': metadata.get('global_mean_eta'),
        'group_by': metadata.get('group_by')
    }


def batch_predict(model_key: str, input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Batch prediction for multiple inputs.
    
    Args:
        model_key: Model identifier in registry
        input_df: DataFrame with features
        
    Returns:
        DataFrame with predictions added
    """
    registry = get_registry()
    model = registry.load_model(model_key)
    
    result_df = input_df.copy()
    result_df['predicted_eta_seconds'] = model.predict(input_df)
    result_df['predicted_eta_minutes'] = result_df['predicted_eta_seconds'] / 60
    
    # Add coverage information
    result_df['has_historical_data'] = False
    merged = input_df[model.group_by].merge(
        model.lookup_table,
        on=model.group_by,
        how='left',
        indicator=True
    )
    result_df.loc[merged['_merge'] == 'both', 'has_historical_data'] = True
    
    return result_df


if __name__ == "__main__":
    # Example usage
    
    # Single prediction
    result = predict_eta(
        model_key="historical_mean_sample_dataset_temporal-route_20250126_143022",
        route_id="1",
        stop_sequence=5,
        hour=8,
        day_of_week=0  # Monday
    )
    
    print("Single Prediction:")
    print(f"  ETA: {result['eta_formatted']}")
    print(f"  Has historical data: {result['has_historical_data']}")
    
    # Batch prediction example
    test_data = pd.DataFrame({
        'route_id': ['1', '1', '2'],
        'stop_sequence': [5, 10, 3],
        'hour': [8, 17, 12]
    })
    
    predictions = batch_predict(
        model_key="historical_mean_sample_dataset_temporal-route_20250126_143022",
        input_df=test_data
    )
    
    print("\nBatch Predictions:")
    print(predictions[['route_id', 'stop_sequence', 'predicted_eta_minutes', 'has_historical_data']])