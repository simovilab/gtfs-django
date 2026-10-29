"""
Utility functions for models package.
"""

import numpy as np
import pandas as pd
from typing import Any, Dict, List, Optional
from datetime import datetime
import logging


def setup_logging(name: str = "eta_models", level: str = "INFO") -> logging.Logger:
    """
    Setup consistent logging for models.
    
    Args:
        name: Logger name
        level: Logging level
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def safe_divide(numerator: np.ndarray, 
                denominator: np.ndarray, 
                fill_value: float = 0.0) -> np.ndarray:
    """
    Safe division that handles division by zero.
    
    Args:
        numerator: Numerator array
        denominator: Denominator array
        fill_value: Value to use when denominator is zero
        
    Returns:
        Result array
    """
    result = np.full_like(numerator, fill_value, dtype=float)
    mask = denominator != 0
    result[mask] = numerator[mask] / denominator[mask]
    return result


def clip_predictions(predictions: np.ndarray,
                     min_value: float = 0.0,
                     max_value: float = 7200.0) -> np.ndarray:
    """
    Clip predictions to reasonable range.
    
    Args:
        predictions: Raw predictions
        min_value: Minimum ETA (seconds)
        max_value: Maximum ETA (seconds, default 2 hours)
        
    Returns:
        Clipped predictions
    """
    return np.clip(predictions, min_value, max_value)


def add_lag_features(df: pd.DataFrame,
                    columns: List[str],
                    lags: List[int],
                    group_by: Optional[str] = None) -> pd.DataFrame:
    """
    Add lagged features to dataframe.
    
    Args:
        df: Input dataframe
        columns: Columns to lag
        lags: List of lag values (e.g., [1, 2, 3])
        group_by: Optional column to group by before lagging
        
    Returns:
        DataFrame with lag features added
    """
    df_copy = df.copy()
    
    for col in columns:
        for lag in lags:
            lag_col_name = f"{col}_lag{lag}"
            
            if group_by:
                df_copy[lag_col_name] = df_copy.groupby(group_by)[col].shift(lag)
            else:
                df_copy[lag_col_name] = df_copy[col].shift(lag)
    
    return df_copy


def smooth_predictions(predictions: np.ndarray,
                      window_size: int = 3,
                      method: str = 'ewma',
                      alpha: float = 0.3) -> np.ndarray:
    """
    Smooth predictions using moving average.
    
    Args:
        predictions: Array of predictions
        window_size: Window size for smoothing
        method: 'mean', 'median', or 'ewma'
        alpha: Alpha parameter for EWMA
        
    Returns:
        Smoothed predictions
    """
    if len(predictions) < window_size:
        return predictions
    
    if method == 'mean':
        return pd.Series(predictions).rolling(window_size, min_periods=1).mean().values
    elif method == 'median':
        return pd.Series(predictions).rolling(window_size, min_periods=1).median().values
    elif method == 'ewma':
        return pd.Series(predictions).ewm(alpha=alpha).mean().values
    else:
        raise ValueError(f"Unknown smoothing method: {method}")


def calculate_speed_kmh(distance_m: float, time_s: float) -> float:
    """
    Calculate speed in km/h from distance and time.
    
    Args:
        distance_m: Distance in meters
        time_s: Time in seconds
        
    Returns:
        Speed in km/h
    """
    if time_s <= 0:
        return 0.0
    return (distance_m / 1000) / (time_s / 3600)


def haversine_distance(lat1: float, lon1: float, 
                      lat2: float, lon2: float) -> float:
    """
    Calculate great-circle distance between two points.
    
    Args:
        lat1, lon1: First point coordinates
        lat2, lon2: Second point coordinates
        
    Returns:
        Distance in meters
    """
    R = 6371000  # Earth radius in meters
    
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    
    a = np.sin(delta_phi / 2) ** 2 + \
        np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    return R * c


def format_seconds(seconds: float) -> str:
    """
    Format seconds as human-readable string.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted string (e.g., "2m 30s", "1h 15m")
    """
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"


def print_metrics_table(metrics: Dict[str, float], title: str = "Metrics"):
    """
    Pretty print metrics as a table.
    
    Args:
        metrics: Dictionary of metric names to values
        title: Table title
    """
    print(f"\n{'='*50}")
    print(f"{title:^50}")
    print(f"{'='*50}")
    
    for name, value in metrics.items():
        if isinstance(value, float):
            print(f"{name:.<40} {value:.4f}")
        else:
            print(f"{name:.<40} {value}")
    
    print(f"{'='*50}\n")


def train_test_summary(train_df: pd.DataFrame, 
                       test_df: pd.DataFrame,
                       val_df: Optional[pd.DataFrame] = None):
    """
    Print summary of train/test split.
    
    Args:
        train_df: Training dataframe
        test_df: Test dataframe
        val_df: Optional validation dataframe
    """
    print("\n" + "="*60)
    print("DATASET SPLIT SUMMARY".center(60))
    print("="*60)
    
    print(f"\nTrain: {len(train_df):,} samples")
    print(f"  Date range: {train_df['vp_ts'].min()} to {train_df['vp_ts'].max()}")
    print(f"  Routes: {train_df['route_id'].nunique()}")
    print(f"  Mean ETA: {train_df['time_to_arrival_seconds'].mean()/60:.1f} min")
    
    if val_df is not None:
        print(f"\nValidation: {len(val_df):,} samples")
        print(f"  Date range: {val_df['vp_ts'].min()} to {val_df['vp_ts'].max()}")
        print(f"  Routes: {val_df['route_id'].nunique()}")
        print(f"  Mean ETA: {val_df['time_to_arrival_seconds'].mean()/60:.1f} min")
    
    print(f"\nTest: {len(test_df):,} samples")
    print(f"  Date range: {test_df['vp_ts'].min()} to {test_df['vp_ts'].max()}")
    print(f"  Routes: {test_df['route_id'].nunique()}")
    print(f"  Mean ETA: {test_df['time_to_arrival_seconds'].mean()/60:.1f} min")
    
    print("="*60 + "\n")


def create_feature_importance_df(feature_names: List[str],
                                 importances: np.ndarray,
                                 top_n: int = 20) -> pd.DataFrame:
    """
    Create sorted feature importance dataframe.
    
    Args:
        feature_names: List of feature names
        importances: Array of importance values
        top_n: Number of top features to return
        
    Returns:
        DataFrame sorted by importance
    """
    df = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    })
    
    df = df.sort_values('importance', ascending=False)
    
    if top_n:
        df = df.head(top_n)
    
    return df