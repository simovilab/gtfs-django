"""
Evaluation metrics for ETA prediction models.
Provides domain-specific metrics beyond standard regression metrics.
"""

import numpy as np
import pandas as pd
from typing import Dict, Optional, Tuple
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def mae_seconds(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean Absolute Error in seconds."""
    return mean_absolute_error(y_true, y_pred)


def mae_minutes(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean Absolute Error in minutes."""
    return mean_absolute_error(y_true, y_pred) / 60


def rmse_seconds(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Root Mean Squared Error in seconds."""
    return np.sqrt(mean_squared_error(y_true, y_pred))


def rmse_minutes(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Root Mean Squared Error in minutes."""
    return np.sqrt(mean_squared_error(y_true, y_pred)) / 60


def mape(y_true: np.ndarray, y_pred: np.ndarray, epsilon: float = 1.0) -> float:
    """
    Mean Absolute Percentage Error.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        epsilon: Small value to avoid division by zero
        
    Returns:
        MAPE as percentage
    """
    return 100 * np.mean(np.abs((y_true - y_pred) / (y_true + epsilon)))


def median_ae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Median Absolute Error in seconds (robust to outliers)."""
    return np.median(np.abs(y_true - y_pred))


def within_threshold(y_true: np.ndarray, 
                     y_pred: np.ndarray, 
                     threshold_seconds: float = 60) -> float:
    """
    Fraction of predictions within threshold.
    
    Args:
        y_true: True values in seconds
        y_pred: Predicted values in seconds
        threshold_seconds: Acceptable error threshold
        
    Returns:
        Fraction of predictions within threshold (0-1)
    """
    errors = np.abs(y_true - y_pred)
    return np.mean(errors <= threshold_seconds)


def late_penalty_mae(y_true: np.ndarray, 
                     y_pred: np.ndarray,
                     late_multiplier: float = 2.0) -> float:
    """
    MAE with higher penalty for late predictions (user-centric).
    
    Args:
        y_true: True values
        y_pred: Predicted values
        late_multiplier: Multiplier for underprediction errors
        
    Returns:
        Weighted MAE
    """
    errors = y_pred - y_true
    weights = np.where(errors < 0, late_multiplier, 1.0)
    return np.mean(np.abs(errors) * weights)


def quantile_error(y_true: np.ndarray, 
                   y_pred: np.ndarray,
                   quantiles: list = [0.5, 0.9, 0.95]) -> Dict[str, float]:
    """
    Absolute errors at different quantiles.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        quantiles: List of quantiles to compute
        
    Returns:
        Dictionary mapping quantile to error
    """
    errors = np.abs(y_true - y_pred)
    return {f"q{int(q*100)}": np.quantile(errors, q) for q in quantiles}


def bias(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Mean bias (positive = overprediction, negative = underprediction).
    
    Returns:
        Mean bias in seconds
    """
    return np.mean(y_pred - y_true)


def compute_all_metrics(y_true: np.ndarray, 
                        y_pred: np.ndarray,
                        prefix: str = "") -> Dict[str, float]:
    """
    Compute comprehensive set of metrics.
    
    Args:
        y_true: True values in seconds
        y_pred: Predicted values in seconds
        prefix: Optional prefix for metric names (e.g., "val_")
        
    Returns:
        Dictionary of all metrics
    """
    metrics = {
        f"{prefix}mae_seconds": mae_seconds(y_true, y_pred),
        f"{prefix}mae_minutes": mae_minutes(y_true, y_pred),
        f"{prefix}rmse_seconds": rmse_seconds(y_true, y_pred),
        f"{prefix}rmse_minutes": rmse_minutes(y_true, y_pred),
        f"{prefix}mape": mape(y_true, y_pred),
        f"{prefix}median_ae": median_ae(y_true, y_pred),
        f"{prefix}bias_seconds": bias(y_true, y_pred),
        f"{prefix}r2": r2_score(y_true, y_pred),
        f"{prefix}within_60s": within_threshold(y_true, y_pred, 60),
        f"{prefix}within_120s": within_threshold(y_true, y_pred, 120),
        f"{prefix}within_300s": within_threshold(y_true, y_pred, 300),
        f"{prefix}late_penalty_mae": late_penalty_mae(y_true, y_pred),
    }
    
    # Add quantile errors
    quantile_errs = quantile_error(y_true, y_pred)
    for k, v in quantile_errs.items():
        metrics[f"{prefix}error_{k}"] = v
    
    return metrics


def compare_models(results: Dict[str, Dict[str, float]], 
                   metric: str = "mae_seconds") -> pd.DataFrame:
    """
    Compare multiple models on a metric.
    
    Args:
        results: Dict mapping model_name -> metrics_dict
        metric: Metric to compare (or 'all' for all metrics)
        
    Returns:
        DataFrame with comparison
    """
    if metric == 'all':
        df = pd.DataFrame(results).T
    else:
        df = pd.DataFrame({
            'model': list(results.keys()),
            metric: [results[m].get(metric, np.nan) for m in results.keys()]
        })
        df = df.sort_values(metric)
    
    return df


def error_analysis(y_true: np.ndarray,
                   y_pred: np.ndarray,
                   feature_df: Optional[pd.DataFrame] = None,
                   group_by: Optional[str] = None) -> pd.DataFrame:
    """
    Analyze errors by different segments.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        feature_df: DataFrame with features for grouping
        group_by: Column name to group by
        
    Returns:
        DataFrame with error statistics per group
    """
    errors = np.abs(y_true - y_pred)
    
    if feature_df is None or group_by is None:
        # Overall statistics
        return pd.DataFrame({
            'count': [len(errors)],
            'mae': [np.mean(errors)],
            'median_ae': [np.median(errors)],
            'rmse': [np.sqrt(np.mean(errors**2))],
            'max_error': [np.max(errors)],
        })
    
    # Group-wise statistics
    df = feature_df.copy()
    df['error'] = errors
    
    stats = df.groupby(group_by)['error'].agg([
        ('count', 'count'),
        ('mae', 'mean'),
        ('median_ae', 'median'),
        ('std', 'std'),
        ('max', 'max')
    ]).round(2)
    
    return stats.sort_values('mae', ascending=False)


def prediction_intervals(y_pred: np.ndarray,
                        residuals: np.ndarray,
                        confidence: float = 0.95) -> Tuple[np.ndarray, np.ndarray]:
    """
    Compute prediction intervals based on residual distribution.
    
    Args:
        y_pred: Point predictions
        residuals: Training residuals (y_true - y_pred)
        confidence: Confidence level (e.g., 0.95 for 95% interval)
        
    Returns:
        lower_bound, upper_bound arrays
    """
    alpha = 1 - confidence
    lower_q = alpha / 2
    upper_q = 1 - alpha / 2
    
    lower_percentile = np.percentile(residuals, lower_q * 100)
    upper_percentile = np.percentile(residuals, upper_q * 100)
    
    lower_bound = y_pred + lower_percentile
    upper_bound = y_pred + upper_percentile
    
    return lower_bound, upper_bound