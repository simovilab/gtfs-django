"""
Exponentially Weighted Moving Average (EWMA) Model
Adapts predictions based on recent observations with exponential decay.
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from collections import defaultdict
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from common.data import load_dataset
from common.metrics import compute_all_metrics
from common.keys import ModelKey
from common.registry import get_registry
from common.utils import print_metrics_table, train_test_summary, clip_predictions


class EWMAModel:
    """
    EWMA-based ETA prediction.
    
    Maintains exponentially weighted moving averages for each (route, stop) pair.
    Updates incrementally as new observations arrive.
    
    ETA_new = alpha * observed + (1 - alpha) * ETA_old
    """
    
    def __init__(self, 
                 alpha: float = 0.3,
                 group_by: list = ['route_id', 'stop_sequence'],
                 min_observations: int = 3):
        """
        Initialize EWMA model.
        
        Args:
            alpha: Smoothing parameter (0-1, higher = more weight on recent)
            group_by: Features to group by
            min_observations: Min observations before using EWMA
        """
        self.alpha = alpha
        self.group_by = group_by
        self.min_observations = min_observations
        
        self.ewma_values = {}  # (route, stop, ...) -> current EWMA
        self.observation_counts = {}  # (route, stop, ...) -> count
        self.global_mean = None
        
    def _make_key(self, row: pd.Series) -> tuple:
        """Create lookup key from row."""
        return tuple(row[col] for col in self.group_by)
    
    def fit(self, train_df: pd.DataFrame, target_col: str = 'time_to_arrival_seconds'):
        """
        Train EWMA model by processing observations in time order.
        
        Args:
            train_df: Training dataframe (should be time-sorted)
            target_col: Target column name
        """
        # Sort by timestamp
        df_sorted = train_df.sort_values('vp_ts').reset_index(drop=True)
        
        self.global_mean = df_sorted[target_col].mean()
        
        # Process observations sequentially
        for _, row in df_sorted.iterrows():
            key = self._make_key(row)
            observed = row[target_col]
            
            if key not in self.ewma_values:
                # Initialize with first observation
                self.ewma_values[key] = observed
                self.observation_counts[key] = 1
            else:
                # Update EWMA
                old_ewma = self.ewma_values[key]
                new_ewma = self.alpha * observed + (1 - self.alpha) * old_ewma
                self.ewma_values[key] = new_ewma
                self.observation_counts[key] += 1
        
        print(f"Trained EWMA model (alpha={self.alpha})")
        print(f"  Unique groups: {len(self.ewma_values)}")
        print(f"  Total observations: {len(df_sorted)}")
        print(f"  Global mean: {self.global_mean/60:.2f} minutes")
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict ETAs using current EWMA values.
        
        Args:
            X: DataFrame with group_by columns
            
        Returns:
            Array of predicted ETAs
        """
        predictions = []
        
        for _, row in X.iterrows():
            key = self._make_key(row)
            
            if key in self.ewma_values:
                count = self.observation_counts[key]
                if count >= self.min_observations:
                    predictions.append(self.ewma_values[key])
                else:
                    # Not enough observations, use global mean
                    predictions.append(self.global_mean)
            else:
                # New group, use global mean
                predictions.append(self.global_mean)
        
        return clip_predictions(np.array(predictions))
    
    def update(self, X: pd.DataFrame, y: np.ndarray):
        """
        Update EWMA values with new observations (online learning).
        
        Args:
            X: Features
            y: Observed values
        """
        for (_, row), observed in zip(X.iterrows(), y):
            key = self._make_key(row)
            
            if key not in self.ewma_values:
                self.ewma_values[key] = observed
                self.observation_counts[key] = 1
            else:
                old_ewma = self.ewma_values[key]
                new_ewma = self.alpha * observed + (1 - self.alpha) * old_ewma
                self.ewma_values[key] = new_ewma
                self.observation_counts[key] += 1
    
    def get_coverage(self, X: pd.DataFrame) -> float:
        """Get fraction of predictions with EWMA values."""
        covered = 0
        for _, row in X.iterrows():
            key = self._make_key(row)
            if key in self.ewma_values and self.observation_counts[key] >= self.min_observations:
                covered += 1
        return covered / len(X)
    
    def get_group_stats(self) -> pd.DataFrame:
        """Get statistics about learned groups."""
        stats = []
        for key, ewma in self.ewma_values.items():
            count = self.observation_counts[key]
            stats.append({
                **{col: val for col, val in zip(self.group_by, key)},
                'ewma_eta_minutes': ewma / 60,
                'n_observations': count
            })
        
        return pd.DataFrame(stats).sort_values('n_observations', ascending=False)


def train_ewma(dataset_name: str = "sample_dataset",
               route_id: Optional[str] = None,
               alpha: float = 0.3,
               group_by: list = ['route_id', 'stop_sequence'],
               min_observations: int = 3,
               test_size: float = 0.2,
               save_model: bool = True,
               pre_split: Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]] = None) -> Dict:
    """
    Train and evaluate EWMA model.
    
    Args:
        dataset_name: Name of dataset
        route_id: Optional route ID for route-specific training
        alpha: EWMA smoothing parameter
        group_by: Grouping columns
        min_observations: Minimum observations threshold
        test_size: Test fraction
        save_model: Whether to save
        pre_split: Optional (train, val, test) DataFrames to reuse
        
    Returns:
        Dictionary with model, metrics, metadata
    """
    print(f"\n{'='*60}")
    print(f"Training EWMA Model".center(60))
    print(f"{'='*60}\n")
    
    route_info = f" (route: {route_id})" if route_id else " (global)"
    print(f"Scope{route_info}")
    print(f"Config: alpha={alpha}, group_by={group_by}, min_obs={min_observations}")
    
    # Load dataset
    print(f"\nLoading dataset: {dataset_name}")
    dataset = load_dataset(dataset_name)
    dataset.clean_data()
    
    # Filter by route if specified
    if route_id is not None:
        df = dataset.df
        df_filtered = df[df['route_id'] == route_id].copy()
        print(f"Filtered to route {route_id}: {len(df_filtered):,} samples")
        
        if len(df_filtered) == 0:
            raise ValueError(f"No data found for route {route_id}")
        
        dataset.df = df_filtered
    
    if pre_split is not None:
        train_df, val_df, test_df = (df.copy() for df in pre_split)
    else:
        # Split data temporally (important for time series)
        train_df, val_df, test_df = dataset.temporal_split(
            train_frac=1-test_size-0.1,
            val_frac=0.1
        )
    
    train_test_summary(train_df, test_df, val_df)
    
    # Train model
    print("Training model...")
    model = EWMAModel(
        alpha=alpha,
        group_by=group_by,
        min_observations=min_observations
    )
    model.fit(train_df)
    
    # Evaluate on validation (with optional online updates)
    print("\nValidation Performance:")
    y_val = val_df['time_to_arrival_seconds'].values
    val_preds = model.predict(val_df)
    val_metrics = compute_all_metrics(y_val, val_preds, prefix="val_")
    print_metrics_table(val_metrics, "Validation Metrics")
    
    val_coverage = model.get_coverage(val_df)
    print(f"Validation coverage: {val_coverage*100:.1f}%")
    
    # Optionally update model with validation data
    print("\nUpdating model with validation data...")
    model.update(val_df, y_val)
    
    # Evaluate on test set
    print("\nTest Performance:")
    y_test = test_df['time_to_arrival_seconds'].values
    test_preds = model.predict(test_df)
    test_metrics = compute_all_metrics(y_test, test_preds, prefix="test_")
    print_metrics_table(test_metrics, "Test Metrics")
    
    test_coverage = model.get_coverage(test_df)
    print(f"Test coverage: {test_coverage*100:.1f}%")
    
    # Show some group stats
    group_stats = model.get_group_stats()
    print(f"\nTop 10 groups by observation count:")
    print(group_stats.head(10))
    
    # Prepare metadata
    metadata = {
        'model_type': 'ewma',
        'dataset': dataset_name,
        'route_id': route_id,
        'alpha': alpha,
        'group_by': group_by,
        'min_observations': min_observations,
        'n_groups': len(model.ewma_values),
        'n_samples': len(train_df) + len(val_df) + len(test_df),
        'n_trips': dataset.df['trip_id'].nunique() if route_id else None,
        'train_samples': len(train_df),
        'test_samples': len(test_df),
        'test_coverage': float(test_coverage),
        'global_mean_eta': float(model.global_mean),
        'metrics': {**val_metrics, **test_metrics}
    }
    
    # Save model
    if save_model:
        model_key = ModelKey.generate(
            model_type='ewma',
            dataset_name=dataset_name,
            feature_groups=['temporal', 'route'],
            route_id=route_id,
            alpha=str(alpha).replace('.', '_')
        )
        
        registry = get_registry()
        registry.save_model(model_key, model, metadata)
        metadata['model_key'] = model_key
    
    return {
        'model': model,
        'metrics': metadata['metrics'],
        'metadata': metadata
    }


if __name__ == "__main__":
    # Train with different alpha values
    
    # Conservative (slow adaptation)
    result1 = train_ewma(alpha=0.1)
    
    # Balanced
    result2 = train_ewma(alpha=0.3)
    
    # Aggressive (fast adaptation)
    result3 = train_ewma(alpha=0.5)
    
    # With hourly grouping
    result4 = train_ewma(
        alpha=0.3,
        group_by=['route_id', 'stop_sequence', 'hour']
    )
    
    # Compare
    print("\n" + "="*60)
    print("Model Comparison (Test MAE)")
    print("="*60)
    results = [result1, result2, result3, result4]
    labels = ["alpha=0.1", "alpha=0.3", "alpha=0.5", "alpha=0.3+hour"]
    
    for label, result in zip(labels, results):
        mae_min = result['metrics']['test_mae_minutes']
        print(f"{label:20s}: {mae_min:.3f} minutes")
