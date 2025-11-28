"""
Data loading and preprocessing utilities for ETA prediction models.
Handles dataset splitting, feature engineering, and train/test preparation.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, List, Optional, Dict
from datetime import datetime, timedelta


class ETADataset:
    """
    Manages ETA prediction datasets with consistent preprocessing and splitting.
    
    Expected columns from VP dataset builder:
    - Identifiers: trip_id, route_id, vehicle_id, stop_id, stop_sequence
    - Position: vp_ts, vp_lat, vp_lon, vp_bearing
    - Stop: stop_lat, stop_lon, distance_to_stop
    - Target: actual_arrival, time_to_arrival_seconds
    - Temporal: hour, day_of_week, is_weekend, is_holiday, is_peak_hour
    - Operational: headway_seconds, current_speed_kmh
    - Weather (optional): temperature_c, precipitation_mm, wind_speed_kmh
    """
    
    FEATURE_GROUPS = {
        'identifiers': ['trip_id', 'route_id', 'vehicle_id', 'stop_id', 'stop_sequence'],
        'position': ['vp_lat', 'vp_lon', 'vp_bearing', 'distance_to_stop'],
        'temporal': ['hour', 'day_of_week', 'is_weekend', 'is_holiday', 'is_peak_hour'],
        'operational': ['headway_seconds', 'current_speed_kmh'],
        'weather': ['temperature_c', 'precipitation_mm', 'wind_speed_kmh'],
        'target': ['time_to_arrival_seconds']
    }
    
    def __init__(self, data_path: str):
        """
        Initialize dataset from parquet file.
        
        Args:
            data_path: Path to parquet file from VP dataset builder
        """
        self.data_path = Path(data_path)
        self.df = pd.read_parquet(data_path)
        self.df['vp_ts'] = pd.to_datetime(self.df['vp_ts'])
        
        # Store original size
        self.original_size = len(self.df)
        
    def clean_data(self, 
                   drop_missing_target: bool = True,
                   max_eta_seconds: float = 3600 * 2,  # 2 hours
                   min_distance: float = 10.0) -> 'ETADataset':
        """
        Clean dataset by removing invalid rows.
        
        Args:
            drop_missing_target: Remove rows without valid ETA target
            max_eta_seconds: Maximum reasonable ETA (filter outliers)
            min_distance: Minimum distance to stop (meters) to keep
            
        Returns:
            Self for chaining
        """
        initial_rows = len(self.df)
        
        if drop_missing_target:
            self.df = self.df.dropna(subset=['time_to_arrival_seconds'])
        
        # Filter outliers
        self.df = self.df[
            (self.df['time_to_arrival_seconds'] >= 0) &
            (self.df['time_to_arrival_seconds'] <= max_eta_seconds)
        ]
        
        # Filter too-close stops (likely already passed)
        if 'distance_to_stop' in self.df.columns:
            self.df = self.df[self.df['distance_to_stop'] >= min_distance]
        
        print(f"Cleaned: {initial_rows} → {len(self.df)} rows "
              f"({100 * len(self.df) / initial_rows:.1f}% retained)")
        
        return self
    
    def get_features(self, feature_groups: List[str]) -> List[str]:
        """
        Get list of feature columns from specified groups.
        
        Args:
            feature_groups: List of group names (e.g., ['temporal', 'position'])
            
        Returns:
            List of column names that exist in the dataset
        """
        features = []
        for group in feature_groups:
            if group in self.FEATURE_GROUPS:
                features.extend(self.FEATURE_GROUPS[group])
        
        # Return only columns that exist
        return [f for f in features if f in self.df.columns]
    
    def temporal_split(self, 
                       train_frac: float = 0.7,
                       val_frac: float = 0.15) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Split dataset by time (avoids data leakage).
        
        Args:
            train_frac: Fraction for training
            val_frac: Fraction for validation (remainder goes to test)
            
        Returns:
            train_df, val_df, test_df
        """
        # Sort by timestamp
        df_sorted = self.df.sort_values('vp_ts').reset_index(drop=True)
        
        n = len(df_sorted)
        train_end = int(n * train_frac)
        val_end = int(n * (train_frac + val_frac))
        
        train_df = df_sorted.iloc[:train_end].copy()
        val_df = df_sorted.iloc[train_end:val_end].copy()
        test_df = df_sorted.iloc[val_end:].copy()
        
        print(f"Temporal split: train={len(train_df)}, val={len(val_df)}, test={len(test_df)}")
        
        return train_df, val_df, test_df
    
    def route_split(self,
                    test_routes: Optional[List[str]] = None,
                    test_frac: float = 0.2) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split by route for cross-route generalization testing.
        
        Args:
            test_routes: Specific routes for test set, or None to sample
            test_frac: Fraction of routes for testing if test_routes is None
            
        Returns:
            train_df, test_df
        """
        if test_routes is None:
            routes = self.df['route_id'].unique()
            n_test = max(1, int(len(routes) * test_frac))
            test_routes = np.random.choice(routes, size=n_test, replace=False)
        
        test_df = self.df[self.df['route_id'].isin(test_routes)].copy()
        train_df = self.df[~self.df['route_id'].isin(test_routes)].copy()
        
        print(f"Route split: {len(test_routes)} test routes")
        print(f"  Train: {len(train_df)} samples")
        print(f"  Test: {len(test_df)} samples")
        
        return train_df, test_df
    
    def get_route_stats(self) -> pd.DataFrame:
        """Get statistics per route."""
        return self.df.groupby('route_id').agg({
            'time_to_arrival_seconds': ['count', 'mean', 'std'],
            'distance_to_stop': ['mean', 'std'],
            'trip_id': 'nunique'
        }).round(2)
    
    def summary(self) -> Dict:
        """Get dataset summary statistics."""
        return {
            'total_samples': len(self.df),
            'date_range': (self.df['vp_ts'].min(), self.df['vp_ts'].max()),
            'routes': self.df['route_id'].nunique(),
            'trips': self.df['trip_id'].nunique(),
            'vehicles': self.df['vehicle_id'].nunique(),
            'stops': self.df['stop_id'].nunique(),
            'eta_mean_minutes': self.df['time_to_arrival_seconds'].mean() / 60,
            'eta_std_minutes': self.df['time_to_arrival_seconds'].std() / 60,
            'missing_weather': self.df['temperature_c'].isna().sum() if 'temperature_c' in self.df.columns else 'N/A'
        }


def load_dataset(dataset_name: str = "sample_dataset") -> ETADataset:
    """
    Load dataset from datasets directory.
    
    Args:
        dataset_name: Name of dataset (without .parquet extension)
        
    Returns:
        ETADataset instance
    """
    datasets_dir = Path(__file__).parent.parent.parent / "datasets"
    data_path = datasets_dir / f"{dataset_name}.parquet"
    
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")
    
    return ETADataset(str(data_path))


def prepare_features_target(df: pd.DataFrame, 
                            feature_cols: List[str],
                            target_col: str = 'time_to_arrival_seconds',
                            fill_na: bool = True) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Extract features and target, handle missing values.
    
    Args:
        df: DataFrame with features and target
        feature_cols: List of feature column names
        target_col: Name of target column
        fill_na: Whether to fill NaN values
        
    Returns:
        X (features), y (target)
    """
    # Get features that exist
    available_features = [f for f in feature_cols if f in df.columns]
    
    X = df[available_features].copy()
    y = df[target_col].copy()
    
    if fill_na:
        # Fill numeric features with median, boolean with False
        for col in X.columns:
            if X[col].dtype == 'bool':
                X[col] = X[col].fillna(False)
            elif X[col].dtype in ['int64', 'float64']:
                X[col] = X[col].fillna(X[col].median())
    
    return X, y