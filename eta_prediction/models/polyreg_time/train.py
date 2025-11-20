"""
Polynomial Regression Model - Time Enhanced
Fits polynomial features with temporal, operational, and optional weather features.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from common.data import load_dataset
from common.metrics import compute_all_metrics
from common.keys import ModelKey
from common.registry import get_registry
from common.utils import print_metrics_table, train_test_summary, clip_predictions


class PolyRegTimeModel:
    """
    Enhanced polynomial regression with temporal/operational features.
    
    Features: 
    - Core: distance_to_stop (polynomialized)
    - Temporal: hour, day_of_week, is_weekend, is_peak_hour
    - Operational: headway_seconds, current_speed_kmh
    - Weather: temperature_c, precipitation_mm, wind_speed_kmh
    """
    
    def __init__(self,
                 poly_degree: int = 2,
                 alpha: float = 1.0,
                 include_temporal: bool = True,
                 include_operational: bool = True,
                 include_weather: bool = False,
                 handle_nan: str = 'drop'):  # 'drop', 'impute', or 'error'
        """
        Initialize model.
        
        Args:
            poly_degree: Polynomial degree for distance
            alpha: Ridge regularization strength
            include_temporal: Include time-of-day features
            include_operational: Include headway/speed features
            include_weather: Include weather features
            handle_nan: How to handle NaN - 'drop', 'impute', or 'error'
        """
        self.poly_degree = poly_degree
        self.alpha = alpha
        self.include_temporal = include_temporal
        self.include_operational = include_operational
        self.include_weather = include_weather
        self.handle_nan = handle_nan
        self.model = None
        self.feature_cols = None
        self.available_features = None
        
    def _get_feature_groups(self) -> Dict[str, List[str]]:
        """Define feature groups."""
        return {
            'core': ['distance_to_stop'],
            'temporal': ['hour', 'day_of_week', 'is_weekend', 'is_peak_hour'] 
                       if self.include_temporal else [],
            'operational': ['headway_seconds', 'current_speed_kmh'] 
                          if self.include_operational else [],
            'weather': ['temperature_c', 'precipitation_mm', 'wind_speed_kmh'] 
                      if self.include_weather else []
        }
    
    def _clean_data(self, df: pd.DataFrame, 
                   target_col: str = 'time_to_arrival_seconds') -> pd.DataFrame:
        """
        Clean data and handle NaN values.
        
        Args:
            df: Input dataframe
            target_col: Target column name
            
        Returns:
            Cleaned dataframe
        """
        print(f"\n{'='*60}")
        print("Data Cleaning")
        print(f"{'='*60}")
        print(f"Initial rows: {len(df):,}")
        
        # Get all potential features
        feature_groups = self._get_feature_groups()
        all_features = []
        for group_features in feature_groups.values():
            all_features.extend(group_features)
        
        # Check which features are available and have acceptable NaN levels
        available = []
        missing = []
        high_nan = []
        
        for feat in all_features:
            if feat not in df.columns:
                missing.append(feat)
                continue
            
            nan_ratio = df[feat].isna().sum() / len(df)
            
            if nan_ratio > 0.3:  # More than 30% NaN
                high_nan.append((feat, f"{nan_ratio*100:.1f}%"))
            else:
                available.append(feat)
        
        # Print feature availability
        if missing:
            print(f"\n⚠️  Missing features: {', '.join(missing)}")
        if high_nan:
            print(f"⚠️  High NaN features (>30%):")
            for feat, ratio in high_nan:
                print(f"   - {feat}: {ratio} NaN")
        
        print(f"\n✓ Available features ({len(available)}):")
        for feat in available:
            nan_count = df[feat].isna().sum()
            if nan_count > 0:
                print(f"   - {feat} ({nan_count:,} NaN)")
            else:
                print(f"   - {feat}")
        
        # Store available features
        self.available_features = available
        
        # Handle NaN based on strategy
        if self.handle_nan == 'error':
            # Check for any NaN in available features + target
            check_cols = available + [target_col]
            nan_counts = df[check_cols].isna().sum()
            if nan_counts.sum() > 0:
                print("\nNaN values found:")
                for col, count in nan_counts[nan_counts > 0].items():
                    print(f"  {col}: {count}")
                raise ValueError("NaN values found and handle_nan='error'")
            df_clean = df
            
        elif self.handle_nan == 'drop':
            # Drop rows with NaN in available features or target
            check_cols = available + [target_col]
            initial_len = len(df)
            df_clean = df.dropna(subset=check_cols)
            dropped = initial_len - len(df_clean)
            
            if dropped > 0:
                pct = (dropped / initial_len) * 100
                print(f"\n✓ Dropped {dropped:,} rows ({pct:.2f}%) with NaN values")
            
        elif self.handle_nan == 'impute':
            # Impute NaN values
            df_clean = df.copy()
            imputed = []
            
            for feat in available:
                nan_count = df_clean[feat].isna().sum()
                if nan_count > 0:
                    if pd.api.types.is_numeric_dtype(df_clean[feat]):
                        fill_val = df_clean[feat].median()
                        df_clean[feat] = df_clean[feat].fillna(fill_val)
                        imputed.append(f"{feat} (median={fill_val:.2f})")
                    else:
                        mode_val = df_clean[feat].mode()[0]
                        df_clean[feat] = df_clean[feat].fillna(mode_val)
                        imputed.append(f"{feat} (mode={mode_val})")
            
            if imputed:
                print(f"\n✓ Imputed features:")
                for imp in imputed:
                    print(f"   - {imp}")
            
            # Still drop rows with target NaN
            target_nan = df_clean[target_col].isna().sum()
            if target_nan > 0:
                df_clean = df_clean.dropna(subset=[target_col])
                print(f"\n✓ Dropped {target_nan:,} rows with target NaN")
        
        else:
            raise ValueError(f"Invalid handle_nan: {self.handle_nan}")
        
        print(f"\nFinal rows: {len(df_clean):,}")
        
        # Validate no NaN remains
        remaining_nan = df_clean[available + [target_col]].isna().sum().sum()
        if remaining_nan > 0:
            raise ValueError(f"ERROR: {remaining_nan} NaN values remain after cleaning!")
        
        print("✓ No NaN values in features or target")
        
        return df_clean
    
    def _create_pipeline(self) -> Pipeline:
        """Create sklearn pipeline."""
        # Separate polynomial features for distance and scaling for others
        return Pipeline([
            ('features', ColumnTransformer([
                ('poly_distance', PolynomialFeatures(
                    degree=self.poly_degree, 
                    include_bias=False
                ), [0]),  # First column is distance
                ('scale_others', StandardScaler(), slice(1, None))  # Rest of features
            ])),
            ('ridge', Ridge(alpha=self.alpha))
        ])
    
    def fit(self, train_df: pd.DataFrame, 
            target_col: str = 'time_to_arrival_seconds'):
        """
        Train model.
        
        Args:
            train_df: Training dataframe
            target_col: Target column name
        """
        # Clean data
        train_clean = self._clean_data(train_df, target_col)
        
        # Build feature list from available features
        feature_groups = self._get_feature_groups()
        self.feature_cols = []
        
        # Always include core (distance)
        for feat in feature_groups['core']:
            if feat in self.available_features:
                self.feature_cols.append(feat)
        
        # Add other available features
        for group in ['temporal', 'operational', 'weather']:
            for feat in feature_groups[group]:
                if feat in self.available_features:
                    self.feature_cols.append(feat)
        
        if not self.feature_cols:
            raise ValueError("No features available after cleaning!")
        
        print(f"\n{'='*60}")
        print("Model Training")
        print(f"{'='*60}")
        print(f"Features ({len(self.feature_cols)}): {', '.join(self.feature_cols)}")
        
        # Prepare data
        X = train_clean[self.feature_cols].values
        y = train_clean[target_col].values
        
        # Create and fit model
        self.model = self._create_pipeline()
        self.model.fit(X, y)
        
        print(f"✓ Model trained (poly_degree={self.poly_degree}, alpha={self.alpha})")
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict ETAs.
        
        Args:
            X: DataFrame with features
            
        Returns:
            Array of predicted ETAs in seconds
        """
        if self.model is None:
            raise ValueError("Model not trained")
        
        if self.feature_cols is None:
            raise ValueError("Feature columns not set")
        
        # Check for missing features
        missing = [f for f in self.feature_cols if f not in X.columns]
        if missing:
            raise ValueError(f"Missing features in input: {missing}")
        
        # Handle NaN in prediction data
        X_pred = X[self.feature_cols].copy()
        
        if self.handle_nan == 'impute':
            # Impute with median/mode
            for col in self.feature_cols:
                if X_pred[col].isna().any():
                    if pd.api.types.is_numeric_dtype(X_pred[col]):
                        X_pred[col] = X_pred[col].fillna(X_pred[col].median())
                    else:
                        X_pred[col] = X_pred[col].fillna(X_pred[col].mode()[0])
        elif self.handle_nan == 'drop':
            # For prediction, we can't drop - impute instead
            for col in self.feature_cols:
                if X_pred[col].isna().any():
                    if pd.api.types.is_numeric_dtype(X_pred[col]):
                        X_pred[col] = X_pred[col].fillna(0)  # Safe default
        
        X_array = X_pred.values
        predictions = self.model.predict(X_array)
        
        return clip_predictions(predictions)
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature coefficients (approximate importance)."""
        if self.model is None:
            return {}
        
        coefs = self.model.named_steps['ridge'].coef_
        
        importance = {}
        for i, feat in enumerate(self.feature_cols):
            importance[feat] = abs(coefs[i])
        
        # Sort by importance
        return dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))


def train_polyreg_time(dataset_name: str = "sample_dataset",
                      route_id: Optional[str] = None,
                      poly_degree: int = 2,
                      alpha: float = 1.0,
                      include_temporal: bool = True,
                      include_operational: bool = True,
                      include_weather: bool = False,
                      handle_nan: str = 'drop',
                      test_size: float = 0.2,
                      save_model: bool = True) -> Dict:
    """
    Train and evaluate polynomial regression time model.
    
    Args:
        dataset_name: Dataset name
        route_id: Optional route ID for route-specific training
        poly_degree: Polynomial degree for distance
        alpha: Ridge regularization
        include_temporal: Include temporal features
        include_operational: Include operational features
        include_weather: Include weather features
        handle_nan: 'drop', 'impute', or 'error'
        test_size: Test set fraction
        save_model: Save to registry
        
    Returns:
        Dictionary with model, metrics, metadata
    """
    print(f"\n{'='*60}")
    print(f"Polynomial Regression Time Model".center(60))
    print(f"{'='*60}\n")
    
    route_info = f" (route: {route_id})" if route_id else " (global)"
    print(f"Scope{route_info}")
    print(f"Config:")
    print(f"  poly_degree={poly_degree}, alpha={alpha}")
    print(f"  temporal={include_temporal}, operational={include_operational}")
    print(f"  weather={include_weather}, handle_nan='{handle_nan}'")
    
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
    
    # Split data
    train_df, val_df, test_df = dataset.temporal_split(
        train_frac=1-test_size-0.1,
        val_frac=0.1
    )
    
    train_test_summary(train_df, test_df, val_df)
    
    # Train model
    print("\n" + "="*60)
    print("Training")
    print("="*60)
    model = PolyRegTimeModel(
        poly_degree=poly_degree,
        alpha=alpha,
        include_temporal=include_temporal,
        include_operational=include_operational,
        include_weather=include_weather,
        handle_nan=handle_nan
    )
    model.fit(train_df)
    
    # Validation
    print(f"\n{'='*60}")
    print("Validation Performance")
    print("="*60)
    y_val = val_df['time_to_arrival_seconds'].values
    val_preds = model.predict(val_df)
    val_metrics = compute_all_metrics(y_val, val_preds, prefix="val_")
    print_metrics_table(val_metrics, "Validation")
    
    # Test
    print(f"\n{'='*60}")
    print("Test Performance")
    print("="*60)
    y_test = test_df['time_to_arrival_seconds'].values
    test_preds = model.predict(test_df)
    test_metrics = compute_all_metrics(y_test, test_preds, prefix="test_")
    print_metrics_table(test_metrics, "Test")
    
    # Feature importance
    importance = model.get_feature_importance()
    if importance:
        print(f"\nTop 5 Features by Coefficient:")
        for i, (feat, coef) in enumerate(list(importance.items())[:5], 1):
            print(f"  {i}. {feat}: {coef:.6f}")
    
    # Metadata
    metadata = {
        'model_type': 'polyreg_time',
        'dataset': dataset_name,
        'route_id': route_id,
        'poly_degree': poly_degree,
        'alpha': alpha,
        'include_temporal': include_temporal,
        'include_operational': include_operational,
        'include_weather': include_weather,
        'handle_nan': handle_nan,
        'n_features': len(model.feature_cols) if model.feature_cols else 0,
        'features': model.feature_cols,
        'n_samples': len(train_df) + len(val_df) + len(test_df),
        'n_trips': dataset.df['trip_id'].nunique() if route_id else None,
        'train_samples': len(train_df),
        'test_samples': len(test_df),
        'metrics': {**val_metrics, **test_metrics}
    }
    
    # Save
    if save_model:
        feature_groups = ['temporal', 'operational'] if include_temporal else ['operational']
        model_key = ModelKey.generate(
            model_type='polyreg_time',
            dataset_name=dataset_name,
            feature_groups=feature_groups,
            route_id=route_id,
            degree=poly_degree,
            handle_nan=handle_nan
        )
        
        registry = get_registry()
        registry.save_model(model_key, model, metadata)
        metadata['model_key'] = model_key
        print(f"\n✓ Model saved: {model_key}")
    
    return {
        'model': model,
        'metrics': metadata['metrics'],
        'metadata': metadata
    }


if __name__ == "__main__":
    # Example: Train with different configurations
    
    # Basic model - drop NaN
    result1 = train_polyreg_time(
        poly_degree=2,
        include_temporal=True,
        include_operational=True,
        include_weather=False,
        handle_nan='drop'
    )
    
    # With imputation
    result2 = train_polyreg_time(
        poly_degree=2,
        include_temporal=True,
        include_operational=True,
        include_weather=False,
        handle_nan='impute'
    )