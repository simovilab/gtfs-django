"""
Polynomial Regression Model - Distance-based
Fits polynomial features on distance_to_stop with optional route-specific models.
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from common.data import load_dataset, prepare_features_target
from common.metrics import compute_all_metrics
from common.keys import ModelKey
from common.registry import get_registry
from common.utils import print_metrics_table, train_test_summary, clip_predictions


class PolyRegDistanceModel:
    """
    Polynomial regression on distance with optional route-specific models.
    
    Features: distance_to_stop, (distance)^2, (distance)^3, ...
    Can fit separate models per route for better performance.
    """
    
    def __init__(self, 
                 degree: int = 2,
                 alpha: float = 1.0,
                 route_specific: bool = False):
        """
        Initialize model.
        
        Args:
            degree: Polynomial degree (2 or 3 recommended)
            alpha: Ridge regression alpha (regularization strength)
            route_specific: Whether to fit separate model per route
        """
        self.degree = degree
        self.alpha = alpha
        self.route_specific = route_specific
        self.models = {}  # route_id -> model mapping
        self.global_model = None
        self.feature_cols = ['distance_to_stop']
        
    def _create_pipeline(self) -> Pipeline:
        """Create sklearn pipeline with polynomial features and ridge regression."""
        return Pipeline([
            ('poly', PolynomialFeatures(degree=self.degree, include_bias=True)),
            ('ridge', Ridge(alpha=self.alpha))
        ])
    
    def fit(self, train_df: pd.DataFrame, target_col: str = 'time_to_arrival_seconds'):
        """
        Train model(s).
        
        Args:
            train_df: Training dataframe with distance_to_stop and target
            target_col: Name of target column
        """
        if 'distance_to_stop' not in train_df.columns:
            raise ValueError("distance_to_stop column required")
        
        if self.route_specific:
            # Fit separate model per route
            for route_id, route_df in train_df.groupby('route_id'):
                X = route_df[['distance_to_stop']].values
                y = route_df[target_col].values
                
                model = self._create_pipeline()
                model.fit(X, y)
                self.models[route_id] = model
            
            print(f"Trained {len(self.models)} route-specific models (degree={self.degree})")
        else:
            # Fit single global model
            X = train_df[['distance_to_stop']].values
            y = train_df[target_col].values
            
            self.global_model = self._create_pipeline()
            self.global_model.fit(X, y)
            
            print(f"Trained global model (degree={self.degree}, alpha={self.alpha})")
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict ETAs.
        
        Args:
            X: DataFrame with distance_to_stop (and route_id if route_specific)
            
        Returns:
            Array of predicted ETAs in seconds
        """
        if self.route_specific:
            if 'route_id' not in X.columns:
                raise ValueError("route_id required for route-specific model")
            
            # Create predictions array matching input length
            predictions = np.zeros(len(X))
            
            # Reset index to get positional indices
            X_reset = X.reset_index(drop=True)
            
            for route_id, route_df in X_reset.groupby('route_id'):
                # Get positional indices (0-based from reset_index)
                pos_indices = route_df.index.values
                X_route = route_df[['distance_to_stop']].values
                
                if route_id in self.models:
                    predictions[pos_indices] = self.models[route_id].predict(X_route)
                elif self.global_model is not None:
                    # Fallback to global model
                    predictions[pos_indices] = self.global_model.predict(X_route)
                else:
                    # No model available - use simple linear estimate (30 km/h avg)
                    predictions[pos_indices] = X_route.flatten() / 30000 * 3600
        else:
            if self.global_model is None:
                raise ValueError("Model not trained")
            
            X_dist = X[['distance_to_stop']].values
            predictions = self.global_model.predict(X_dist)
        
        # Clip to reasonable range
        return clip_predictions(predictions)
    
    def get_coefficients(self, route_id: Optional[str] = None) -> Dict:
        """
        Get model coefficients.
        
        Args:
            route_id: Route ID for route-specific model, None for global
            
        Returns:
            Dictionary with coefficients
        """
        if route_id and route_id in self.models:
            model = self.models[route_id]
        elif self.global_model:
            model = self.global_model
        else:
            return {}
        
        coefs = model.named_steps['ridge'].coef_
        intercept = model.named_steps['ridge'].intercept_
        
        return {
            'intercept': float(intercept),
            'coefficients': coefs.tolist(),
            'degree': self.degree
        }


def train_polyreg_distance(dataset_name: str = "sample_dataset",
                          route_id: Optional[str] = None,
                          degree: int = 2,
                          alpha: float = 1.0,
                          route_specific: bool = False,
                          test_size: float = 0.2,
                          save_model: bool = True,
                          pre_split: Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]] = None) -> Dict:
    """
    Train and evaluate polynomial regression distance model.
    
    Args:
        dataset_name: Name of dataset in datasets/ directory
        route_id: Optional route ID for route-specific training (overrides route_specific)
        degree: Polynomial degree
        alpha: Ridge regularization strength
        route_specific: Whether to fit per-route models (ignored if route_id specified)
        test_size: Fraction of data for testing
        save_model: Whether to save to registry
        pre_split: Optional (train, val, test) DataFrames to reuse
        
    Returns:
        Dictionary with model, metrics, and metadata
    """
    print(f"\n{'='*60}")
    print(f"Training Polynomial Regression Distance Model".center(60))
    print(f"{'='*60}\n")
    
    route_info = f" (route: {route_id})" if route_id else " (global)"
    print(f"Scope{route_info}")
    print(f"Config: degree={degree}, alpha={alpha}, route_specific={route_specific}")
    
    # Load dataset
    print(f"\nLoading dataset: {dataset_name}")
    dataset = load_dataset(dataset_name)
    dataset.clean_data()
    
    # Filter by route if specified (single-route model)
    if route_id is not None:
        df = dataset.df
        df_filtered = df[df['route_id'] == route_id].copy()
        print(f"Filtered to route {route_id}: {len(df_filtered):,} samples")
        
        if len(df_filtered) == 0:
            raise ValueError(f"No data found for route {route_id}")
        
        dataset.df = df_filtered
        # When training on single route, don't need route_specific flag
        route_specific = False
    
    if pre_split is not None:
        train_df, val_df, test_df = (df.copy() for df in pre_split)
    else:
        train_df, val_df, test_df = dataset.temporal_split(
            train_frac=1-test_size-0.1,
            val_frac=0.1
        )
    
    train_test_summary(train_df, test_df, val_df)
    
    # Train model
    print("Training model...")
    model = PolyRegDistanceModel(
        degree=degree,
        alpha=alpha,
        route_specific=route_specific
    )
    model.fit(train_df)
    
    # Evaluate on validation set
    print("\nValidation Performance:")
    y_val = val_df['time_to_arrival_seconds'].values
    val_preds = model.predict(val_df)
    val_metrics = compute_all_metrics(y_val, val_preds, prefix="val_")
    print_metrics_table(val_metrics, "Validation Metrics")
    
    # Evaluate on test set
    print("\nTest Performance:")
    y_test = test_df['time_to_arrival_seconds'].values
    test_preds = model.predict(test_df)
    test_metrics = compute_all_metrics(y_test, test_preds, prefix="test_")
    print_metrics_table(test_metrics, "Test Metrics")
    
    # Get sample coefficients
    sample_coefs = model.get_coefficients(route_id=route_id)
    if sample_coefs:
        print(f"\nModel coefficients:")
        print(f"  Intercept: {sample_coefs['intercept']:.2f}")
        print(f"  Coefficients: {[f'{c:.6f}' for c in sample_coefs['coefficients'][:5]]}")
    
    # Prepare metadata
    metadata = {
        'model_type': 'polyreg_distance',
        'dataset': dataset_name,
        'route_id': route_id,
        'degree': degree,
        'alpha': alpha,
        'route_specific': route_specific,
        'n_models': len(model.models) if route_specific else 1,
        'n_samples': len(train_df) + len(val_df) + len(test_df),
        'n_trips': dataset.df['trip_id'].nunique() if route_id else None,
        'train_samples': len(train_df),
        'test_samples': len(test_df),
        'metrics': {**val_metrics, **test_metrics}
    }
    
    # Save model
    if save_model:
        model_key = ModelKey.generate(
            model_type='polyreg_distance',
            dataset_name=dataset_name,
            feature_groups=['distance'],
            route_id=route_id,
            degree=degree,
            route_specific='yes' if route_specific else 'no'
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
    # Train different configurations
    
    # Degree 2, global model
    result1 = train_polyreg_distance(
        degree=2,
        alpha=1.0,
        route_specific=False
    )
    
    # Degree 3, global model
    result2 = train_polyreg_distance(
        degree=3,
        alpha=1.0,
        route_specific=False
    )
    
    # Degree 2, route-specific
    result3 = train_polyreg_distance(
        degree=2,
        alpha=1.0,
        route_specific=True
    )
    
    # Compare results
    print("\n" + "="*60)
    print("Model Comparison (Test MAE)")
    print("="*60)
    for i, result in enumerate([result1, result2, result3], 1):
        mae_min = result['metrics']['test_mae_minutes']
        print(f"Model {i}: {mae_min:.3f} minutes")
