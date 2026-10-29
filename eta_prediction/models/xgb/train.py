"""
XGBoost Regression Model - Time & Spatial Features
Fits a gradient boosted tree model with temporal, spatial,
and optional weather features for ETA prediction.
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBRegressor

sys.path.append(str(Path(__file__).parent.parent))

from common.data import load_dataset
from common.metrics import compute_all_metrics
from common.keys import ModelKey
from common.registry import get_registry
from common.utils import print_metrics_table, train_test_summary, clip_predictions


def _make_one_hot_encoder() -> OneHotEncoder:
    """Return a OneHotEncoder that works across sklearn versions."""
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


class XGBTimeModel:
    """
    Gradient boosted tree model (XGBoost) with temporal/spatial features.
    
    Features: 
    - Core: distance_to_stop
    - Temporal: hour, day_of_week, is_weekend, is_peak_hour
    - Spatial: progress_on_segment, progress_ratio
    - Weather: temperature_c, precipitation_mm, wind_speed_kmh
    """

    def __init__(
        self,
        max_depth: int = 5,
        n_estimators: int = 200,
        learning_rate: float = 0.05,
        subsample: float = 0.8,
        colsample_bytree: float = 0.8,
        include_temporal: bool = True,
        include_spatial: bool = True,
        include_weather: bool = False,
        handle_nan: str = "drop",  # 'drop', 'impute', or 'error'
        random_state: int = 42,
        n_jobs: int = 4,
    ) -> None:
        """
        Initialize model.
        
        Args:
            max_depth: Maximum tree depth
            n_estimators: Number of boosting stages
            learning_rate: Learning rate (eta)
            subsample: Row subsample ratio per tree
            colsample_bytree: Column subsample ratio per tree
            include_temporal: Include time-of-day features
            include_spatial: Include spatial progress/segment features
            include_weather: Include weather features
            handle_nan: How to handle NaN - 'drop', 'impute', or 'error'
            random_state: Random seed
            n_jobs: Number of parallel threads
        """
        self.max_depth = max_depth
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.subsample = subsample
        self.colsample_bytree = colsample_bytree
        self.include_temporal = include_temporal
        self.include_spatial = include_spatial
        self.include_weather = include_weather
        self.handle_nan = handle_nan
        self.random_state = random_state
        self.n_jobs = n_jobs

        self.model: Optional[XGBRegressor] = None
        self.feature_cols: Optional[List[str]] = None
        self.available_features: Optional[List[str]] = None
        self.preprocessor: Optional[ColumnTransformer] = None
        self.categorical_features: List[str] = []
        self.numeric_features: List[str] = []

    def _get_feature_groups(self) -> Dict[str, List[str]]:
        """Define feature groups."""
        return {
            "core": ["distance_to_stop"],
            "spatial_numeric": (
                ["progress_on_segment", "progress_ratio"]
                if self.include_spatial
                else []
            ),
            "temporal": (
                ["hour", "day_of_week", "is_weekend", "is_peak_hour"]
                if self.include_temporal
                else []
            ),
            "weather": (
                ["temperature_c", "precipitation_mm", "wind_speed_kmh"]
                if self.include_weather
                else []
            ),
        }

    def _build_preprocessor(self) -> None:
        """Create a ColumnTransformer to handle numeric + categorical features."""
        if not self.feature_cols:
            raise ValueError("Feature columns must be set before building the preprocessor")

        transformers = []
        numeric_features = [
            col for col in self.feature_cols if col not in self.categorical_features
        ]
        self.numeric_features = numeric_features

        if numeric_features:
            transformers.append((
                "numeric",
                "passthrough",
                numeric_features,
            ))

        if self.categorical_features:
            transformers.append((
                "categorical",
                _make_one_hot_encoder(),
                self.categorical_features,
            ))

        self.preprocessor = ColumnTransformer(
            transformers,
            remainder="drop",
            sparse_threshold=0.0,
        )

    def _clean_data(
        self,
        df: pd.DataFrame,
        target_col: str = "time_to_arrival_seconds",
    ) -> pd.DataFrame:
        """
        Clean data and handle NaN values.
        
        Args:
            df: Input dataframe
            target_col: Target column name
            
        Returns:
            Cleaned dataframe
        """
        print(f"\n{'=' * 60}")
        print("Data Cleaning (XGBoost)".center(60))
        print(f"{'=' * 60}")
        print(f"Initial rows: {len(df):,}")

        # Get all potential features
        feature_groups = self._get_feature_groups()
        all_features: List[str] = []
        for group_features in feature_groups.values():
            all_features.extend(group_features)

        # Check which features are available and have acceptable NaN levels
        available: List[str] = []
        missing: List[str] = []
        high_nan: List[tuple[str, str]] = []

        for feat in all_features:
            if feat not in df.columns:
                missing.append(feat)
                continue

            nan_ratio = df[feat].isna().sum() / len(df)

            if nan_ratio > 0.3:  # More than 30% NaN
                high_nan.append((feat, f"{nan_ratio * 100:.1f}%"))
            else:
                available.append(feat)

        # Print feature availability
        if missing:
            print(f"\n⚠️  Missing features: {', '.join(missing)}")
        if high_nan:
            print("⚠️  High NaN features (>30%):")
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
        if self.handle_nan == "error":
            # Check for any NaN in available features + target
            check_cols = available + [target_col]
            nan_counts = df[check_cols].isna().sum()
            if nan_counts.sum() > 0:
                print("\nNaN values found:")
                for col, count in nan_counts[nan_counts > 0].items():
                    print(f"  {col}: {count}")
                raise ValueError("NaN values found and handle_nan='error'")
            df_clean = df

        elif self.handle_nan == "drop":
            # Drop rows with NaN in available features or target
            check_cols = available + [target_col]
            initial_len = len(df)
            df_clean = df.dropna(subset=check_cols)
            dropped = initial_len - len(df_clean)

            if dropped > 0:
                pct = (dropped / initial_len) * 100
                print(f"\n✓ Dropped {dropped:,} rows ({pct:.2f}%) with NaN values")

        elif self.handle_nan == "impute":
            # Impute NaN values
            df_clean = df.copy()
            imputed: List[str] = []

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
            raise ValueError(
                f"ERROR: {remaining_nan} NaN values remain after cleaning!"
            )

        print("✓ No NaN values in features or target")

        return df_clean

    def fit(
        self,
        train_df: pd.DataFrame,
        target_col: str = "time_to_arrival_seconds",
    ) -> None:
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

        group_order = [
            "core",
            "spatial_numeric",
            "spatial_categorical",
            "temporal",
            "weather",
        ]
        for group in group_order:
            for feat in feature_groups.get(group, []):
                if feat in self.available_features and feat not in self.feature_cols:
                    self.feature_cols.append(feat)

        if not self.feature_cols:
            raise ValueError("No features available after cleaning!")

        self.categorical_features = []
        self._build_preprocessor()

        print(f"\n{'=' * 60}")
        print("Model Training (XGBoost)".center(60))
        print(f"{'=' * 60}")
        print(f"Features ({len(self.feature_cols)}): {', '.join(self.feature_cols)}")

        # Prepare data
        X = train_clean[self.feature_cols]
        y = train_clean[target_col].values

        X_transformed = self.preprocessor.fit_transform(X)

        # Create and fit model
        self.model = XGBRegressor(
            max_depth=self.max_depth,
            n_estimators=self.n_estimators,
            learning_rate=self.learning_rate,
            subsample=self.subsample,
            colsample_bytree=self.colsample_bytree,
            objective="reg:squarederror",
            random_state=self.random_state,
            n_jobs=self.n_jobs,
        )
        self.model.fit(X_transformed, y)

        print(
            f"✓ XGBoost model trained "
            f"(max_depth={self.max_depth}, n_estimators={self.n_estimators}, "
            f"learning_rate={self.learning_rate})"
        )

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

        if self.feature_cols is None or self.preprocessor is None:
            raise ValueError("Feature/preprocessor not set")

        # Check for missing features
        missing = [f for f in self.feature_cols if f not in X.columns]
        if missing:
            raise ValueError(f"Missing features in input: {missing}")

        # Handle NaN in prediction data
        X_pred = X[self.feature_cols].copy()

        if self.handle_nan == "impute":
            for col in self.numeric_features:
                if X_pred[col].isna().any():
                    X_pred[col] = X_pred[col].fillna(X_pred[col].median())
            for col in self.categorical_features:
                if X_pred[col].isna().any():
                    mode_series = X_pred[col].mode()
                    filler = mode_series.iloc[0] if not mode_series.empty else "missing"
                    X_pred[col] = X_pred[col].fillna(filler)
        elif self.handle_nan == "drop":
            for col in self.numeric_features:
                if X_pred[col].isna().any():
                    X_pred[col] = X_pred[col].fillna(0.0)
            for col in self.categorical_features:
                if X_pred[col].isna().any():
                    X_pred[col] = X_pred[col].fillna("missing")

        X_transformed = self.preprocessor.transform(X_pred[self.feature_cols])
        predictions = self.model.predict(X_transformed)

        return clip_predictions(predictions)

    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importances as reported by XGBoost."""
        if self.model is None or self.preprocessor is None:
            return {}

        if not hasattr(self.model, "feature_importances_"):
            return {}

        importances = self.model.feature_importances_
        feature_names = self.preprocessor.get_feature_names_out()

        # Map back to feature names
        importance = {
            feat: float(imp)
            for feat, imp in zip(feature_names, importances)
        }

        # Sort by importance
        return dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))


def train_xgboost(
    dataset_name: str = "sample_dataset",
    route_id: Optional[str] = None,
    max_depth: int = 5,
    n_estimators: int = 200,
    learning_rate: float = 0.05,
    subsample: float = 0.8,
    colsample_bytree: float = 0.8,
    include_temporal: bool = True,
    include_spatial: bool = True,
    include_weather: bool = False,
    handle_nan: str = "drop",
    test_size: float = 0.2,
    save_model: bool = True,
    pre_split: Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]] = None,
) -> Dict:
    """
    Train and evaluate XGBoost time model.
    
    Args:
        dataset_name: Dataset name
        route_id: Optional route ID for route-specific training
        max_depth: Maximum tree depth
        n_estimators: Number of boosting rounds
        learning_rate: Learning rate (eta)
        subsample: Row subsample ratio per tree
        colsample_bytree: Column subsample ratio per tree
        include_temporal: Include temporal features
        include_spatial: Include spatial features
        include_weather: Include weather features
        handle_nan: 'drop', 'impute', or 'error'
        test_size: Test set fraction (test); val is fixed at 0.1
        save_model: Save to registry
        pre_split: Optional (train, val, test) DataFrames to reuse
        
    Returns:
        Dictionary with model, metrics, metadata
    """
    print(f"\n{'=' * 60}")
    print("XGBoost Time Model".center(60))
    print(f"{'=' * 60}\n")

    route_info = f" (route: {route_id})" if route_id else " (global)"
    print(f"Scope{route_info}")
    print("Config:")
    print(f"  max_depth={max_depth}, n_estimators={n_estimators}, learning_rate={learning_rate}")
    print(f"  subsample={subsample}, colsample_bytree={colsample_bytree}")
    print(f"  temporal={include_temporal}, spatial={include_spatial}")
    print(f"  weather={include_weather}, handle_nan='{handle_nan}'")

    # Load dataset
    print(f"\nLoading dataset: {dataset_name}")
    dataset = load_dataset(dataset_name)
    dataset.clean_data()

    # Filter by route if specified
    if route_id is not None:
        df = dataset.df
        df_filtered = df[df["route_id"] == route_id].copy()
        print(f"Filtered to route {route_id}: {len(df_filtered):,} samples")

        if len(df_filtered) == 0:
            raise ValueError(f"No data found for route {route_id}")

        dataset.df = df_filtered

    # Split data (train / val / test)
    if pre_split is not None:
        train_df, val_df, test_df = (df.copy() for df in pre_split)
    else:
        train_df, val_df, test_df = dataset.temporal_split(
            train_frac=1 - test_size - 0.1,
            val_frac=0.1,
        )

    train_test_summary(train_df, test_df, val_df)

    # Train model
    print("\n" + "=" * 60)
    print("Training".center(60))
    print("=" * 60)

    model = XGBTimeModel(
        max_depth=max_depth,
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        include_temporal=include_temporal,
        include_spatial=include_spatial,
        include_weather=include_weather,
        handle_nan=handle_nan,
    )
    model.fit(train_df)

    # Validation
    print(f"\n{'=' * 60}")
    print("Validation Performance".center(60))
    print(f"{'=' * 60}")
    y_val = val_df["time_to_arrival_seconds"].values
    val_preds = model.predict(val_df)
    val_metrics = compute_all_metrics(y_val, val_preds, prefix="val_")
    print_metrics_table(val_metrics, "Validation")

    # Test
    print(f"\n{'=' * 60}")
    print("Test Performance".center(60))
    print(f"{'=' * 60}")
    y_test = test_df["time_to_arrival_seconds"].values
    test_preds = model.predict(test_df)
    test_metrics = compute_all_metrics(y_test, test_preds, prefix="test_")
    print_metrics_table(test_metrics, "Test")

    # Feature importance
    importance = model.get_feature_importance()
    if importance:
        print(f"\nTop 5 Features by Importance:")
        for i, (feat, imp) in enumerate(list(importance.items())[:5], 1):
            print(f"  {i}. {feat}: {imp:.6f}")

    # Metadata
    metadata = {
        "model_type": "xgboost",
        "dataset": dataset_name,
        "route_id": route_id,
        "max_depth": max_depth,
        "n_estimators": n_estimators,
        "learning_rate": learning_rate,
        "subsample": subsample,
        "colsample_bytree": colsample_bytree,
        "include_temporal": include_temporal,
        "include_spatial": include_spatial,
        "include_weather": include_weather,
        "handle_nan": handle_nan,
        "n_features": len(model.feature_cols) if model.feature_cols else 0,
        "features": model.feature_cols,
        "n_samples": len(train_df) + len(val_df) + len(test_df),
        "n_trips": dataset.df["trip_id"].nunique() if route_id else None,
        "train_samples": len(train_df),
        "test_samples": len(test_df),
        "metrics": {**val_metrics, **test_metrics},
    }

    # Save
    if save_model:
        feature_groups = []
        if include_temporal:
            feature_groups.append("temporal")
        if include_spatial:
            feature_groups.append("spatial")
        if include_weather:
            feature_groups.append("weather")

        model_key = ModelKey.generate(
            model_type="xgboost",
            dataset_name=dataset_name,
            feature_groups=feature_groups,
            route_id=route_id,
            max_depth=max_depth,
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            handle_nan=handle_nan,
        )

        registry = get_registry()
        registry.save_model(model_key, model, metadata)
        metadata["model_key"] = model_key
        print(f"\n✓ Model saved: {model_key}")

    return {
        "model": model,
        "metrics": metadata["metrics"],
        "metadata": metadata,
    }


if __name__ == "__main__":
    # Example: basic global model
    _ = train_xgboost(
        dataset_name="sample_dataset",
        include_temporal=True,
        include_weather=False,
        handle_nan="drop",
    )
