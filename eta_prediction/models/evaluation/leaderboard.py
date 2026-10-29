"""
Model leaderboard for comparing performance across models.
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Dict
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from common.registry import get_registry
from common.data import load_dataset
from common.metrics import compute_all_metrics


class ModelLeaderboard:
    """
    Compare multiple models on standardized test sets.
    """
    
    def __init__(self):
        self.registry = get_registry()
        self.results = []
    
    def evaluate_model(self, 
                      model_key: str,
                      test_df: pd.DataFrame,
                      target_col: str = 'time_to_arrival_seconds') -> Dict:
        """
        Evaluate single model on test set.
        
        Args:
            model_key: Model identifier
            test_df: Test dataframe
            target_col: Target column name
            
        Returns:
            Dictionary with metrics
        """
        print(f"Evaluating {model_key}...")
        
        # Load model and metadata
        model = self.registry.load_model(model_key)
        metadata = self.registry.load_metadata(model_key)
        
        # Predict
        y_true = test_df[target_col].values
        y_pred = model.predict(test_df)
        
        # Compute metrics
        metrics = compute_all_metrics(y_true, y_pred)
        
        # Add model info
        result = {
            'model_key': model_key,
            'model_type': metadata.get('model_type', 'unknown'),
            'dataset': metadata.get('dataset', 'unknown'),
            **metrics
        }
        
        return result
    
    def compare_models(self,
                      model_keys: List[str],
                      dataset_name: str = "sample_dataset",
                      test_routes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Compare multiple models on same test set.
        
        Args:
            model_keys: List of model keys to compare
            dataset_name: Dataset to evaluate on
            test_routes: Specific routes for testing
            
        Returns:
            DataFrame with comparison results
        """
        print(f"\n{'='*60}")
        print(f"MODEL LEADERBOARD".center(60))
        print(f"{'='*60}\n")
        print(f"Dataset: {dataset_name}")
        print(f"Models: {len(model_keys)}\n")
        
        # Load dataset
        dataset = load_dataset(dataset_name)
        dataset.clean_data()
        
        # Get test set
        if test_routes:
            _, test_df = dataset.route_split(test_routes=test_routes)
        else:
            _, _, test_df = dataset.temporal_split(train_frac=0.7, val_frac=0.15)
        
        print(f"Test set: {len(test_df)} samples\n")
        
        # Evaluate each model
        results = []
        for model_key in model_keys:
            try:
                result = self.evaluate_model(model_key, test_df)
                results.append(result)
            except Exception as e:
                print(f"  Error evaluating {model_key}: {e}")
        
        # Create comparison dataframe
        df = pd.DataFrame(results)
        
        # Sort by MAE (primary metric)
        if 'mae_seconds' in df.columns:
            df = df.sort_values('mae_seconds')
        
        self.results = results
        
        return df
    
    def print_leaderboard(self,
                         df: pd.DataFrame,
                         metrics: List[str] = ['mae_minutes', 'rmse_minutes', 'r2', 
                                              'within_60s', 'bias_seconds']):
        """
        Pretty print leaderboard.
        
        Args:
            df: Results dataframe
            metrics: Metrics to display
        """
        print(f"\n{'='*80}")
        print(f"LEADERBOARD RESULTS".center(80))
        print(f"{'='*80}\n")
        
        # Select columns
        display_cols = ['model_type'] + [m for m in metrics if m in df.columns]
        display_df = df[display_cols].copy()
        
        # Format numbers
        for col in metrics:
            if col in display_df.columns:
                if col.startswith('within_'):
                    display_df[col] = (display_df[col] * 100).round(1).astype(str) + '%'
                else:
                    display_df[col] = display_df[col].round(3)
        
        # Add rank
        display_df.insert(0, 'rank', range(1, len(display_df) + 1))
        
        print(display_df.to_string(index=False))
        print(f"\n{'='*80}\n")
        
        # Highlight winner
        winner = df.iloc[0]
        print(f"🏆 Best Model: {winner['model_type']}")
        print(f"   MAE: {winner['mae_minutes']:.3f} minutes")
        print(f"   RMSE: {winner['rmse_minutes']:.3f} minutes")
        print(f"   R²: {winner['r2']:.3f}")
    
    def model_comparison_summary(self, df: pd.DataFrame) -> str:
        """
        Generate text summary of model comparison.
        
        Args:
            df: Results dataframe
            
        Returns:
            Summary string
        """
        best = df.iloc[0]
        worst = df.iloc[-1]
        
        improvement = (worst['mae_seconds'] - best['mae_seconds']) / worst['mae_seconds'] * 100
        
        summary = f"""
Model Comparison Summary
========================

Total Models Evaluated: {len(df)}
Test Samples: {df.iloc[0].get('test_samples', 'N/A')}

Best Model: {best['model_type']}
  - MAE: {best['mae_minutes']:.2f} minutes
  - RMSE: {best['rmse_minutes']:.2f} minutes
  - R²: {best['r2']:.3f}
  - Within 60s: {best['within_60s']*100:.1f}%

Baseline (Worst): {worst['model_type']}
  - MAE: {worst['mae_minutes']:.2f} minutes

Improvement: {improvement:.1f}% reduction in MAE from baseline to best model.
"""
        return summary


def quick_compare(model_keys: List[str], 
                 dataset_name: str = "sample_dataset") -> pd.DataFrame:
    """
    Quick comparison function.
    
    Args:
        model_keys: List of model keys
        dataset_name: Dataset name
        
    Returns:
        Comparison dataframe
    """
    leaderboard = ModelLeaderboard()
    df = leaderboard.compare_models(model_keys, dataset_name)
    leaderboard.print_leaderboard(df)
    
    return df


if __name__ == "__main__":
    # Example: Compare all model types
    
    # You would need to train these first
    model_keys = [
        "historical_mean_sample_dataset_temporal-route_20250126_143022",
        "polyreg_distance_sample_dataset_distance_20250126_143022_degree=2",
        "polyreg_time_sample_dataset_distance-operational-temporal_20250126_143022_degree=2",
        "ewma_sample_dataset_temporal-route_20250126_143022_alpha=0_3"
    ]
    
    # Run comparison
    results_df = quick_compare(model_keys)
    
    # Save results
    results_df.to_csv("models/leaderboard_results.csv", index=False)
    print("\nResults saved to models/leaderboard_results.csv")