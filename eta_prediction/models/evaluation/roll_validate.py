"""
Rolling window (walk-forward) validation for time series models.
Evaluates model performance over time with realistic train/test splits.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Callable, Optional
from datetime import timedelta
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from common.data import load_dataset
from common.metrics import compute_all_metrics
from common.utils import print_metrics_table


class RollingValidator:
    """
    Perform rolling window validation on time series data.
    """
    
    def __init__(self,
                 train_window_days: int = 7,
                 test_window_days: int = 1,
                 step_days: int = 1):
        """
        Initialize validator.
        
        Args:
            train_window_days: Size of training window in days
            test_window_days: Size of test window in days
            step_days: Step size between windows
        """
        self.train_window = timedelta(days=train_window_days)
        self.test_window = timedelta(days=test_window_days)
        self.step = timedelta(days=step_days)
        
        self.results = []
    
    def validate(self,
                dataset_name: str,
                train_fn: Callable,
                predict_fn: Callable,
                target_col: str = 'time_to_arrival_seconds') -> pd.DataFrame:
        """
        Perform rolling window validation.
        
        Args:
            dataset_name: Dataset to validate on
            train_fn: Function(train_df) -> model
            predict_fn: Function(model, test_df) -> predictions
            target_col: Target column name
            
        Returns:
            DataFrame with results per window
        """
        print(f"\n{'='*60}")
        print(f"ROLLING WINDOW VALIDATION".center(60))
        print(f"{'='*60}\n")
        print(f"Train window: {self.train_window.days} days")
        print(f"Test window: {self.test_window.days} days")
        print(f"Step size: {self.step.days} days\n")
        
        # Load dataset
        dataset = load_dataset(dataset_name)
        dataset.clean_data()
        
        df = dataset.df.sort_values('vp_ts').reset_index(drop=True)
        
        # Get date range
        start_date = df['vp_ts'].min()
        end_date = df['vp_ts'].max()
        
        print(f"Data range: {start_date} to {end_date}")
        print(f"Total duration: {(end_date - start_date).days} days\n")
        
        # Generate windows
        current_start = start_date
        window_num = 1
        results = []
        
        while current_start + self.train_window + self.test_window <= end_date:
            train_end = current_start + self.train_window
            test_start = train_end
            test_end = test_start + self.test_window
            
            print(f"Window {window_num}:")
            print(f"  Train: {current_start.date()} to {train_end.date()}")
            print(f"  Test:  {test_start.date()} to {test_end.date()}")
            
            # Split data
            train_df = df[(df['vp_ts'] >= current_start) & (df['vp_ts'] < train_end)]
            test_df = df[(df['vp_ts'] >= test_start) & (df['vp_ts'] < test_end)]
            
            print(f"  Train samples: {len(train_df)}, Test samples: {len(test_df)}")
            
            if len(train_df) == 0 or len(test_df) == 0:
                print(f"  Skipping (insufficient data)\n")
                current_start += self.step
                window_num += 1
                continue
            
            try:
                # Train model
                model = train_fn(train_df)
                
                # Predict
                y_true = test_df[target_col].values
                y_pred = predict_fn(model, test_df)
                
                # Compute metrics
                metrics = compute_all_metrics(y_true, y_pred)
                
                # Store results
                result = {
                    'window': window_num,
                    'train_start': current_start,
                    'train_end': train_end,
                    'test_start': test_start,
                    'test_end': test_end,
                    'train_samples': len(train_df),
                    'test_samples': len(test_df),
                    **metrics
                }
                results.append(result)
                
                print(f"  MAE: {metrics['mae_minutes']:.2f} min, RMSE: {metrics['rmse_minutes']:.2f} min")
                print()
                
            except Exception as e:
                print(f"  Error: {e}\n")
            
            # Move to next window
            current_start += self.step
            window_num += 1
        
        # Create results dataframe
        results_df = pd.DataFrame(results)
        
        if not results_df.empty:
            self._print_summary(results_df)
        
        self.results = results_df
        return results_df
    
    def _print_summary(self, results_df: pd.DataFrame):
        """Print summary statistics."""
        print(f"\n{'='*60}")
        print(f"VALIDATION SUMMARY".center(60))
        print(f"{'='*60}\n")
        
        print(f"Total windows: {len(results_df)}")
        print(f"\nAverage Metrics:")
        print(f"  MAE: {results_df['mae_minutes'].mean():.3f} ± {results_df['mae_minutes'].std():.3f} minutes")
        print(f"  RMSE: {results_df['rmse_minutes'].mean():.3f} ± {results_df['rmse_minutes'].std():.3f} minutes")
        print(f"  R²: {results_df['r2'].mean():.3f} ± {results_df['r2'].std():.3f}")
        print(f"  Within 60s: {results_df['within_60s'].mean()*100:.1f}%")
        
        print(f"\nBest Window: {results_df.loc[results_df['mae_minutes'].idxmin(), 'window']}")
        print(f"  MAE: {results_df['mae_minutes'].min():.3f} minutes")
        
        print(f"\nWorst Window: {results_df.loc[results_df['mae_minutes'].idxmax(), 'window']}")
        print(f"  MAE: {results_df['mae_minutes'].max():.3f} minutes")
        
        print(f"\n{'='*60}\n")
    
    def plot_results(self, results_df: Optional[pd.DataFrame] = None, 
                    metric: str = 'mae_minutes'):
        """
        Plot metric over time (requires matplotlib).
        
        Args:
            results_df: Results dataframe (uses self.results if None)
            metric: Metric to plot
        """
        if results_df is None:
            results_df = self.results
        
        if results_df.empty:
            print("No results to plot")
            return
        
        try:
            import matplotlib.pyplot as plt
            
            fig, ax = plt.subplots(figsize=(12, 6))
            
            ax.plot(results_df['window'], results_df[metric], marker='o')
            ax.set_xlabel('Window Number')
            ax.set_ylabel(metric.replace('_', ' ').title())
            ax.set_title(f'Rolling Window Validation: {metric}')
            ax.grid(True, alpha=0.3)
            
            # Add mean line
            mean_val = results_df[metric].mean()
            ax.axhline(mean_val, color='r', linestyle='--', 
                      label=f'Mean: {mean_val:.3f}')
            ax.legend()
            
            plt.tight_layout()
            plt.savefig(f'rolling_validation_{metric}.png', dpi=150)
            print(f"Plot saved to rolling_validation_{metric}.png")
            
        except ImportError:
            print("matplotlib not available for plotting")


def quick_rolling_validate(model_class,
                           model_params: Dict,
                           dataset_name: str = "sample_dataset",
                           train_window_days: int = 7) -> pd.DataFrame:
    """
    Quick rolling validation for a model class.
    
    Args:
        model_class: Model class to instantiate
        model_params: Parameters for model initialization
        dataset_name: Dataset name
        train_window_days: Training window size
        
    Returns:
        Results dataframe
    """
    def train_fn(train_df):
        model = model_class(**model_params)
        model.fit(train_df)
        return model
    
    def predict_fn(model, test_df):
        return model.predict(test_df)
    
    validator = RollingValidator(
        train_window_days=train_window_days,
        test_window_days=1,
        step_days=1
    )
    
    results_df = validator.validate(dataset_name, train_fn, predict_fn)
    
    return results_df


if __name__ == "__main__":
    # Example: Rolling validation for EWMA model
    import sys
    sys.path.append(str(Path(__file__).parent.parent))
    from ewma.train import EWMAModel
    
    results = quick_rolling_validate(
        model_class=EWMAModel,
        model_params={'alpha': 0.3, 'group_by': ['route_id', 'stop_sequence']},
        train_window_days=7
    )
    
    # Save results
    results.to_csv("models/rolling_validation_results.csv", index=False)
    print("Results saved to models/rolling_validation_results.csv")