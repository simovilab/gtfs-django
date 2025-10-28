"""
Main training script for all ETA prediction models.
Trains multiple model types and compares performance.
Updated with robust NaN handling.
"""

import argparse
import sys
from pathlib import Path

# Add models directory to path
sys.path.append(str(Path(__file__).parent))

from historical_mean.train import train_historical_mean
from polyreg_distance.train import train_polyreg_distance
from polyreg_time.train import train_polyreg_time
from ewma.train import train_ewma
from evaluation.leaderboard import quick_compare
from common.registry import get_registry


def train_all_baselines(dataset_name: str = "sample_dataset",
                        save_models: bool = True,
                        handle_nan: str = 'drop'):
    """
    Train all baseline models with robust NaN handling.
    
    Args:
        dataset_name: Dataset to train on
        save_models: Whether to save models to registry
        handle_nan: How to handle NaN - 'drop' or 'impute'
        
    Returns:
        Dictionary of trained models and their keys
    """
    print("\n" + "="*80)
    print("TRAINING ALL BASELINE MODELS".center(80))
    print("="*80 + "\n")
    print(f"NaN Handling Strategy: {handle_nan}")
    
    models = {}
    
    # 1. Historical Mean (simplest baseline)
    print("\n[1/4] Historical Mean Model")
    print("-" * 80)
    try:
        result = train_historical_mean(
            dataset_name=dataset_name,
            group_by=['route_id', 'stop_sequence', 'hour'],
            save_model=save_models
        )
        models['historical_mean'] = result
        print("✓ Historical Mean trained successfully")
    except Exception as e:
        print(f"✗ Historical Mean failed: {e}")
    
    # 2. Polynomial Regression - Distance Only
    print("\n[2/4] Polynomial Regression Distance Model")
    print("-" * 80)
    try:
        result = train_polyreg_distance(
            dataset_name=dataset_name,
            degree=2,
            alpha=1.0,
            route_specific=False,
            save_model=save_models
        )
        models['polyreg_distance'] = result
        print("✓ Polynomial Regression Distance trained successfully")
        if save_models and 'model_key' in result.get('metadata', {}):
            print(f"   Saved as: {result['metadata']['model_key']}")
    except Exception as e:
        print(f"✗ Polynomial Regression Distance failed: {e}")
        import traceback
        traceback.print_exc()
    
    # 3. Polynomial Regression - Time Enhanced
    print("\n[3/4] Polynomial Regression Time Model")
    print("-" * 80)
    try:
        result = train_polyreg_time(
            dataset_name=dataset_name,
            poly_degree=2,
            alpha=1.0,
            include_temporal=True,
            include_operational=True,
            include_weather=False,
            handle_nan=handle_nan,  # Use configurable NaN handling
            save_model=save_models
        )
        models['polyreg_time'] = result
        print("✓ Polynomial Regression Time trained successfully")
    except Exception as e:
        print(f"✗ Polynomial Regression Time failed: {e}")
        import traceback
        traceback.print_exc()
    
    # 4. EWMA (adaptive baseline)
    print("\n[4/4] EWMA Model")
    print("-" * 80)
    try:
        result = train_ewma(
            dataset_name=dataset_name,
            alpha=0.3,
            group_by=['route_id', 'stop_sequence'],
            save_model=save_models
        )
        models['ewma'] = result
        print("✓ EWMA trained successfully")
    except Exception as e:
        print(f"✗ EWMA failed: {e}")
    
    # Summary
    print("\n" + "="*80)
    print(f"Training Summary: {len(models)}/4 models trained successfully")
    print("="*80)
    
    return models


def compare_all_models(models: dict):
    """
    Compare all trained models.
    
    Args:
        models: Dictionary of model results
    """
    if not models:
        print("\n⚠️  No models to compare")
        return None
    
    print("\n" + "="*80)
    print("MODEL COMPARISON".center(80))
    print("="*80 + "\n")
    
    # Extract metrics
    comparison = []
    for model_type, result in models.items():
        metrics = result['metrics']
        comparison.append({
            'model': model_type,
            'test_mae_min': metrics.get('test_mae_minutes', 0),
            'test_rmse_min': metrics.get('test_rmse_minutes', 0),
            'test_r2': metrics.get('test_r2', 0),
            'test_within_60s': metrics.get('test_within_60s', 0) * 100,
            'test_bias_sec': metrics.get('test_bias_seconds', 0)
        })
    
    import pandas as pd
    df = pd.DataFrame(comparison)
    df = df.sort_values('test_mae_min')
    
    print("\nRanking by Test MAE:")
    print("-" * 80)
    print(df.to_string(index=False))
    
    # Highlight best
    best = df.iloc[0]
    print(f"\n🏆 Winner: {best['model']}")
    print(f"   MAE: {best['test_mae_min']:.3f} minutes")
    print(f"   RMSE: {best['test_rmse_min']:.3f} minutes")
    print(f"   R²: {best['test_r2']:.3f}")
    print(f"   Within 60s: {best['test_within_60s']:.1f}%")
    
    # Calculate improvements
    if len(df) > 1:
        worst = df.iloc[-1]
        improvement = (worst['test_mae_min'] - best['test_mae_min']) / worst['test_mae_min'] * 100
        print(f"\n📈 Improvement from baseline: {improvement:.1f}% reduction in MAE")
    
    return df


def train_advanced_configurations(dataset_name: str = "sample_dataset",
                                  handle_nan: str = 'drop'):
    """
    Train advanced model configurations.
    
    Args:
        dataset_name: Dataset to train on
        handle_nan: NaN handling strategy
        
    Returns:
        Dictionary of results
    """
    print("\n" + "="*80)
    print("TRAINING ADVANCED CONFIGURATIONS".center(80))
    print("="*80 + "\n")
    
    advanced = {}
    
    # Route-specific polynomial regression
    print("[1/4] Route-Specific Polynomial Regression")
    print("-" * 80)
    try:
        result = train_polyreg_distance(
            dataset_name=dataset_name,
            degree=2,
            route_specific=True,
            save_model=True
        )
        advanced['polyreg_route_specific'] = result
        print("✓ Route-specific model trained successfully")
    except Exception as e:
        print(f"✗ Route-specific model failed: {e}")
    
    # Higher degree polynomial
    print("\n[2/4] Degree-3 Polynomial Regression")
    print("-" * 80)
    try:
        result = train_polyreg_distance(
            dataset_name=dataset_name,
            degree=3,
            route_specific=False,
            save_model=True
        )
        advanced['polyreg_degree3'] = result
        print("✓ Degree-3 model trained successfully")
    except Exception as e:
        print(f"✗ Degree-3 model failed: {e}")
    
    # Polynomial with weather (if available)
    print("\n[3/4] Polynomial Regression with Weather")
    print("-" * 80)
    try:
        result = train_polyreg_time(
            dataset_name=dataset_name,
            poly_degree=2,
            include_temporal=True,
            include_operational=True,
            include_weather=True,
            handle_nan=handle_nan,
            save_model=True
        )
        advanced['polyreg_with_weather'] = result
        print("✓ Weather-enhanced model trained successfully")
    except Exception as e:
        print(f"✗ Weather-enhanced model failed: {e}")
        print("   (Weather features may not be available in dataset)")
    
    # EWMA with hourly grouping
    print("\n[4/4] EWMA with Hourly Grouping")
    print("-" * 80)
    try:
        result = train_ewma(
            dataset_name=dataset_name,
            alpha=0.3,
            group_by=['route_id', 'stop_sequence', 'hour'],
            save_model=True
        )
        advanced['ewma_hourly'] = result
        print("✓ Hourly EWMA trained successfully")
    except Exception as e:
        print(f"✗ Hourly EWMA failed: {e}")
    
    print("\n" + "="*80)
    print(f"Advanced Training Summary: {len(advanced)}/4 models trained")
    print("="*80)
    
    return advanced


def main():
    """Main training pipeline."""
    parser = argparse.ArgumentParser(
        description="Train ETA prediction models with robust NaN handling"
    )
    parser.add_argument(
        '--dataset',
        type=str,
        default='sample_dataset',
        help='Dataset name (without .parquet extension)'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['baseline', 'advanced', 'all'],
        default='baseline',
        help='Training mode: baseline, advanced, or all'
    )
    parser.add_argument(
        '--handle-nan',
        type=str,
        choices=['drop', 'impute'],
        default='drop',
        help='How to handle NaN values: drop rows or impute'
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save models to registry'
    )
    parser.add_argument(
        '--compare-only',
        action='store_true',
        help='Only compare existing models in registry'
    )
    
    args = parser.parse_args()
    
    if args.compare_only:
        # Compare existing models
        registry = get_registry()
        models_df = registry.list_models()
        
        if models_df.empty:
            print("No models found in registry. Train some models first.")
            return
        
        print("\nModels in Registry:")
        print(models_df)
        
        # Get model keys
        model_keys = models_df['model_key'].tolist()
        
        # Run comparison
        quick_compare(model_keys, args.dataset)
        
        return
    
    save_models = not args.no_save
    
    # Train models based on mode
    if args.mode == 'baseline':
        models = train_all_baselines(
            args.dataset, 
            save_models, 
            handle_nan=args.handle_nan
        )
        compare_all_models(models)
        
    elif args.mode == 'advanced':
        models = train_advanced_configurations(
            args.dataset,
            handle_nan=args.handle_nan
        )
        compare_all_models(models)
        
    elif args.mode == 'all':
        print("\n" + "="*80)
        print("COMPREHENSIVE MODEL TRAINING".center(80))
        print("="*80)
        
        baseline_models = train_all_baselines(
            args.dataset, 
            save_models,
            handle_nan=args.handle_nan
        )
        advanced_models = train_advanced_configurations(
            args.dataset,
            handle_nan=args.handle_nan
        )
        
        all_models = {**baseline_models, **advanced_models}
        compare_all_models(all_models)
    
    # Print registry summary
    print("\n" + "="*80)
    print("MODEL REGISTRY SUMMARY".center(80))
    print("="*80 + "\n")
    
    registry = get_registry()
    models_df = registry.list_models()
    print(f"Total models in registry: {len(models_df)}")
    
    if not models_df.empty:
        print(f"\nRecent models:")
        print(models_df.head(10))
        print(f"\nModels saved to: {registry.base_dir}")
    
    print("\n✅ Training complete!")


if __name__ == "__main__":
    main()