"""
Train all models either globally or per-route for comparative analysis.
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
from typing import Tuple

sys.path.append(str(Path(__file__).parent))

from common.data import load_dataset
from historical_mean.train import train_historical_mean
from polyreg_distance.train import train_polyreg_distance
from polyreg_time.train import train_polyreg_time
from ewma.train import train_ewma
from xgb.train import train_xgboost

DEFAULT_TEST_SIZE = 0.2
DEFAULT_VAL_FRAC = 0.1
DEFAULT_TRAIN_FRAC = 1.0 - DEFAULT_TEST_SIZE - DEFAULT_VAL_FRAC


def _temporal_split_df(df: pd.DataFrame,
                       train_frac: float,
                       val_frac: float) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Deterministically split a dataframe by vp_ts."""
    df_sorted = df.sort_values('vp_ts').reset_index(drop=True)
    n = len(df_sorted)
    train_end = int(n * train_frac)
    val_end = int(n * (train_frac + val_frac))
    train_df = df_sorted.iloc[:train_end].copy()
    val_df = df_sorted.iloc[train_end:val_end].copy()
    test_df = df_sorted.iloc[val_end:].copy()
    return train_df, val_df, test_df


def _clone_splits(splits: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return deep copies of precomputed splits so callers can mutate freely."""
    return tuple(split.copy() for split in splits)


def train_all_models(dataset_name: str,
                    by_route: bool = False,
                    model_types: list = None,
                    save: bool = True):
    """
    Train all baseline models, optionally per-route.
    
    Args:
        dataset_name: Dataset to train on
        by_route: If True, train separate model for each route
        model_types: List of model types to train (None = all)
        save: Whether to save models to registry
    """
    
    if model_types is None:
        model_types = [
            'historical_mean',
            'polyreg_distance',
            'polyreg_time',
            'ewma',
            'xgboost',   # NEW
        ]
    
    print(f"\n{'='*80}")
    print(f"TRAINING ALL MODELS".center(80))
    print(f"{'='*80}")
    print(f"Dataset: {dataset_name}")
    print(f"Mode: {'Route-Specific' if by_route else 'Global'}")
    print(f"Models: {', '.join(model_types)}")
    print(f"Save: {save}")
    print(f"{'='*80}\n")
    
    # Load dataset to get routes / global data
    dataset = load_dataset(dataset_name)
    dataset.clean_data()
    
    if by_route:
        routes = sorted(dataset.df['route_id'].unique())
        print(f"Found {len(routes)} routes: {routes}\n")
        
        # Get trip counts per route for summary
        route_stats = dataset.df.groupby('route_id').agg({
            'trip_id': 'nunique',
            'time_to_arrival_seconds': 'count'
        }).rename(columns={
            'trip_id': 'n_trips',
            'time_to_arrival_seconds': 'n_samples'
        })
        
        print("Route Statistics:")
        print(route_stats.to_string())
        print()
        
        # Train models for each route
        results = {}
        
        for route_id in routes:
            n_trips = route_stats.loc[route_id, 'n_trips']
            n_samples = route_stats.loc[route_id, 'n_samples']
            route_df = dataset.df[dataset.df['route_id'] == route_id].copy()
            if route_df.empty:
                print(f"\nSkipping route {route_id}: no samples after cleaning")
                continue
            route_splits = _temporal_split_df(
                route_df,
                DEFAULT_TRAIN_FRAC,
                DEFAULT_VAL_FRAC,
            )
            
            print(f"\n{'#'*80}")
            print(f"ROUTE {route_id} ({n_trips} trips, {n_samples} samples)".center(80))
            print(f"{'#'*80}\n")
            
            route_results = {}
            
            try:
                if 'historical_mean' in model_types:
                    print(f"\n>>> Training Historical Mean for route {route_id}...")
                    result = train_historical_mean(
                        dataset_name=dataset_name,
                        route_id=route_id,
                        save_model=save,
                        pre_split=_clone_splits(route_splits),
                    )
                    route_results['historical_mean'] = result
                    
                if 'polyreg_distance' in model_types:
                    print(f"\n>>> Training PolyReg Distance for route {route_id}...")
                    result = train_polyreg_distance(
                        dataset_name=dataset_name,
                        route_id=route_id,
                        degree=2,
                        save_model=save,
                        pre_split=_clone_splits(route_splits),
                    )
                    route_results['polyreg_distance'] = result
                    
                if 'polyreg_time' in model_types:
                    print(f"\n>>> Training PolyReg Time for route {route_id}...")
                    result = train_polyreg_time(
                        dataset_name=dataset_name,
                        route_id=route_id,
                        poly_degree=2,
                        include_temporal=True,
                        save_model=save,
                        pre_split=_clone_splits(route_splits),
                    )
                    route_results['polyreg_time'] = result
                    
                if 'ewma' in model_types:
                    print(f"\n>>> Training EWMA for route {route_id}...")
                    result = train_ewma(
                        dataset_name=dataset_name,
                        route_id=route_id,
                        alpha=0.3,
                        save_model=save,
                        pre_split=_clone_splits(route_splits),
                    )
                    route_results['ewma'] = result

                # NEW: XGBoost route-specific
                if 'xgboost' in model_types:
                    print(f"\n>>> Training XGBoost for route {route_id}...")
                    result = train_xgboost(
                        dataset_name=dataset_name,
                        route_id=route_id,
                        save_model=save,
                        pre_split=_clone_splits(route_splits),
                    )
                    route_results['xgboost'] = result
                
                results[route_id] = route_results
                
            except Exception as e:
                print(f"\n❌ ERROR training models for route {route_id}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Print summary
        print(f"\n{'='*80}")
        print("TRAINING SUMMARY".center(80))
        print(f"{'='*80}\n")
        
        summary_data = []
        for route_id in routes:
            if route_id not in results:
                continue
            
            n_trips = route_stats.loc[route_id, 'n_trips']
            n_samples = route_stats.loc[route_id, 'n_samples']
            
            for model_type in model_types:
                if model_type in results[route_id]:
                    metrics = results[route_id][model_type]['metrics']
                    summary_data.append({
                        'route_id': route_id,
                        'n_trips': n_trips,
                        'n_samples': n_samples,  # NEW: number of observations
                        'model_type': model_type,
                        'test_mae_min': metrics.get('test_mae_minutes', None),
                        'test_rmse_sec': metrics.get('test_rmse_seconds', None),
                        'test_r2': metrics.get('test_r2', None)
                    })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df = summary_df.sort_values(
                ['n_trips', 'model_type'],
                ascending=[False, True]
            )
            
            print("Performance by Route and Model:")
            print(summary_df.to_string(index=False))
            
            # Show correlation between training data size (trips) and performance
            if len(summary_df) > 2:
                print(f"\n{'='*80}")
                print("Data Size vs Performance Analysis".center(80))
                print(f"{'='*80}\n")
                
                for model_type in model_types:
                    model_data = summary_df[summary_df['model_type'] == model_type]
                    if len(model_data) > 1:
                        corr = model_data['n_trips'].corr(model_data['test_mae_min'])
                        print(f"{model_type:20s}: trips vs MAE correlation = {corr:+.3f}")

                # NEW: Correlation between number of observations and performance
                print(f"\n{'='*80}")
                print("Observations vs Performance Analysis".center(80))
                print(f"{'='*80}\n")

                for model_type in model_types:
                    model_data = summary_df[summary_df['model_type'] == model_type]
                    if len(model_data) > 1:
                        corr_mae = model_data['n_samples'].corr(model_data['test_mae_min'])
                        corr_rmse = model_data['n_samples'].corr(model_data['test_rmse_sec'])
                        print(
                            f"{model_type:20s}: "
                            f"obs vs MAE  = {corr_mae:+.3f}, "
                            f"obs vs RMSE = {corr_rmse:+.3f}"
                        )
        
        return results
    
    else:
        # Train global models
        print("Training GLOBAL models across all routes...\n")
        
        results = {}
        global_splits = dataset.temporal_split(
            train_frac=DEFAULT_TRAIN_FRAC,
            val_frac=DEFAULT_VAL_FRAC,
        )
        
        try:
            if 'historical_mean' in model_types:
                print("\n>>> Training Historical Mean (global)...")
                results['historical_mean'] = train_historical_mean(
                    dataset_name=dataset_name,
                    save_model=save,
                    pre_split=_clone_splits(global_splits),
                )
                
            if 'polyreg_distance' in model_types:
                print("\n>>> Training PolyReg Distance (global)...")
                results['polyreg_distance'] = train_polyreg_distance(
                    dataset_name=dataset_name,
                    degree=2,
                    save_model=save,
                    pre_split=_clone_splits(global_splits),
                )
                
            if 'polyreg_time' in model_types:
                print("\n>>> Training PolyReg Time (global)...")
                results['polyreg_time'] = train_polyreg_time(
                    dataset_name=dataset_name,
                    poly_degree=2,
                    include_temporal=True,
                    save_model=save,
                    pre_split=_clone_splits(global_splits),
                )
                
            if 'ewma' in model_types:
                print("\n>>> Training EWMA (global)...")
                results['ewma'] = train_ewma(
                    dataset_name=dataset_name,
                    alpha=0.3,
                    save_model=save,
                    pre_split=_clone_splits(global_splits),
                )

            # NEW: XGBoost global
            if 'xgboost' in model_types:
                print("\n>>> Training XGBoost (global)...")
                results['xgboost'] = train_xgboost(
                    dataset_name=dataset_name,
                    route_id=None,  # global model
                    save_model=save,
                    pre_split=_clone_splits(global_splits),
                )
        
        except Exception as e:
            print(f"\n❌ ERROR training global models: {e}")
            import traceback
            traceback.print_exc()
        
        # Print summary
        print(f"\n{'='*80}")
        print("TRAINING SUMMARY".center(80))
        print(f"{'='*80}\n")
        
        for model_type, result in results.items():
            metrics = result['metrics']
            print(f"{model_type:20s}: MAE = {metrics['test_mae_minutes']:.3f} min, "
                  f"RMSE = {metrics['test_rmse_seconds']:.1f} sec, "
                  f"R² = {metrics.get('test_r2', 0):.3f}")
        
        return results


def main():
    parser = argparse.ArgumentParser(
        description='Train ETA prediction models'
    )
    
    parser.add_argument(
        '--dataset',
        type=str,
        default='sample_dataset',
        help='Dataset name (default: sample_dataset)'
    )
    
    parser.add_argument(
        '--by-route',
        action='store_true',
        help='Train separate models for each route'
    )
    
    parser.add_argument(
        '--models',
        type=str,
        nargs='+',
        choices=[
            'historical_mean',
            'polyreg_distance',
            'polyreg_time',
            'ewma',
            'xgboost',   # NEW
            'all'
        ],
        default=['all'],
        help='Models to train (default: all)'
    )
    
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save models to registry (dry run)'
    )
    
    args = parser.parse_args()
    
    # Parse model types
    if 'all' in args.models:
        model_types = [
            'historical_mean',
            'polyreg_distance',
            'polyreg_time',
            'ewma',
            'xgboost',   # NEW
        ]
    else:
        model_types = args.models
    
    # Train models
    results = train_all_models(
        dataset_name=args.dataset,
        by_route=args.by_route,
        model_types=model_types,
        save=not args.no_save
    )
    
    print(f"\n{'='*80}")
    print("✓ TRAINING COMPLETE".center(80))
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
