"""
Train all models either globally or per-route for comparative analysis.
"""

import argparse
import sys
import time
from pathlib import Path
import pandas as pd
from typing import Tuple

sys.path.append(str(Path(__file__).parent))          # models/
sys.path.append(str(Path(__file__).parent.parent))   # eta_prediction/ (for feature_engineering)

from common.data import load_dataset
from historical_mean.train import train_historical_mean
from polyreg_distance.train import train_polyreg_distance
from polyreg_time.train import train_polyreg_time
from ewma.train import train_ewma
from xgb.train import train_xgboost
from feature_engineering import progress as ui


def _metric_str(result: dict) -> str:
    """Compact one-line metrics summary from a trainer result dict."""
    m = (result or {}).get('metrics', {})
    parts = []
    if m.get('test_mae_minutes') is not None:
        parts.append(f"MAE {m['test_mae_minutes']:.2f}m")
    if m.get('test_rmse_seconds') is not None:
        parts.append(f"RMSE {m['test_rmse_seconds']:.0f}s")
    if m.get('test_r2') is not None:
        parts.append(f"R² {m['test_r2']:.3f}")
    return " · ".join(parts) if parts else "done"

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
    
    ui.banner(
        "Train ETA models",
        [
            f"dataset  {dataset_name}",
            f"mode     {'route-specific' if by_route else 'global'}",
            f"models   {', '.join(model_types)}",
            f"save     {save}",
        ],
    )

    # Load dataset to get routes / global data
    with ui.step("Load + clean dataset") as s:
        dataset = load_dataset(dataset_name)
        dataset.clean_data()
        s.detail(f"{len(dataset.df):,} rows · {dataset.df['route_id'].nunique()} routes")
    
    if by_route:
        routes = sorted(dataset.df['route_id'].unique())

        # Get trip counts per route for summary
        route_stats = dataset.df.groupby('route_id').agg({
            'trip_id': 'nunique',
            'time_to_arrival_seconds': 'count'
        }).rename(columns={
            'trip_id': 'n_trips',
            'time_to_arrival_seconds': 'n_samples'
        })

        ui.heading(f"Route statistics · {len(routes)} routes")
        print(route_stats.to_string())

        # Train models for each route
        results = {}
        
        for route_id in routes:
            n_trips = route_stats.loc[route_id, 'n_trips']
            n_samples = route_stats.loc[route_id, 'n_samples']
            route_df = dataset.df[dataset.df['route_id'] == route_id].copy()
            if route_df.empty:
                ui.note(f"Route {route_id}: no samples after cleaning — skipped")
                continue
            route_splits = _temporal_split_df(
                route_df,
                DEFAULT_TRAIN_FRAC,
                DEFAULT_VAL_FRAC,
            )

            ui.heading(f"Route {route_id} · {n_trips} trips · {n_samples} samples")

            route_results = {}

            try:
                if 'historical_mean' in model_types:
                    with ui.step(f"Historical Mean · {route_id}", spinner=False) as s:
                        result = train_historical_mean(
                            dataset_name=dataset_name,
                            route_id=route_id,
                            save_model=save,
                            pre_split=_clone_splits(route_splits),
                        )
                        route_results['historical_mean'] = result
                        s.detail(_metric_str(result))

                if 'polyreg_distance' in model_types:
                    with ui.step(f"PolyReg Distance · {route_id}", spinner=False) as s:
                        result = train_polyreg_distance(
                            dataset_name=dataset_name,
                            route_id=route_id,
                            degree=2,
                            save_model=save,
                            pre_split=_clone_splits(route_splits),
                        )
                        route_results['polyreg_distance'] = result
                        s.detail(_metric_str(result))

                if 'polyreg_time' in model_types:
                    with ui.step(f"PolyReg Time · {route_id}", spinner=False) as s:
                        result = train_polyreg_time(
                            dataset_name=dataset_name,
                            route_id=route_id,
                            poly_degree=2,
                            include_temporal=True,
                            save_model=save,
                            pre_split=_clone_splits(route_splits),
                        )
                        route_results['polyreg_time'] = result
                        s.detail(_metric_str(result))

                if 'ewma' in model_types:
                    with ui.step(f"EWMA · {route_id}", spinner=False) as s:
                        result = train_ewma(
                            dataset_name=dataset_name,
                            route_id=route_id,
                            alpha=0.3,
                            save_model=save,
                            pre_split=_clone_splits(route_splits),
                        )
                        route_results['ewma'] = result
                        s.detail(_metric_str(result))

                # NEW: XGBoost route-specific
                if 'xgboost' in model_types:
                    with ui.step(f"XGBoost · {route_id}", spinner=False) as s:
                        result = train_xgboost(
                            dataset_name=dataset_name,
                            route_id=route_id,
                            save_model=save,
                            pre_split=_clone_splits(route_splits),
                        )
                        route_results['xgboost'] = result
                        s.detail(_metric_str(result))

                results[route_id] = route_results

            except Exception as e:
                ui.note(f"✗ Route {route_id} failed: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Print summary
        ui.heading("Training summary")

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
            
            print(summary_df.to_string(index=False))

            # Show correlation between training data size (trips) and performance
            if len(summary_df) > 2:
                ui.heading("Data size vs performance")

                for model_type in model_types:
                    model_data = summary_df[summary_df['model_type'] == model_type]
                    if len(model_data) > 1:
                        corr = model_data['n_trips'].corr(model_data['test_mae_min'])
                        print(f"{model_type:20s}: trips vs MAE correlation = {corr:+.3f}")

                # NEW: Correlation between number of observations and performance
                ui.heading("Observations vs performance")

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
        ui.heading("Global models (all routes)")

        results = {}
        global_splits = dataset.temporal_split(
            train_frac=DEFAULT_TRAIN_FRAC,
            val_frac=DEFAULT_VAL_FRAC,
        )

        try:
            if 'historical_mean' in model_types:
                with ui.step("Historical Mean · global", spinner=False) as s:
                    results['historical_mean'] = train_historical_mean(
                        dataset_name=dataset_name,
                        save_model=save,
                        pre_split=_clone_splits(global_splits),
                    )
                    s.detail(_metric_str(results['historical_mean']))

            if 'polyreg_distance' in model_types:
                with ui.step("PolyReg Distance · global", spinner=False) as s:
                    results['polyreg_distance'] = train_polyreg_distance(
                        dataset_name=dataset_name,
                        degree=2,
                        save_model=save,
                        pre_split=_clone_splits(global_splits),
                    )
                    s.detail(_metric_str(results['polyreg_distance']))

            if 'polyreg_time' in model_types:
                with ui.step("PolyReg Time · global", spinner=False) as s:
                    results['polyreg_time'] = train_polyreg_time(
                        dataset_name=dataset_name,
                        poly_degree=2,
                        include_temporal=True,
                        save_model=save,
                        pre_split=_clone_splits(global_splits),
                    )
                    s.detail(_metric_str(results['polyreg_time']))

            if 'ewma' in model_types:
                with ui.step("EWMA · global", spinner=False) as s:
                    results['ewma'] = train_ewma(
                        dataset_name=dataset_name,
                        alpha=0.3,
                        save_model=save,
                        pre_split=_clone_splits(global_splits),
                    )
                    s.detail(_metric_str(results['ewma']))

            # NEW: XGBoost global
            if 'xgboost' in model_types:
                with ui.step("XGBoost · global", spinner=False) as s:
                    results['xgboost'] = train_xgboost(
                        dataset_name=dataset_name,
                        route_id=None,  # global model
                        save_model=save,
                        pre_split=_clone_splits(global_splits),
                    )
                    s.detail(_metric_str(results['xgboost']))

        except Exception as e:
            ui.note(f"✗ Global training failed: {e}")
            import traceback
            traceback.print_exc()

        # Print summary
        ui.summary(
            "Training summary",
            [
                (
                    mt,
                    f"MAE {r['metrics']['test_mae_minutes']:.2f}m · "
                    f"RMSE {r['metrics']['test_rmse_seconds']:.0f}s · "
                    f"R² {r['metrics'].get('test_r2', 0):.3f}",
                )
                for mt, r in results.items()
            ],
        )

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
    
    ui.banner("✓ Training complete")


if __name__ == "__main__":
    main()
