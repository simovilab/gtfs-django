"""
ETA Prediction Models Package

Comprehensive modeling framework for transit ETA prediction.

Example usage:
    >>> from models import train_all_baselines, get_registry
    >>> models = train_all_baselines("sample_dataset")
    >>> registry = get_registry()
    >>> best_key = registry.get_best_model()
"""

__version__ = "1.0.0"
__author__ = "SIMOVI Team"

# Core utilities
from .common.data import load_dataset, ETADataset, prepare_features_target
from .common.registry import get_registry, ModelRegistry
from .common.keys import ModelKey, PredictionKey
from .common.metrics import (
    compute_all_metrics,
    mae_minutes,
    rmse_minutes,
    within_threshold
)
from .common.utils import (
    format_seconds,
    haversine_distance,
    clip_predictions,
    setup_logging
)

# Training functions
from .historical_mean.train import train_historical_mean, HistoricalMeanModel
from .polyreg_distance.train import train_polyreg_distance, PolyRegDistanceModel
from .polyreg_time.train import train_polyreg_time, PolyRegTimeModel
from .ewma.train import train_ewma, EWMAModel

# Prediction functions
from .historical_mean.predict import predict_eta as predict_historical_mean
from .polyreg_distance.predict import predict_eta as predict_polyreg_distance
from .polyreg_time.predict import predict_eta as predict_polyreg_time
from .ewma.predict import predict_eta as predict_ewma

# Evaluation
from .evaluation.leaderboard import ModelLeaderboard, quick_compare
from .evaluation.roll_validate import RollingValidator, quick_rolling_validate

# Main training pipeline
# from .train_all_models import train_all_baselines, train_advanced_configurations
from .train_all_models import train_all_models


__all__ = [
    # Core utilities
    'load_dataset',
    'ETADataset',
    'prepare_features_target',
    'get_registry',
    'ModelRegistry',
    'ModelKey',
    'PredictionKey',
    'compute_all_metrics',
    'mae_minutes',
    'rmse_minutes',
    'within_threshold',
    'format_seconds',
    'haversine_distance',
    'clip_predictions',
    'setup_logging',
    
    # Models
    'HistoricalMeanModel',
    'PolyRegDistanceModel',
    'PolyRegTimeModel',
    'EWMAModel',
    
    # Training
    'train_historical_mean',
    'train_polyreg_distance',
    'train_polyreg_time',
    'train_ewma',
    # 'train_all_baselines',
    'train_all_models'
    'train_advanced_configurations',
    
    # Prediction
    'predict_historical_mean',
    'predict_polyreg_distance',
    'predict_polyreg_time',
    'predict_ewma',
    
    # Evaluation
    'ModelLeaderboard',
    'quick_compare',
    'RollingValidator',
    'quick_rolling_validate',
]


# Package info
MODELS = {
    'historical_mean': {
        'description': 'Historical average baseline',
        'typical_mae': '2-4 minutes',
        'features': ['route_id', 'stop_sequence', 'temporal']
    },
    'polyreg_distance': {
        'description': 'Polynomial regression on distance',
        'typical_mae': '1.5-3 minutes',
        'features': ['distance_to_stop']
    },
    'polyreg_time': {
        'description': 'Polynomial regression with time features',
        'typical_mae': '1-2.5 minutes',
        'features': ['distance_to_stop', 'temporal', 'operational']
    },
    'ewma': {
        'description': 'Exponentially weighted moving average',
        'typical_mae': '1.5-3 minutes',
        'features': ['route_id', 'stop_sequence', 'temporal'],
        'online_learning': True
    }
}


def list_models():
    """List available model types."""
    print("\nAvailable Model Types:")
    print("=" * 70)
    for name, info in MODELS.items():
        print(f"\n{name}:")
        print(f"  Description: {info['description']}")
        print(f"  Typical MAE: {info['typical_mae']}")
        print(f"  Features: {', '.join(info['features'])}")
        if info.get('online_learning'):
            print(f"  Online Learning: Yes")
    print("\n" + "=" * 70)


def quick_start_guide():
    """Print quick start guide."""
    guide = """
    ETA Prediction Models - Quick Start
    ====================================
    
    1. Train all baselines:
       >>> from models import train_all_baselines
       >>> results = train_all_baselines("sample_dataset")
    
    2. Compare models:
       >>> from models import quick_compare, get_registry
       >>> registry = get_registry()
       >>> model_keys = registry.list_models()['model_key'].tolist()
       >>> comparison = quick_compare(model_keys)
    
    3. Load and use best model:
       >>> best_key = registry.get_best_model(metric='test_mae_seconds')
       >>> model = registry.load_model(best_key)
       >>> predictions = model.predict(your_data)
    
    4. Make single prediction:
       >>> from models import predict_polyreg_time
       >>> result = predict_polyreg_time(
       ...     model_key=best_key,
       ...     distance_to_stop=1500.0,
       ...     hour=8,
       ...     is_peak_hour=True
       ... )
       >>> print(f"ETA: {result['eta_formatted']}")
    
    For more details, see models/README.md
    """
    print(guide)


# Auto-create directories
def _setup_directories():
    """Create necessary directories if they don't exist."""
    from pathlib import Path
    base_dir = Path(__file__).parent
    
    dirs = [
        base_dir / 'trained',
        base_dir.parent / 'datasets',
        base_dir.parent / 'datasets' / 'metadata',
        base_dir.parent / 'datasets' / 'production',
        base_dir.parent / 'datasets' / 'experimental'
    ]
    
    for dir_path in dirs:
        dir_path.mkdir(parents=True, exist_ok=True)


# Run setup on import
_setup_directories()