"""
Model registry for managing trained models and their metadata.
Provides save/load functionality with consistent structure.
"""

import json
import os
import pickle
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
import pandas as pd


def _discover_project_root() -> Path:
    """Try to locate the repository root by walking up for pyproject or .git."""
    path = Path(__file__).resolve()
    for parent in [path.parent, *path.parents]:
        if (parent / "pyproject.toml").exists() or (parent / ".git").exists():
            return parent
    # Fallback to historical assumption (two levels up)
    return path.parents[2]


PROJECT_ROOT = _discover_project_root()
DEFAULT_REGISTRY_DIR = PROJECT_ROOT / "models" / "trained"


def _find_existing_registry_dir() -> Path:
    """
    Search upwards from the current working directory for an existing registry.
    This allows running tools from nested folders (e.g., bytewax/) while still
    reusing the canonical models/trained directory at the project root.
    """
    cwd = Path.cwd().resolve()
    search_roots = [cwd] + list(cwd.parents)
    for root in search_roots:
        candidate = root / "models" / "trained"
        if (candidate / "registry.json").exists():
            return candidate.resolve()
    return DEFAULT_REGISTRY_DIR.resolve()


class ModelRegistry:
    """
    Manages model artifacts and metadata in a structured directory.
    
    Structure:
        models/
        ├── trained/
        │   ├── {model_key}.pkl        # Serialized model
        │   └── {model_key}_meta.json  # Model metadata
        └── registry.json              # Index of all models
    """
    
    def __init__(self, base_dir: Union[str, Path, None] = None):
        """
        Initialize registry.
        
        Args:
            base_dir: Base directory for model storage
        """
        env_dir = os.getenv("MODEL_REGISTRY_DIR")
        configured_dir = base_dir if base_dir is not None else env_dir

        if configured_dir is not None:
            base_path = Path(configured_dir).expanduser()
            if not base_path.is_absolute():
                base_path = (PROJECT_ROOT / base_path).resolve()
            else:
                base_path = base_path.resolve()
        else:
            base_path = _find_existing_registry_dir()

        self.base_dir = base_path
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        self.registry_file = self.base_dir / "registry.json"
        self._load_registry()
    
    def _load_registry(self):
        """Load registry index from disk."""
        if self.registry_file.exists():
            with open(self.registry_file, 'r') as f:
                self.registry = json.load(f)
        else:
            self.registry = {}
    
    def _save_registry(self):
        """Save registry index to disk."""
        with open(self.registry_file, 'w') as f:
            json.dump(self.registry, f, indent=2)
    
    def save_model(self,
                   model_key: str,
                   model: Any,
                   metadata: Dict[str, Any],
                   overwrite: bool = False) -> Path:
        """
        Save model and metadata to registry.
        
        Args:
            model_key: Unique model identifier
            model: Trained model object (must be picklable)
            metadata: Model metadata (training metrics, config, etc.)
            overwrite: Whether to overwrite existing model
            
        Returns:
            Path to saved model file
        """
        model_path = self.base_dir / f"{model_key}.pkl"
        meta_path = self.base_dir / f"{model_key}_meta.json"
        
        if model_path.exists() and not overwrite:
            raise FileExistsError(f"Model {model_key} already exists. Set overwrite=True to replace.")
        
        # Save model artifact
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        
        # Enrich metadata
        metadata['model_key'] = model_key
        metadata['saved_at'] = datetime.now().isoformat()
        # Store registry paths relative to repo root for portability
        relative_model_path = os.path.relpath(model_path, PROJECT_ROOT)
        relative_meta_path = os.path.relpath(meta_path, PROJECT_ROOT)
        metadata['model_path'] = relative_model_path
        
        # Save metadata
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # Update registry
        self.registry[model_key] = {
            'model_path': relative_model_path,
            'meta_path': relative_meta_path,
            'saved_at': metadata['saved_at'],
            'model_type': metadata.get('model_type', 'unknown'),
            'route_id': metadata.get('route_id'),  # Track route scope
            'dataset': metadata.get('dataset', 'unknown')
        }
        self._save_registry()
        
        route_info = f" (route: {metadata.get('route_id')})" if metadata.get('route_id') else " (global)"
        print(f"✓ Saved model: {model_key}{route_info}")
        return model_path
    
    def load_model(self, model_key: str) -> Any:
        """
        Load model from registry.
        
        Args:
            model_key: Unique model identifier
            
        Returns:
            Loaded model object
        """
        if model_key not in self.registry:
            raise KeyError(f"Model {model_key} not found in registry")
        
        model_path = Path(self.registry[model_key]['model_path'])
        if not model_path.is_absolute():
            model_path = (PROJECT_ROOT / model_path).resolve()
        
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        
        return model
    
    def load_metadata(self, model_key: str) -> Dict[str, Any]:
        """
        Load model metadata.
        
        Args:
            model_key: Unique model identifier
            
        Returns:
            Metadata dictionary
        """
        if model_key not in self.registry:
            raise KeyError(f"Model {model_key} not found in registry")
        
        meta_path = Path(self.registry[model_key]['meta_path'])
        if not meta_path.is_absolute():
            meta_path = (PROJECT_ROOT / meta_path).resolve()
        
        with open(meta_path, 'r') as f:
            metadata = json.load(f)
        
        return metadata
    
    def list_models(self, 
                    model_type: Optional[str] = None,
                    route_id: Optional[str] = None,
                    sort_by: str = 'saved_at') -> pd.DataFrame:
        """
        List all models in registry.
        
        Args:
            model_type: Filter by model type (e.g., 'polyreg_distance')
            route_id: Filter by route_id (None for global models, 'all' for all models)
            sort_by: Column to sort by
            
        Returns:
            DataFrame with model information
        """
        models = []
        
        for key, info in self.registry.items():
            # Filter by model type
            if model_type and info.get('model_type') != model_type:
                continue
            
            # Filter by route_id
            model_route_id = info.get('route_id')
            if route_id is not None and route_id != 'all':
                if route_id == 'global' and model_route_id is not None:
                    continue
                elif route_id != 'global' and model_route_id != route_id:
                    continue
            
            # Load metadata for richer info
            try:
                meta = self.load_metadata(key)
                models.append({
                    'model_key': key,
                    'model_type': info.get('model_type', 'unknown'),
                    'route_id': model_route_id or 'global',
                    'saved_at': info['saved_at'],
                    'dataset': meta.get('dataset', 'unknown'),
                    'n_samples': meta.get('n_samples'),
                    'mae_seconds': meta.get('metrics', {}).get('test_mae_seconds', None),
                    'mae_minutes': meta.get('metrics', {}).get('test_mae_minutes', None),
                    'rmse_seconds': meta.get('metrics', {}).get('test_rmse_seconds', None),
                    'r2': meta.get('metrics', {}).get('test_r2', None),
                })
            except Exception as e:
                print(f"Warning: Could not load metadata for {key}: {e}")
                models.append({
                    'model_key': key,
                    'model_type': info.get('model_type', 'unknown'),
                    'route_id': model_route_id or 'global',
                    'saved_at': info['saved_at'],
                })
        
        df = pd.DataFrame(models)
        if not df.empty and sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=False)
        
        return df
    
    def delete_model(self, model_key: str) -> bool:
        """
        Delete model and metadata from registry.
        
        Args:
            model_key: Unique model identifier
            
        Returns:
            True if deleted successfully
        """
        if model_key not in self.registry:
            raise KeyError(f"Model {model_key} not found in registry")
        
        # Delete files
        model_path = Path(self.registry[model_key]['model_path'])
        meta_path = Path(self.registry[model_key]['meta_path'])
        
        if model_path.exists():
            model_path.unlink()
        if meta_path.exists():
            meta_path.unlink()
        
        # Remove from registry
        del self.registry[model_key]
        self._save_registry()
        
        print(f"✓ Deleted model: {model_key}")
        return True
    
    def get_best_model(self, 
                       model_type: Optional[str] = None,
                       route_id: Optional[str] = None,
                       metric: str = 'test_mae_seconds',
                       minimize: bool = True) -> Optional[str]:
        """
        Get best model by metric.
        
        Args:
            model_type: Filter by model type
            route_id: Filter by route_id (None = prefer route-specific if exists, else global)
            metric: Metric to optimize (e.g., 'test_mae_seconds', 'test_rmse_seconds')
            minimize: Whether to minimize (True) or maximize (False) metric
            
        Returns:
            Model key of best model, or None if no models found
        """
        candidates = []
        
        for key in self.registry.keys():
            # Filter by model type
            if model_type and self.registry[key].get('model_type') != model_type:
                continue
            
            # Filter by route
            model_route_id = self.registry[key].get('route_id')
            
            if route_id is not None:
                # Explicit route requested
                if route_id == 'global' and model_route_id is not None:
                    continue
                elif route_id != 'global' and model_route_id != route_id:
                    continue
            # If route_id is None, we'll prefer route-specific in the sorting logic
            
            try:
                meta = self.load_metadata(key)
                metric_value = meta.get('metrics', {}).get(metric)
                
                if metric_value is not None:
                    candidates.append({
                        'key': key,
                        'metric_value': metric_value,
                        'route_id': model_route_id,
                        'is_route_specific': model_route_id is not None
                    })
            except Exception:
                continue
        
        if not candidates:
            return None
        
        # Sort by metric, with preference for route-specific when route_id is None
        if route_id is None:
            # Smart routing: prefer route-specific models if they exist
            route_specific = [c for c in candidates if c['is_route_specific']]
            global_models = [c for c in candidates if not c['is_route_specific']]
            
            # If route-specific models exist, use them; otherwise fall back to global
            candidates_to_sort = route_specific if route_specific else global_models
        else:
            candidates_to_sort = candidates
        
        # Sort by metric value
        candidates_to_sort.sort(key=lambda x: x['metric_value'], reverse=not minimize)
        
        return candidates_to_sort[0]['key'] if candidates_to_sort else None
    
    def get_routes(self, model_type: Optional[str] = None) -> List[str]:
        """
        Get list of all routes that have trained models.
        
        Args:
            model_type: Filter by model type
            
        Returns:
            List of route IDs (excludes None/global)
        """
        routes = set()
        
        for key, info in self.registry.items():
            if model_type and info.get('model_type') != model_type:
                continue
            
            route_id = info.get('route_id')
            if route_id is not None:
                routes.add(route_id)
        
        return sorted(list(routes))
    
    def compare_routes(self, 
                       model_type: str,
                       metric: str = 'test_mae_minutes') -> pd.DataFrame:
        """
        Compare model performance across routes.
        
        Args:
            model_type: Model type to compare
            metric: Metric to display
            
        Returns:
            DataFrame with route comparison
        """
        results = []
        
        routes = self.get_routes(model_type)
        
        # Add global model if exists
        global_key = self.get_best_model(model_type=model_type, route_id='global', metric=f'test_{metric}')
        if global_key:
            meta = self.load_metadata(global_key)
            results.append({
                'route_id': 'global',
                'n_samples': meta.get('n_samples'),
                'n_trips': meta.get('n_trips'),
                metric: meta.get('metrics', {}).get(f'test_{metric}'),
                'model_key': global_key
            })
        
        # Add route-specific models
        for route_id in routes:
            best_key = self.get_best_model(model_type=model_type, route_id=route_id, metric=f'test_{metric}')
            if best_key:
                meta = self.load_metadata(best_key)
                results.append({
                    'route_id': route_id,
                    'n_samples': meta.get('n_samples'),
                    'n_trips': meta.get('n_trips'),
                    metric: meta.get('metrics', {}).get(f'test_{metric}'),
                    'model_key': best_key
                })
        
        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values('n_trips', ascending=False)
        
        return df


# Global registry instance
_registry = None

def get_registry() -> ModelRegistry:
    """Get or create global registry instance."""
    global _registry
    if _registry is None:
        _registry = ModelRegistry()
    return _registry
