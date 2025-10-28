"""
Model registry for managing trained models and their metadata.
Provides save/load functionality with consistent structure.
"""

import json
import pickle
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import pandas as pd


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
    
    def __init__(self, base_dir: str = "models/trained"):
        """
        Initialize registry.
        
        Args:
            base_dir: Base directory for model storage
        """
        self.base_dir = Path(base_dir)
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
        metadata['model_path'] = str(model_path)
        
        # Save metadata
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # Update registry
        self.registry[model_key] = {
            'model_path': str(model_path),
            'meta_path': str(meta_path),
            'saved_at': metadata['saved_at'],
            'model_type': metadata.get('model_type', 'unknown')
        }
        self._save_registry()
        
        print(f"✓ Saved model: {model_key}")
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
        
        with open(meta_path, 'r') as f:
            metadata = json.load(f)
        
        return metadata
    
    def list_models(self, 
                    model_type: Optional[str] = None,
                    sort_by: str = 'saved_at') -> pd.DataFrame:
        """
        List all models in registry.
        
        Args:
            model_type: Filter by model type (e.g., 'polyreg_distance')
            sort_by: Column to sort by
            
        Returns:
            DataFrame with model information
        """
        models = []
        
        for key, info in self.registry.items():
            if model_type and info.get('model_type') != model_type:
                continue
            
            # Load metadata for richer info
            try:
                meta = self.load_metadata(key)
                models.append({
                    'model_key': key,
                    'model_type': info.get('model_type', 'unknown'),
                    'saved_at': info['saved_at'],
                    'dataset': meta.get('dataset', 'unknown'),
                    'mae_seconds': meta.get('metrics', {}).get('test_mae_seconds', None),
                    'rmse_seconds': meta.get('metrics', {}).get('test_rmse_seconds', None),
                    'r2': meta.get('metrics', {}).get('test_r2', None),
                })
            except Exception as e:
                print(f"Warning: Could not load metadata for {key}: {e}")
                models.append({
                    'model_key': key,
                    'model_type': info.get('model_type', 'unknown'),
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
                       metric: str = 'test_mae_seconds',
                       minimize: bool = True) -> str:
        """
        Get best model by metric.
        
        Args:
            model_type: Filter by model type
            metric: Metric to optimize
            minimize: Whether to minimize (True) or maximize (False) metric
            
        Returns:
            Model key of best model
        """
        candidates = []
        
        for key in self.registry.keys():
            if model_type and self.registry[key].get('model_type') != model_type:
                continue
            
            try:
                meta = self.load_metadata(key)
                metric_value = meta.get('metrics', {}).get(metric)
                
                if metric_value is not None:
                    candidates.append((key, metric_value))
            except Exception:
                continue
        
        if not candidates:
            raise ValueError(f"No models found with metric {metric}")
        
        # Sort and return best
        candidates.sort(key=lambda x: x[1], reverse=not minimize)
        return candidates[0][0]


# Global registry instance
_registry = None

def get_registry() -> ModelRegistry:
    """Get or create global registry instance."""
    global _registry
    if _registry is None:
        _registry = ModelRegistry()
    return _registry