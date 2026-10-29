"""
Model key generation and identification utilities.
Ensures consistent naming and versioning across the modeling pipeline.
"""

from typing import Dict, Optional
from datetime import datetime


class ModelKey:
    """
    Generates unique, descriptive keys for trained models.
    
    Format: {model_type}_{dataset}_{features}_{route_id}_{timestamp}
    Example: polyreg_distance_sample_temporal-position_route_1_20250126_143022
             polyreg_distance_sample_temporal-position_global_20250126_143022
    """
    
    @staticmethod
    def generate(model_type: str,
                 dataset_name: str,
                 feature_groups: list,
                 route_id: Optional[str] = None,
                 version: Optional[str] = None,
                 **kwargs) -> str:
        """
        Generate a unique model key.
        
        Args:
            model_type: Type of model (e.g., 'polyreg_distance', 'ewma')
            dataset_name: Name of training dataset
            feature_groups: List of feature group names used
            route_id: Optional route ID for route-specific models
            version: Optional version string, defaults to timestamp
            **kwargs: Additional metadata to include in key
            
        Returns:
            Unique model key string
        """
        # Create features string
        features_str = '-'.join(sorted(feature_groups))
        
        # Create version string
        if version is None:
            version = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Base key parts
        key_parts = [
            model_type,
            dataset_name,
            features_str
        ]
        
        # Add route scope (route-specific or global)
        if route_id is not None:
            key_parts.append(f"route_{route_id}")
        else:
            key_parts.append("global")
        
        # Add version
        key_parts.append(version)
        
        # Add optional kwargs
        for k, v in sorted(kwargs.items()):
            if v is not None:
                key_parts.append(f"{k}={v}")
        
        return '_'.join(key_parts)
    
    @staticmethod
    def parse(key: str) -> Dict[str, str]:
        """
        Parse a model key back into components.
        
        Args:
            key: Model key string
            
        Returns:
            Dictionary with parsed components
        """
        parts = key.split('_')
        
        if len(parts) < 5:
            raise ValueError(f"Invalid model key format: {key}")
        
        parsed = {
            'model_type': parts[0],
            'dataset': parts[1],
            'features': parts[2],
        }
        
        # Parse route scope
        if parts[3] == 'route' and len(parts) >= 5:
            # Route-specific model: ..._route_1_20250126_143022
            parsed['route_id'] = parts[4]
            parsed['scope'] = 'route'
            parsed['version'] = '_'.join(parts[5:7]) if len(parts) >= 7 else parts[5]
            extra_start = 7
        elif parts[3] == 'global':
            # Global model: ..._global_20250126_143022
            parsed['route_id'] = None
            parsed['scope'] = 'global'
            parsed['version'] = '_'.join(parts[4:6]) if len(parts) >= 6 else parts[4]
            extra_start = 6
        else:
            # Legacy format without scope
            parsed['route_id'] = None
            parsed['scope'] = 'global'
            parsed['version'] = '_'.join(parts[3:5]) if len(parts) >= 5 else parts[3]
            extra_start = 5
        
        # Parse additional kwargs (key=value format)
        if len(parts) > extra_start:
            for part in parts[extra_start:]:
                if '=' in part:
                    k, v = part.split('=', 1)
                    parsed[k] = v
        
        return parsed
    
    @staticmethod
    def is_route_specific(key: str) -> bool:
        """
        Check if a model key is route-specific.
        
        Args:
            key: Model key string
            
        Returns:
            True if route-specific, False if global
        """
        try:
            parsed = ModelKey.parse(key)
            return parsed.get('scope') == 'route' and parsed.get('route_id') is not None
        except (ValueError, IndexError):
            return False
    
    @staticmethod
    def extract_route_id(key: str) -> Optional[str]:
        """
        Extract route_id from a model key.
        
        Args:
            key: Model key string
            
        Returns:
            Route ID string or None if global model
        """
        try:
            parsed = ModelKey.parse(key)
            return parsed.get('route_id')
        except (ValueError, IndexError):
            return None


class PredictionKey:
    """
    Generates keys for prediction requests to enable caching and deduplication.
    
    Format: {route_id}_{stop_id}_{vp_hash}
    """
    
    @staticmethod
    def generate(route_id: str,
                 stop_id: str,
                 vehicle_lat: float,
                 vehicle_lon: float,
                 timestamp: datetime,
                 stop_sequence: Optional[int] = None) -> str:
        """
        Generate prediction key for a vehicle-stop pair.
        
        Args:
            route_id: Route identifier
            stop_id: Stop identifier
            vehicle_lat: Vehicle latitude
            vehicle_lon: Vehicle longitude
            timestamp: Timestamp of vehicle position
            stop_sequence: Optional stop sequence number
            
        Returns:
            Prediction key string
        """
        # Round coordinates to ~10m precision for caching
        lat_rounded = round(vehicle_lat, 4)
        lon_rounded = round(vehicle_lon, 4)
        
        # Create position hash
        vp_hash = f"{lat_rounded},{lon_rounded}"
        
        # Build key
        if stop_sequence is not None:
            return f"{route_id}_{stop_id}_{stop_sequence}_{vp_hash}"
        else:
            return f"{route_id}_{stop_id}_{vp_hash}"


class ExperimentKey:
    """
    Generates keys for experiments and model comparisons.
    """
    
    @staticmethod
    def generate(experiment_name: str,
                 models: list,
                 dataset: str,
                 timestamp: Optional[str] = None) -> str:
        """
        Generate experiment key.
        
        Args:
            experiment_name: Name of experiment
            models: List of model types being compared
            dataset: Dataset name
            timestamp: Optional timestamp, defaults to now
            
        Returns:
            Experiment key string
        """
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        models_str = '-'.join(sorted(models))
        
        return f"exp_{experiment_name}_{models_str}_{dataset}_{timestamp}"


def model_filename(model_key: str, extension: str = "pkl") -> str:
    """
    Generate consistent filename for model artifacts.
    
    Args:
        model_key: Model key from ModelKey.generate()
        extension: File extension (pkl, joblib, json, etc.)
        
    Returns:
        Filename string
    """
    return f"{model_key}.{extension}"


def validate_model_key(key: str) -> bool:
    """
    Validate that a string is a properly formatted model key.
    
    Args:
        key: String to validate
        
    Returns:
        True if valid, False otherwise
    """
    try:
        parsed = ModelKey.parse(key)
        required = ['model_type', 'dataset', 'features', 'version']
        return all(k in parsed for k in required)
    except (ValueError, IndexError):
        return False