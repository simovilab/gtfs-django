"""
Model key generation and identification utilities.
Ensures consistent naming and versioning across the modeling pipeline.
"""

from typing import Dict, Optional
from datetime import datetime


class ModelKey:
    """
    Generates unique, descriptive keys for trained models.
    
    Format: {model_type}_{dataset}_{features}_{timestamp}
    Example: polyreg_distance_sample_temporal-position_20250126_143022
    """
    
    @staticmethod
    def generate(model_type: str,
                 dataset_name: str,
                 feature_groups: list,
                 version: Optional[str] = None,
                 **kwargs) -> str:
        """
        Generate a unique model key.
        
        Args:
            model_type: Type of model (e.g., 'polyreg_distance', 'ewma')
            dataset_name: Name of training dataset
            feature_groups: List of feature group names used
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
        
        # Base key
        key_parts = [
            model_type,
            dataset_name,
            features_str,
            version
        ]
        
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
        
        if len(parts) < 4:
            raise ValueError(f"Invalid model key format: {key}")
        
        parsed = {
            'model_type': parts[0],
            'dataset': parts[1],
            'features': parts[2],
            'version': '_'.join(parts[3:5]) if len(parts) >= 5 else parts[3]
        }
        
        # Parse additional kwargs (key=value format)
        if len(parts) > 5:
            for part in parts[5:]:
                if '=' in part:
                    k, v = part.split('=', 1)
                    parsed[k] = v
        
        return parsed


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