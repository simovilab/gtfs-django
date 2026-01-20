"""
Diagnostic script to check model registry and troubleshoot saving issues.
"""

import sys
from pathlib import Path
import json
import os

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from models.common.registry import get_registry


def check_registry_status():
    """Check registry status and list all models."""
    print("="*80)
    print("MODEL REGISTRY DIAGNOSTICS".center(80))
    print("="*80 + "\n")
    
    # Get registry
    registry = get_registry()
    
    # Check directory exists
    print(f"Registry Location: {registry.base_dir}")
    print(f"Directory exists: {os.path.exists(registry.base_dir)}")
    print(f"Is writable: {os.access(registry.base_dir, os.W_OK)}")
    
    # List all files
    if os.path.exists(registry.base_dir):
        all_files = list(Path(registry.base_dir).rglob('*'))
        print(f"\nTotal files in registry: {len(all_files)}")
        
        # Count by type
        pkl_files = [f for f in all_files if f.suffix == '.pkl']
        json_files = [f for f in all_files if f.suffix == '.json']
        
        print(f"  - .pkl files: {len(pkl_files)}")
        print(f"  - .json files: {len(json_files)}")
        
        # Show directory structure
        print("\nDirectory structure:")
        for root, dirs, files in os.walk(registry.base_dir):
            level = root.replace(str(registry.base_dir), '').count(os.sep)
            indent = ' ' * 2 * level
            print(f'{indent}{os.path.basename(root)}/')
            subindent = ' ' * 2 * (level + 1)
            for file in files[:10]:  # Show first 10 files per dir
                size_kb = os.path.getsize(os.path.join(root, file)) / 1024
                print(f'{subindent}{file} ({size_kb:.1f} KB)')
            if len(files) > 10:
                print(f'{subindent}... and {len(files) - 10} more files')
    
    # Get model list from registry
    print("\n" + "="*80)
    print("MODELS IN REGISTRY")
    print("="*80 + "\n")
    
    models_df = registry.list_models()
    
    if models_df.empty:
        print("❌ No models found in registry!")
        print("\nPossible issues:")
        print("1. Models not being saved (check save_model=True)")
        print("2. Registry directory path incorrect")
        print("3. Permission issues writing files")
        print("4. Errors during model saving (check training logs)")
    else:
        print(f"✅ Found {len(models_df)} models\n")
        print(models_df.to_string())
        
        # Show details of most recent model
        print("\n" + "="*80)
        print("MOST RECENT MODEL DETAILS")
        print("="*80 + "\n")
        
        latest = models_df.iloc[0]
        model_key = latest['model_key']
        
        print(f"Model Key: {model_key}")
        print(f"Type: {latest['model_type']}")
        print(f"Saved: {latest['saved_at']}")
        print(f"Dataset: {latest['dataset']}")
        
        # Try to load metadata
        try:
            metadata_path = Path(registry.base_dir) / f"{model_key}_metadata.json"
            if metadata_path.exists():
                with open(metadata_path) as f:
                    metadata = json.load(f)
                print("\nMetadata:")
                print(json.dumps(metadata, indent=2))
        except Exception as e:
            print(f"\n⚠️  Could not load metadata: {e}")
        
        # Check model file
        model_path = Path(registry.base_dir) / f"{model_key}.pkl"
        if model_path.exists():
            size_mb = model_path.stat().st_size / (1024 * 1024)
            print(f"\nModel file: {model_path.name} ({size_mb:.2f} MB)")
        else:
            print(f"\n⚠️  Model file not found: {model_path}")


def test_save_load():
    """Test saving and loading a simple model."""
    print("\n" + "="*80)
    print("SAVE/LOAD TEST")
    print("="*80 + "\n")
    
    registry = get_registry()
    
    # Create a dummy model
    test_model = {"type": "test", "value": 42}
    test_metadata = {
        "test": True,
        "model_type": "diagnostic_test",
        "dataset": "test_dataset",
        "metrics": {"mae": 1.0}
    }
    test_key = "test_model_diagnostic_123"
    
    # Try to save
    print("Attempting to save test model...")
    try:
        registry.save_model(test_key, test_model, test_metadata)
        print("✅ Save successful!")
        
        # Try to load
        print("\nAttempting to load test model...")
        loaded_model, loaded_metadata = registry.load_model(test_key)
        
        if loaded_model == test_model:
            print("✅ Load successful! Model matches.")
        else:
            print("⚠️  Load successful but model doesn't match.")
            print(f"  Expected: {test_model}")
            print(f"  Got: {loaded_model}")
        
        # Clean up
        print("\nCleaning up test files...")
        model_path = Path(registry.base_dir) / f"{test_key}.pkl"
        metadata_path = Path(registry.base_dir) / f"{test_key}_metadata.json"
        
        if model_path.exists():
            model_path.unlink()
        if metadata_path.exists():
            metadata_path.unlink()
        
        print("✅ Test complete!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


def check_model_type_distribution():
    """Show distribution of model types in registry."""
    registry = get_registry()
    models_df = registry.list_models()
    
    if models_df.empty:
        return
    
    print("\n" + "="*80)
    print("MODEL TYPE DISTRIBUTION")
    print("="*80 + "\n")
    
    type_counts = models_df['model_type'].value_counts()
    
    for model_type, count in type_counts.items():
        print(f"{model_type:30s} {count:3d} models")
    
    print(f"\n{'Total':30s} {len(models_df):3d} models")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Check model registry status")
    parser.add_argument('--test', action='store_true', help='Run save/load test')
    
    args = parser.parse_args()
    
    # Always show status
    check_registry_status()
    
    # Show distribution if models exist
    check_model_type_distribution()
    
    # Optional test
    if args.test:
        test_save_load()
    
    print("\n" + "="*80)
    print("DIAGNOSTICS COMPLETE".center(80))
    print("="*80)