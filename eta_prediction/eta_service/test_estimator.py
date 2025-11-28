"""
Enhanced test script for estimate_stop_times() with route-specific model support
Run from project root: python eta_service/test_estimator.py
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
import json

# Setup paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from eta_service.estimator import estimate_stop_times
from models.common.registry import get_registry


def get_sample_vp_data(route='1'):
    """Mock vehicle position in MQTT/Redis format."""
    return {
        'vehicle_id': f'vehicle_{route}_42',
        'route': route,
        'lat': 9.9281,
        'lon': -84.0907,
        'speed': 10.5,  # m/s
        'heading': 90,
        'timestamp': datetime.now(timezone.utc).isoformat()
    }


def get_sample_stops():
    """Mock upcoming stops."""
    return [
        {'stop_id': 'stop_001', 'stop_sequence': 5, 'total_stop_sequence': 20, 'lat': 9.9281, 'lon': -84.0907},
        {'stop_id': 'stop_002', 'stop_sequence': 6, 'total_stop_sequence': 20, 'lat': 9.9306, 'lon': -84.0882},
        {'stop_id': 'stop_003', 'stop_sequence': 7, 'total_stop_sequence': 20, 'lat': 9.9331, 'lon': -84.0857},
        {'stop_id': 'stop_004', 'stop_sequence': 8, 'total_stop_sequence': 20, 'lat': 9.9356, 'lon': -84.0832},
    ]


def test_basic_prediction():
    """Test with mock data using best model."""
    
    print("=" * 70)
    print("TEST 1: Basic Prediction with Auto-Selected Model")
    print("=" * 70)
    
    vp_data = get_sample_vp_data()
    stops = get_sample_stops()
    
    print(f"\n[VEHICLE POSITION]")
    print(f"  Vehicle: {vp_data['vehicle_id']}")
    print(f"  Route: {vp_data['route']}")
    print(f"  Location: ({vp_data['lat']}, {vp_data['lon']})")
    print(f"  Speed: {vp_data['speed']} m/s ({vp_data['speed'] * 3.6:.1f} km/h)")
    
    # Get predictions
    result = estimate_stop_times(
        vehicle_position=vp_data,
        upcoming_stops=stops,
        route_id='1',
        trip_id='trip_001',
        prefer_route_model=True,
        max_stops=3
    )
    
    # Display results
    print(f"\n[PREDICTIONS]")
    print(f"  Model Type: {result.get('model_type', 'N/A')}")
    print(f"  Model Scope: {result.get('model_scope', 'N/A')}")
    print(f"  Model Key: {result['model_key']}")
    print(f"  Computed at: {result['computed_at']}")
    
    if result.get('error'):
        print(f"\n  ERROR: {result['error']}")
        return False
    
    print(f"\n  Upcoming Stops ({len(result['predictions'])}):")
    print("  " + "-" * 66)
    
    for i, pred in enumerate(result['predictions'], 1):
        if pred.get('error'):
            print(f"\n  {i}. Stop {pred['stop_id']}: ERROR - {pred['error']}")
        else:
            print(f"\n  {i}. Stop {pred['stop_id']} (sequence {pred['stop_sequence']})")
            print(f"     Distance: {pred['distance_to_stop_m']:,.1f} m")
            print(f"     ETA: {pred['eta_formatted']} ({pred['eta_seconds']:.0f}s)")
            print(f"     Arrival: {pred['eta_timestamp']}")
    
    print("\n" + "=" * 70)
    return True


def test_route_specific_models():
    """Test route-specific vs global model selection."""
    
    print("\n\nTEST 2: Route-Specific vs Global Model Selection")
    print("=" * 70)
    
    registry = get_registry()
    
    # Get available routes with trained models
    routes = registry.get_routes()
    print(f"\nFound {len(routes)} routes with trained models: {routes}")
    
    # Also check for global models
    global_models = [k for k, info in registry.registry.items() 
                     if info.get('route_id') is None]
    print(f"Found {len(global_models)} global models")
    
    vp_data = get_sample_vp_data()
    stops = get_sample_stops()[:2]  # Just 2 stops for speed
    
    test_routes = routes[:3] if len(routes) >= 3 else routes  # Test up to 3 routes
    
    print(f"\n{'-' * 70}")
    print("Testing route-specific model preference:")
    print(f"{'-' * 70}")
    
    for route_id in test_routes:
        print(f"\nRoute {route_id}:")
        
        # Test 1: With prefer_route_model=True
        result_route = estimate_stop_times(
            vehicle_position=get_sample_vp_data(route_id),
            upcoming_stops=stops,
            route_id=route_id,
            prefer_route_model=True,
            max_stops=2
        )
        
        # Test 2: With prefer_route_model=False (force global)
        result_global = estimate_stop_times(
            vehicle_position=get_sample_vp_data(route_id),
            upcoming_stops=stops,
            route_id=route_id,
            prefer_route_model=False,
            max_stops=2
        )
        
        print(f"  Route-specific: {result_route.get('model_scope', 'N/A')}")
        if result_route.get('predictions') and not result_route['predictions'][0].get('error'):
            print(f"    ETA: {result_route['predictions'][0]['eta_formatted']}")
        
        print(f"  Global forced:  {result_global.get('model_scope', 'N/A')}")
        if result_global.get('predictions') and not result_global['predictions'][0].get('error'):
            print(f"    ETA: {result_global['predictions'][0]['eta_formatted']}")
        
        # Compare if both succeeded
        if (result_route.get('predictions') and result_global.get('predictions') and
            not result_route['predictions'][0].get('error') and 
            not result_global['predictions'][0].get('error')):
            
            eta_route = result_route['predictions'][0]['eta_seconds']
            eta_global = result_global['predictions'][0]['eta_seconds']
            diff = eta_route - eta_global
            diff_pct = (diff / eta_global * 100) if eta_global > 0 else 0
            
            print(f"  Difference: {diff:+.1f}s ({diff_pct:+.1f}%)")
    
    print("\n" + "=" * 70)
    return True


def test_all_model_types():
    """Test with each model type explicitly."""
    
    print("\n\nTEST 3: Testing All Model Types")
    print("=" * 70)
    
    registry = get_registry()
    df = registry.list_models()
    
    if df.empty:
        print("No models found in registry!")
        return False
    
    # Group by model type
    model_types = df['model_type'].unique()
    
    print(f"\nFound {len(model_types)} model types:")
    for mtype in model_types:
        count = len(df[df['model_type'] == mtype])
        route_count = len(df[(df['model_type'] == mtype) & (df['route_id'] != 'global')])
        global_count = len(df[(df['model_type'] == mtype) & (df['route_id'] == 'global')])
        print(f"  - {mtype}: {count} models ({route_count} route-specific, {global_count} global)")
    
    vp_data = get_sample_vp_data()
    stops = get_sample_stops()[:2]  # Just 2 stops for speed
    
    results_by_type = {}
    
    print(f"\n{'-' * 70}")
    print("Testing each model type:")
    print(f"{'-' * 70}")
    
    for model_type in model_types:
        print(f"\n{model_type.upper()}:")
        
        # Test with auto-selection for this type
        result = estimate_stop_times(
            vehicle_position=vp_data,
            upcoming_stops=stops,
            route_id='1',
            trip_id='trip_001',
            model_type=model_type,
            prefer_route_model=True,
            max_stops=2
        )
        
        if result.get('error'):
            print(f"  ✗ Failed: {result['error']}")
        else:
            first_pred = result['predictions'][0] if result['predictions'] else None
            if first_pred and not first_pred.get('error'):
                print(f"  ✓ Success!")
                print(f"    Scope: {result.get('model_scope', 'unknown')}")
                print(f"    Model: {result['model_key'][:60]}...")
                print(f"    First stop ETA: {first_pred['eta_formatted']}")
                results_by_type[model_type] = first_pred['eta_seconds']
            else:
                print(f"  ✗ Prediction failed")
    
    # Summary comparison
    if results_by_type:
        print(f"\n{'-' * 70}")
        print("COMPARISON (First Stop ETA):")
        print(f"{'-' * 70}")
        for mtype, eta_sec in sorted(results_by_type.items(), key=lambda x: x[1]):
            print(f"  {mtype:20s}: {eta_sec:6.0f}s ({eta_sec/60:5.1f} min)")
    
    print("\n" + "=" * 70)
    return True


def test_xgboost_models():
    """Dedicated test to validate XGBoost ETA estimations."""
    
    print("\n\nTEST 4: XGBoost Models (Route-Specific & Global)")
    print("=" * 70)
    
    registry = get_registry()
    
    # List all xgboost models
    df = registry.list_models(model_type='xgboost')
    if df.empty:
        print("\nNo xgboost models found in registry. Skipping test.")
        print("=" * 70)
        return True
    
    print(f"\nFound {len(df)} xgboost models in registry:")
    routes = registry.get_routes(model_type='xgboost')
    print(f"  Routes with xgboost models: {routes}")
    
    # Separate route-specific and global models
    route_df = df[df['route_id'] != 'global']
    global_df = df[df['route_id'] == 'global']
    
    vp_data = get_sample_vp_data()
    stops = get_sample_stops()[:2]
    
    # Test route-specific xgboost if available
    if not route_df.empty:
        print(f"\n--- Route-specific XGBoost ---")
        test_routes = routes[:3] if len(routes) >= 3 else routes
        for route_id in test_routes:
            print(f"\nRoute {route_id}:")
            result = estimate_stop_times(
                vehicle_position=get_sample_vp_data(route_id),
                upcoming_stops=stops,
                route_id=route_id,
                trip_id=f"trip_xgb_{route_id}",
                model_type='xgboost',
                prefer_route_model=True,
                max_stops=2,
            )
            if result.get('error'):
                print(f"  ✗ Error: {result['error']}")
            else:
                print(f"  ✓ Scope: {result.get('model_scope', 'unknown')}")
                print(f"    Model: {result.get('model_key', '')[:60]}...")
                if result['predictions'] and not result['predictions'][0].get('error'):
                    first = result['predictions'][0]
                    print(f"    First stop ETA: {first['eta_formatted']} ({first['eta_seconds']:.0f}s)")
                else:
                    print("    ✗ Prediction failed for first stop.")
    else:
        print("\nNo route-specific xgboost models found.")
    
    # Test global xgboost if available
    if not global_df.empty:
        print(f"\n--- Global XGBoost ---")
        result = estimate_stop_times(
            vehicle_position=vp_data,
            upcoming_stops=stops,
            route_id='1',
            trip_id='trip_xgb_global',
            model_type='xgboost',
            prefer_route_model=False,  # force global
            max_stops=2,
        )
        if result.get('error'):
            print(f"  ✗ Error: {result['error']}")
        else:
            print(f"  ✓ Scope: {result.get('model_scope', 'unknown')}")
            print(f"    Model: {result.get('model_key', '')[:60]}...")
            if result['predictions'] and not result['predictions'][0].get('error'):
                first = result['predictions'][0]
                print(f"    First stop ETA: {first['eta_formatted']} ({first['eta_seconds']:.0f}s)")
            else:
                print("    ✗ Prediction failed for first stop.")
    else:
        print("\nNo global xgboost models found.")
    
    print("\n" + "=" * 70)
    return True


def test_route_performance_comparison():
    """Compare predictions across different routes with varying training data."""
    
    print("\n\nTEST 5: Route Performance Comparison")
    print("=" * 70)
    
    registry = get_registry()
    
    # Get routes with their metadata
    routes = registry.get_routes(model_type='polyreg_time')

    print("ROUTES: ", routes)
    
    if not routes:
        print("No route-specific polyreg_time models found.")
        return True
    
    print(f"\nComparing polyreg_time models across {len(routes)} routes:\n")
    
    stops = get_sample_stops()[:1]  # Single stop for comparison
    results = []
    
    for route_id in routes:
        # Get best model for this route
        model_key = registry.get_best_model(
            model_type='polyreg_time',
            route_id=route_id,
            metric='test_mae_seconds'
        )
        
        if not model_key:
            continue
        
        # Get metadata
        try:
            metadata = registry.load_metadata(model_key)
            n_trips = metadata.get('n_trips', 0)
            test_mae_min = metadata.get('metrics', {}).get('test_mae_minutes', None)
        except:
            continue
        
        # Make prediction
        result = estimate_stop_times(
            vehicle_position=get_sample_vp_data(route_id),
            upcoming_stops=stops,
            route_id=route_id,
            model_key=model_key,
            max_stops=1
        )
        
        if result.get('predictions') and not result['predictions'][0].get('error'):
            eta = result['predictions'][0]['eta_seconds']
            results.append({
                'route_id': route_id,
                'n_trips': n_trips,
                'test_mae_min': test_mae_min,
                'prediction_eta': eta
            })
    
    if results:
        # Sort by number of trips
        results.sort(key=lambda x: x['n_trips'], reverse=True)
        
        print(f"{'Route':8s} {'Trips':>6s} {'Test MAE':>10s} {'Prediction':>12s}")
        print("-" * 45)
        
        for r in results:
            mae_str = f"{r['test_mae_min']:.2f} min" if r['test_mae_min'] else "N/A"
            eta_str = f"{r['prediction_eta']:.0f}s ({r['prediction_eta']/60:.1f}m)"
            print(f"{r['route_id']:8s} {r['n_trips']:6d} {mae_str:>10s} {eta_str:>12s}")
        
        print(f"\n{'='*70}")
        print("Insight: Routes with more training trips should have lower MAE")
        print("and potentially more accurate predictions for similar conditions.")
        print(f"{'='*70}")
    
    print("\n" + "=" * 70)
    return True


def test_different_distances():
    """Test predictions at various distances."""
    
    print("\n\nTEST 6: Predictions at Different Distances")
    print("=" * 70)
    
    vp_data = get_sample_vp_data()
    
    # Create stops at various distances
    test_distances = [100, 500, 1000, 2000, 5000]  # meters
    
    print("\nTesting stop distances:")
    
    registry = get_registry()
    # Try to find a polyreg_distance model for this test
    df = registry.list_models(route_id='Green-E', model_type='xgboost')
    # df = registry.list_models(model_type='polyreg_distance')
    
    if df.empty:
        print("  No polyreg_distance models found. Using best model.")
        model_key = None
    else:
        model_key = df.iloc[0]['model_key']
        print(f"  Using model: {model_key[:60]}...")
    
    model_key = None
    for dist_m in test_distances:
        # Calculate approximate lat/lon offset
        lat_offset = dist_m / 111320.0  # Rough: 1 degree lat ≈ 111.32 km
        
        stops = [{
            'stop_id': f'stop_at_{dist_m}m',
            'stop_sequence': 1,
            'lat': vp_data['lat'] + lat_offset,
            'lon': vp_data['lon']
        }]
        
        result = estimate_stop_times(
            vehicle_position=vp_data,
            upcoming_stops=stops,
            route_id='1',
            model_key=model_key,
            max_stops=1
        )
        
        if result.get('error') or not result['predictions']:
            print(f"  {dist_m:5d}m: ERROR")
        else:
            pred = result['predictions'][0]
            if pred.get('error'):
                print(f"  {dist_m:5d}m: {pred['error']}")
            else:
                print(f"  {dist_m:5d}m → {pred['eta_formatted']:8s} ({pred['eta_seconds']:6.0f}s)")
    
    print("\n" + "=" * 70)
    return True


def test_edge_cases():
    """Test error handling."""
    
    print("\n\nTEST 7: Edge Cases & Error Handling")
    print("=" * 70)
    
    # Test 1: No stops
    print("\n7a. Empty stops list:")
    result = estimate_stop_times(
        vehicle_position=get_sample_vp_data(),
        upcoming_stops=[],
        route_id='1'
    )
    print(f"    Expected error: {result.get('error', 'N/A')}")
    print(f"    ✓ Handled gracefully" if result.get('error') else "    ✗ Should have errored")
    
    # Test 2: Invalid model key
    print("\n7b. Invalid model key:")
    result = estimate_stop_times(
        vehicle_position=get_sample_vp_data(),
        upcoming_stops=get_sample_stops()[:1],
        route_id='1',
        model_key='nonexistent_model_12345'
    )
    print(f"    Expected error: {result.get('error', 'N/A')}")
    print(f"    ✓ Handled gracefully" if result.get('error') else "    ✗ Should have errored")
    
    # Test 3: Missing optional fields
    print("\n7c. Minimal vehicle position:")
    minimal_vp = {
        'vehicle_id': 'minimal_bus',
        'lat': 9.9281,
        'lon': -84.0907,
        'timestamp': datetime.now(timezone.utc).isoformat()
        # Missing: speed, heading, route
    }
    result = estimate_stop_times(
        vehicle_position=minimal_vp,
        upcoming_stops=get_sample_stops()[:1],
        route_id='1'
    )
    if result.get('error'):
        print(f"    Error: {result['error']}")
    else:
        print(f"    ✓ Handled missing fields, got ETA: {result['predictions'][0].get('eta_formatted', 'N/A')}")
    
    # Test 4: Route with no trained model
    print("\n7d. Route without trained model:")
    result = estimate_stop_times(
        vehicle_position=get_sample_vp_data('999'),
        upcoming_stops=get_sample_stops()[:1],
        route_id='999',
        prefer_route_model=True,
        max_stops=1
    )
    if result.get('error'):
        print(f"    Error: {result['error']}")
    else:
        print(f"    ✓ Fell back to: {result.get('model_scope', 'N/A')} model")
        if result['predictions'] and not result['predictions'][0].get('error'):
            print(f"    ETA: {result['predictions'][0]['eta_formatted']}")
    
    print("\n" + "=" * 70)
    return True


if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("ETA ESTIMATOR TEST SUITE - Route-Specific Edition")
    print("=" * 70 + "\n")
    
    tests = [
        ("Basic Prediction", test_basic_prediction),
        ("Route-Specific Models", test_route_specific_models),
        ("All Model Types", test_all_model_types),
        ("XGBoost Models", test_xgboost_models),  # NEW
        ("Route Performance Comparison", test_route_performance_comparison),
        ("Different Distances", test_different_distances),
        ("Edge Cases", test_edge_cases),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"\n✗ {test_name} CRASHED: {str(e)}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n" + "=" * 70)
    print(f"FINAL RESULTS: {passed}/{len(tests)} tests passed")
    if failed == 0:
        print("🎉 All tests passed!")
    else:
        print(f"⚠️  {failed} test(s) failed or crashed")
    print("=" * 70 + "\n")
