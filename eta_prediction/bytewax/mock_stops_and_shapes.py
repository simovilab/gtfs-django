#!/usr/bin/env python3
"""
Mock route stops and GTFS shapes data for testing ETA prediction flow.
This provides hardcoded stop sequences and shape polylines for routes when Redis/PostgreSQL cache is not available.
Replace with actual GTFS data in production.

Stops are positioned around NYC area (40.7128, -74.0060) with ~150-250m spacing.
Shapes follow realistic paths with intermediate points between stops.
"""

# Mock stop data for testing
# Format: route_id -> list of stops with stop_id, stop_sequence, lat, lon
# Stops are clustered around NYC coordinates with realistic 150-250m spacing
MOCK_ROUTE_STOPS = {
    "Route-1": [
        {"stop_id": "stop_1_001", "stop_sequence": 1, "lat": 40.7128, "lon": -74.0060},
        {"stop_id": "stop_1_002", "stop_sequence": 2, "lat": 40.7143, "lon": -74.0058},
        {"stop_id": "stop_1_003", "stop_sequence": 3, "lat": 40.7158, "lon": -74.0056},
        {"stop_id": "stop_1_004", "stop_sequence": 4, "lat": 40.7173, "lon": -74.0054},
        {"stop_id": "stop_1_005", "stop_sequence": 5, "lat": 40.7188, "lon": -74.0052},
        {"stop_id": "stop_1_006", "stop_sequence": 6, "lat": 40.7203, "lon": -74.0050},
        {"stop_id": "stop_1_007", "stop_sequence": 7, "lat": 40.7218, "lon": -74.0048},
        {"stop_id": "stop_1_008", "stop_sequence": 8, "lat": 40.7233, "lon": -74.0046},
    ],
    "Route-2": [
        {"stop_id": "stop_2_001", "stop_sequence": 1, "lat": 40.7100, "lon": -74.0065},
        {"stop_id": "stop_2_002", "stop_sequence": 2, "lat": 40.7115, "lon": -74.0063},
        {"stop_id": "stop_2_003", "stop_sequence": 3, "lat": 40.7130, "lon": -74.0061},
        {"stop_id": "stop_2_004", "stop_sequence": 4, "lat": 40.7145, "lon": -74.0059},
        {"stop_id": "stop_2_005", "stop_sequence": 5, "lat": 40.7160, "lon": -74.0057},
        {"stop_id": "stop_2_006", "stop_sequence": 6, "lat": 40.7175, "lon": -74.0055},
        {"stop_id": "stop_2_007", "stop_sequence": 7, "lat": 40.7190, "lon": -74.0053},
    ],
    "Route-3": [
        {"stop_id": "stop_3_001", "stop_sequence": 1, "lat": 40.7080, "lon": -74.0075},
        {"stop_id": "stop_3_002", "stop_sequence": 2, "lat": 40.7098, "lon": -74.0071},
        {"stop_id": "stop_3_003", "stop_sequence": 3, "lat": 40.7116, "lon": -74.0067},
        {"stop_id": "stop_3_004", "stop_sequence": 4, "lat": 40.7134, "lon": -74.0063},
        {"stop_id": "stop_3_005", "stop_sequence": 5, "lat": 40.7152, "lon": -74.0059},
    ],
    "Route-4": [
        {"stop_id": "stop_4_001", "stop_sequence": 1, "lat": 40.7150, "lon": -74.0070},
        {"stop_id": "stop_4_002", "stop_sequence": 2, "lat": 40.7165, "lon": -74.0067},
        {"stop_id": "stop_4_003", "stop_sequence": 3, "lat": 40.7180, "lon": -74.0064},
        {"stop_id": "stop_4_004", "stop_sequence": 4, "lat": 40.7195, "lon": -74.0061},
        {"stop_id": "stop_4_005", "stop_sequence": 5, "lat": 40.7210, "lon": -74.0058},
        {"stop_id": "stop_4_006", "stop_sequence": 6, "lat": 40.7225, "lon": -74.0055},
    ],
    "Route-5": [
        {"stop_id": "stop_5_001", "stop_sequence": 1, "lat": 40.7050, "lon": -74.0040},
        {"stop_id": "stop_5_002", "stop_sequence": 2, "lat": 40.7067, "lon": -74.0044},
        {"stop_id": "stop_5_003", "stop_sequence": 3, "lat": 40.7084, "lon": -74.0048},
        {"stop_id": "stop_5_004", "stop_sequence": 4, "lat": 40.7101, "lon": -74.0052},
        {"stop_id": "stop_5_005", "stop_sequence": 5, "lat": 40.7118, "lon": -74.0056},
        {"stop_id": "stop_5_006", "stop_sequence": 6, "lat": 40.7135, "lon": -74.0060},
        {"stop_id": "stop_5_007", "stop_sequence": 7, "lat": 40.7152, "lon": -74.0064},
        {"stop_id": "stop_5_008", "stop_sequence": 8, "lat": 40.7169, "lon": -74.0068},
        {"stop_id": "stop_5_009", "stop_sequence": 9, "lat": 40.7186, "lon": -74.0072},
    ],
    "Route-6": [
        {"stop_id": "stop_6_001", "stop_sequence": 1, "lat": 40.7128, "lon": -73.9980},
        {"stop_id": "stop_6_002", "stop_sequence": 2, "lat": 40.7143, "lon": -73.9985},
        {"stop_id": "stop_6_003", "stop_sequence": 3, "lat": 40.7158, "lon": -73.9990},
        {"stop_id": "stop_6_004", "stop_sequence": 4, "lat": 40.7173, "lon": -73.9995},
        {"stop_id": "stop_6_005", "stop_sequence": 5, "lat": 40.7188, "lon": -74.0000},
        {"stop_id": "stop_6_006", "stop_sequence": 6, "lat": 40.7203, "lon": -74.0005},
    ],
    "Route-7": [
        {"stop_id": "stop_7_001", "stop_sequence": 1, "lat": 40.7070, "lon": -74.0095},
        {"stop_id": "stop_7_002", "stop_sequence": 2, "lat": 40.7087, "lon": -74.0090},
        {"stop_id": "stop_7_003", "stop_sequence": 3, "lat": 40.7104, "lon": -74.0085},
        {"stop_id": "stop_7_004", "stop_sequence": 4, "lat": 40.7121, "lon": -74.0080},
        {"stop_id": "stop_7_005", "stop_sequence": 5, "lat": 40.7138, "lon": -74.0075},
        {"stop_id": "stop_7_006", "stop_sequence": 6, "lat": 40.7155, "lon": -74.0070},
        {"stop_id": "stop_7_007", "stop_sequence": 7, "lat": 40.7172, "lon": -74.0065},
    ],
    "Route-8": [
        {"stop_id": "stop_8_001", "stop_sequence": 1, "lat": 40.7180, "lon": -74.0100},
        {"stop_id": "stop_8_002", "stop_sequence": 2, "lat": 40.7195, "lon": -74.0095},
        {"stop_id": "stop_8_003", "stop_sequence": 3, "lat": 40.7210, "lon": -74.0090},
        {"stop_id": "stop_8_004", "stop_sequence": 4, "lat": 40.7225, "lon": -74.0085},
        {"stop_id": "stop_8_005", "stop_sequence": 5, "lat": 40.7240, "lon": -74.0080},
    ],
    "Route-9": [
        {"stop_id": "stop_9_001", "stop_sequence": 1, "lat": 40.7040, "lon": -74.0020},
        {"stop_id": "stop_9_002", "stop_sequence": 2, "lat": 40.7057, "lon": -74.0026},
        {"stop_id": "stop_9_003", "stop_sequence": 3, "lat": 40.7074, "lon": -74.0032},
        {"stop_id": "stop_9_004", "stop_sequence": 4, "lat": 40.7091, "lon": -74.0038},
        {"stop_id": "stop_9_005", "stop_sequence": 5, "lat": 40.7108, "lon": -74.0044},
        {"stop_id": "stop_9_006", "stop_sequence": 6, "lat": 40.7125, "lon": -74.0050},
        {"stop_id": "stop_9_007", "stop_sequence": 7, "lat": 40.7142, "lon": -74.0056},
        {"stop_id": "stop_9_008", "stop_sequence": 8, "lat": 40.7159, "lon": -74.0062},
    ],
    "Route-10": [
        {"stop_id": "stop_10_001", "stop_sequence": 1, "lat": 40.7160, "lon": -73.9960},
        {"stop_id": "stop_10_002", "stop_sequence": 2, "lat": 40.7175, "lon": -73.9966},
        {"stop_id": "stop_10_003", "stop_sequence": 3, "lat": 40.7190, "lon": -73.9972},
        {"stop_id": "stop_10_004", "stop_sequence": 4, "lat": 40.7205, "lon": -73.9978},
        {"stop_id": "stop_10_005", "stop_sequence": 5, "lat": 40.7220, "lon": -73.9984},
        {"stop_id": "stop_10_006", "stop_sequence": 6, "lat": 40.7235, "lon": -73.9990},
    ],
}


def _generate_shape_points(stops: list, points_per_segment: int = 3) -> list:
    """
    Generate realistic shape points between stops with slight curves.
    
    Args:
        stops: List of stop dictionaries with lat/lon
        points_per_segment: Number of intermediate points between each stop pair
    
    Returns:
        List of shape point dicts with shape_pt_lat, shape_pt_lon, shape_pt_sequence
    """
    shape_points = []
    sequence = 1
    
    for i in range(len(stops)):
        # Add the stop location as a shape point
        shape_points.append({
            "shape_pt_lat": stops[i]["lat"],
            "shape_pt_lon": stops[i]["lon"],
            "shape_pt_sequence": sequence,
            "shape_dist_traveled": None  # Will be calculated by ShapePolyline
        })
        sequence += 1
        
        # Add intermediate points between this stop and the next
        if i < len(stops) - 1:
            lat1, lon1 = stops[i]["lat"], stops[i]["lon"]
            lat2, lon2 = stops[i + 1]["lat"], stops[i + 1]["lon"]
            
            for j in range(1, points_per_segment + 1):
                # Linear interpolation with slight curve
                t = j / (points_per_segment + 1)
                
                # Add small perpendicular offset for realistic road curves
                # Alternating left/right curve
                curve_factor = 0.00002 * (1 if i % 2 == 0 else -1)
                perp_offset = curve_factor * (1 - 2 * abs(t - 0.5))
                
                lat = lat1 + (lat2 - lat1) * t
                lon = lon1 + (lon2 - lon1) * t + perp_offset
                
                shape_points.append({
                    "shape_pt_lat": lat,
                    "shape_pt_lon": lon,
                    "shape_pt_sequence": sequence,
                    "shape_dist_traveled": None
                })
                sequence += 1
    
    return shape_points


# Generate mock shapes for each route
MOCK_ROUTE_SHAPES = {}
for route_id, stops in MOCK_ROUTE_STOPS.items():
    shape_id = f"shape_{route_id.lower().replace('-', '_')}"
    MOCK_ROUTE_SHAPES[route_id] = {
        "shape_id": shape_id,
        "points": _generate_shape_points(stops, points_per_segment=3)
    }


def get_route_stops(route_id: str) -> list:
    """
    Get stops for a given route.
    
    Args:
        route_id: Route identifier
    
    Returns:
        List of stop dictionaries, or empty list if route not found
    """
    return MOCK_ROUTE_STOPS.get(route_id, [])


def get_route_shape(route_id: str) -> dict:
    """
    Get shape data for a given route.
    
    Args:
        route_id: Route identifier
    
    Returns:
        Dict with shape_id and points list, or None if route not found
    """
    return MOCK_ROUTE_SHAPES.get(route_id)


def load_all_stops_to_redis(redis_client, key_prefix: str = "route_stops:"):
    """
    Load all mock route stops into Redis for testing.
    
    Args:
        redis_client: Redis client instance
        key_prefix: Prefix for Redis keys (default: "route_stops:")
    
    Returns:
        Number of routes loaded
    """
    import json
    
    count = 0
    for route_id, stops in MOCK_ROUTE_STOPS.items():
        key = f"{key_prefix}{route_id}"
        value = json.dumps(stops)
        redis_client.set(key, value)
        count += 1
        print(f"✓ Loaded {len(stops)} stops for {route_id} -> {key}")
    
    print(f"\n✓ Total: Loaded {count} routes to Redis")
    return count


def load_all_shapes_to_redis(redis_client, key_prefix: str = "route_shape:"):
    """
    Load all mock route shapes into Redis for testing.
    
    Args:
        redis_client: Redis client instance
        key_prefix: Prefix for Redis keys (default: "route_shape:")
    
    Returns:
        Number of shapes loaded
    """
    import json
    
    count = 0
    for route_id, shape_data in MOCK_ROUTE_SHAPES.items():
        key = f"{key_prefix}{route_id}"
        value = json.dumps(shape_data)
        redis_client.set(key, value)
        count += 1
        print(f"✓ Loaded {len(shape_data['points'])} shape points for {route_id} -> {key}")
    
    print(f"\n✓ Total: Loaded {count} shapes to Redis")
    return count


def load_all_to_redis(redis_client, 
                     stops_key_prefix: str = "route_stops:",
                     shapes_key_prefix: str = "route_shape:"):
    """
    Load both stops and shapes to Redis.
    
    Args:
        redis_client: Redis client instance
        stops_key_prefix: Prefix for stop keys
        shapes_key_prefix: Prefix for shape keys
    
    Returns:
        Tuple of (stops_count, shapes_count)
    """
    print("Loading stops...")
    stops_count = load_all_stops_to_redis(redis_client, stops_key_prefix)
    
    print("\nLoading shapes...")
    shapes_count = load_all_shapes_to_redis(redis_client, shapes_key_prefix)
    
    return stops_count, shapes_count


def create_mock_shape_polyline(route_id: str):
    """
    Create a ShapePolyline object from mock data for testing.
    
    Args:
        route_id: Route identifier
    
    Returns:
        ShapePolyline object or None if route not found or spatial module unavailable
    """
    try:
        from feature_engineering.spatial import ShapePolyline
    except ImportError:
        print("⚠️  ShapePolyline not available, install feature_engineering module")
        return None
    
    shape_data = get_route_shape(route_id)
    if not shape_data:
        return None
    
    # Convert to format expected by ShapePolyline
    points = [
        (pt["shape_pt_lat"], pt["shape_pt_lon"]) 
        for pt in shape_data["points"]
    ]
    
    return ShapePolyline(points)


if __name__ == "__main__":
    """
    Standalone script to populate Redis with mock route stops and shapes.
    Usage: python mock_route_stops.py
    """
    import redis
    
    print("="*70)
    print("LOADING MOCK ROUTE DATA TO REDIS")
    print("NYC AREA - 150-250m stop spacing with realistic shape polylines")
    print("="*70 + "\n")
    
    try:
        client = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=True
        )
        client.ping()
        print("✓ Connected to Redis at localhost:6379\n")
        
        stops_count, shapes_count = load_all_to_redis(client)
        
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        print(f"✓ Loaded {stops_count} route stop sequences")
        print(f"✓ Loaded {shapes_count} route shape polylines")
        print(f"✓ Total shape points: {sum(len(s['points']) for s in MOCK_ROUTE_SHAPES.values())}")
        print("\nYour Bytewax flow can now fetch:")
        print("  • Stops from Redis: route_stops:{route_id}")
        print("  • Shapes from Redis: route_shape:{route_id}")
        print("\nShape data enables:")
        print("  • Accurate distance along route")
        print("  • Cross-track error detection")
        print("  • Progress ratio calculation")
        print("="*70)
        
    except redis.ConnectionError as e:
        print(f"✗ Failed to connect to Redis: {e}")
        print("Make sure Redis is running: redis-server")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()