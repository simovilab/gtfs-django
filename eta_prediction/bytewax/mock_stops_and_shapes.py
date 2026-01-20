#!/usr/bin/env python3
"""
Mock route stops and GTFS shapes data for testing ETA prediction flow.
This provides hardcoded stop sequences and shape polylines for 5 coherent routes.

Each route has realistic stops and shapes that match the demo buses:
- Route-1: BUS-001 (8 stops, North-South route)
- Route-2: BUS-002 (7 stops, Northeast diagonal)
- Route-3: BUS-003 (6 stops, East-West route)
- Route-4: BUS-004 (9 stops, Circular route)
- Route-5: BUS-005 (7 stops, Northwest route)
"""

# Mock stop data for 5 demo routes
# Format: route_id -> list of stops with stop_id, stop_sequence, lat, lon
# Stops are positioned around NYC coordinates with realistic 150-250m spacing
MOCK_ROUTE_STOPS = {
    # Route 1: North-South route (8 stops)
    "Route-1": [
        {"stop_id": "stop_1_001", "stop_sequence": 1, "lat": 40.7128, "lon": -74.0060, "name": "Downtown Hub"},
        {"stop_id": "stop_1_002", "stop_sequence": 2, "lat": 40.7143, "lon": -74.0058, "name": "City Hall"},
        {"stop_id": "stop_1_003", "stop_sequence": 3, "lat": 40.7158, "lon": -74.0056, "name": "Park Avenue"},
        {"stop_id": "stop_1_004", "stop_sequence": 4, "lat": 40.7173, "lon": -74.0054, "name": "Central Plaza"},
        {"stop_id": "stop_1_005", "stop_sequence": 5, "lat": 40.7188, "lon": -74.0052, "name": "Business District"},
        {"stop_id": "stop_1_006", "stop_sequence": 6, "lat": 40.7203, "lon": -74.0050, "name": "University Station"},
        {"stop_id": "stop_1_007", "stop_sequence": 7, "lat": 40.7218, "lon": -74.0048, "name": "North Market"},
        {"stop_id": "stop_1_008", "stop_sequence": 8, "lat": 40.7233, "lon": -74.0046, "name": "Terminal North"},
    ],
    
    # Route 2: Northeast diagonal (7 stops)
    "Route-2": [
        {"stop_id": "stop_2_001", "stop_sequence": 1, "lat": 40.7100, "lon": -74.0065, "name": "South Terminal"},
        {"stop_id": "stop_2_002", "stop_sequence": 2, "lat": 40.7115, "lon": -74.0050, "name": "Market Street"},
        {"stop_id": "stop_2_003", "stop_sequence": 3, "lat": 40.7130, "lon": -74.0035, "name": "Shopping Center"},
        {"stop_id": "stop_2_004", "stop_sequence": 4, "lat": 40.7145, "lon": -74.0020, "name": "Metro Station"},
        {"stop_id": "stop_2_005", "stop_sequence": 5, "lat": 40.7160, "lon": -74.0005, "name": "Hospital"},
        {"stop_id": "stop_2_006", "stop_sequence": 6, "lat": 40.7175, "lon": -73.9990, "name": "Tech Campus"},
        {"stop_id": "stop_2_007", "stop_sequence": 7, "lat": 40.7190, "lon": -73.9975, "name": "Northeast Hub"},
    ],
    
    # Route 3: East-West route (6 stops)
    "Route-3": [
        {"stop_id": "stop_3_001", "stop_sequence": 1, "lat": 40.7150, "lon": -74.0100, "name": "West Terminal"},
        {"stop_id": "stop_3_002", "stop_sequence": 2, "lat": 40.7150, "lon": -74.0070, "name": "Convention Center"},
        {"stop_id": "stop_3_003", "stop_sequence": 3, "lat": 40.7150, "lon": -74.0040, "name": "City Center"},
        {"stop_id": "stop_3_004", "stop_sequence": 4, "lat": 40.7150, "lon": -74.0010, "name": "Arts District"},
        {"stop_id": "stop_3_005", "stop_sequence": 5, "lat": 40.7150, "lon": -73.9980, "name": "Riverside Park"},
        {"stop_id": "stop_3_006", "stop_sequence": 6, "lat": 40.7150, "lon": -73.9950, "name": "East Terminal"},
    ],
    
    # Route 4: Circular route (9 stops)
    "Route-4": [
        {"stop_id": "stop_4_001", "stop_sequence": 1, "lat": 40.7128, "lon": -74.0060, "name": "Circle Start"},
        {"stop_id": "stop_4_002", "stop_sequence": 2, "lat": 40.7145, "lon": -74.0080, "name": "West Point"},
        {"stop_id": "stop_4_003", "stop_sequence": 3, "lat": 40.7170, "lon": -74.0085, "name": "Northwest Corner"},
        {"stop_id": "stop_4_004", "stop_sequence": 4, "lat": 40.7190, "lon": -74.0070, "name": "North Point"},
        {"stop_id": "stop_4_005", "stop_sequence": 5, "lat": 40.7195, "lon": -74.0040, "name": "Northeast Corner"},
        {"stop_id": "stop_4_006", "stop_sequence": 6, "lat": 40.7180, "lon": -74.0020, "name": "East Point"},
        {"stop_id": "stop_4_007", "stop_sequence": 7, "lat": 40.7155, "lon": -74.0015, "name": "Southeast Corner"},
        {"stop_id": "stop_4_008", "stop_sequence": 8, "lat": 40.7135, "lon": -74.0030, "name": "South Point"},
        {"stop_id": "stop_4_009", "stop_sequence": 9, "lat": 40.7128, "lon": -74.0060, "name": "Circle End"},
    ],
    
    # Route 5: Northwest route (7 stops)
    "Route-5": [
        {"stop_id": "stop_5_001", "stop_sequence": 1, "lat": 40.7080, "lon": -74.0030, "name": "Southeast Station"},
        {"stop_id": "stop_5_002", "stop_sequence": 2, "lat": 40.7100, "lon": -74.0045, "name": "Memorial Park"},
        {"stop_id": "stop_5_003", "stop_sequence": 3, "lat": 40.7120, "lon": -74.0060, "name": "Central Hub"},
        {"stop_id": "stop_5_004", "stop_sequence": 4, "lat": 40.7145, "lon": -74.0075, "name": "Industrial Area"},
        {"stop_id": "stop_5_005", "stop_sequence": 5, "lat": 40.7170, "lon": -74.0090, "name": "Port District"},
        {"stop_id": "stop_5_006", "stop_sequence": 6, "lat": 40.7195, "lon": -74.0105, "name": "Harbor View"},
        {"stop_id": "stop_5_007", "stop_sequence": 7, "lat": 40.7220, "lon": -74.0120, "name": "Northwest Terminal"},
    ],
}


def _generate_shape_points(stops: list, points_per_segment: int = 4) -> list:
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
                curve_factor = 0.00003 * (1 if i % 2 == 0 else -1)
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
        "points": _generate_shape_points(stops, points_per_segment=4)
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
    print("LOADING MOCK ROUTE DATA TO REDIS - 5 COHERENT DEMO ROUTES")
    print("NYC AREA - Routes matched to BUS-001 through BUS-005")
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
        print("SUMMARY - DEMO ROUTES")
        print("="*70)
        print(f"✓ Loaded {stops_count} route stop sequences")
        print(f"✓ Loaded {shapes_count} route shape polylines")
        print(f"✓ Total shape points: {sum(len(s['points']) for s in MOCK_ROUTE_SHAPES.values())}")
        print("\nRoute-to-Bus Mapping:")
        print("  • Route-1 (8 stops) → BUS-001 (North-South)")
        print("  • Route-2 (7 stops) → BUS-002 (Northeast diagonal)")
        print("  • Route-3 (6 stops) → BUS-003 (East-West)")
        print("  • Route-4 (9 stops) → BUS-004 (Circular)")
        print("  • Route-5 (7 stops) → BUS-005 (Northwest)")
        print("\nYour Bytewax flow can now fetch:")
        print("  • Stops from Redis: route_stops:{route_id}")
        print("  • Shapes from Redis: route_shape:{route_id}")
        print("="*70)
        
    except redis.ConnectionError as e:
        print(f"✗ Failed to connect to Redis: {e}")
        print("Make sure Redis is running: redis-server")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()