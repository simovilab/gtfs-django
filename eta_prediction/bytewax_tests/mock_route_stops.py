#!/usr/bin/env python3
"""
Mock route stops data for testing ETA prediction flow.

This provides hardcoded stop sequences for routes when Redis cache is not available.
Replace with actual GTFS data in production.
"""

# Mock stop data for testing
# Format: route_id -> list of stops with stop_id, stop_sequence, lat, lon
MOCK_ROUTE_STOPS = {
    "Route-1": [
        {"stop_id": "stop_1_001", "stop_sequence": 1, "lat": 9.9281, "lon": -84.0907},
        {"stop_id": "stop_1_002", "stop_sequence": 2, "lat": 9.9291, "lon": -84.0897},
        {"stop_id": "stop_1_003", "stop_sequence": 3, "lat": 9.9301, "lon": -84.0887},
        {"stop_id": "stop_1_004", "stop_sequence": 4, "lat": 9.9311, "lon": -84.0877},
        {"stop_id": "stop_1_005", "stop_sequence": 5, "lat": 9.9321, "lon": -84.0867},
        {"stop_id": "stop_1_006", "stop_sequence": 6, "lat": 9.9331, "lon": -84.0857},
        {"stop_id": "stop_1_007", "stop_sequence": 7, "lat": 9.9341, "lon": -84.0847},
        {"stop_id": "stop_1_008", "stop_sequence": 8, "lat": 9.9351, "lon": -84.0837},
    ],
    
    "Route-2": [
        {"stop_id": "stop_2_001", "stop_sequence": 1, "lat": 9.9300, "lon": -84.0800},
        {"stop_id": "stop_2_002", "stop_sequence": 2, "lat": 9.9310, "lon": -84.0810},
        {"stop_id": "stop_2_003", "stop_sequence": 3, "lat": 9.9320, "lon": -84.0820},
        {"stop_id": "stop_2_004", "stop_sequence": 4, "lat": 9.9330, "lon": -84.0830},
        {"stop_id": "stop_2_005", "stop_sequence": 5, "lat": 9.9340, "lon": -84.0840},
        {"stop_id": "stop_2_006", "stop_sequence": 6, "lat": 9.9350, "lon": -84.0850},
        {"stop_id": "stop_2_007", "stop_sequence": 7, "lat": 9.9360, "lon": -84.0860},
    ],
    
    "Route-3": [
        {"stop_id": "stop_3_001", "stop_sequence": 1, "lat": 9.9250, "lon": -84.0900},
        {"stop_id": "stop_3_002", "stop_sequence": 2, "lat": 9.9260, "lon": -84.0890},
        {"stop_id": "stop_3_003", "stop_sequence": 3, "lat": 9.9270, "lon": -84.0880},
        {"stop_id": "stop_3_004", "stop_sequence": 4, "lat": 9.9280, "lon": -84.0870},
        {"stop_id": "stop_3_005", "stop_sequence": 5, "lat": 9.9290, "lon": -84.0860},
    ],
    
    "Route-4": [
        {"stop_id": "stop_4_001", "stop_sequence": 1, "lat": 9.9400, "lon": -84.0950},
        {"stop_id": "stop_4_002", "stop_sequence": 2, "lat": 9.9410, "lon": -84.0940},
        {"stop_id": "stop_4_003", "stop_sequence": 3, "lat": 9.9420, "lon": -84.0930},
        {"stop_id": "stop_4_004", "stop_sequence": 4, "lat": 9.9430, "lon": -84.0920},
        {"stop_id": "stop_4_005", "stop_sequence": 5, "lat": 9.9440, "lon": -84.0910},
        {"stop_id": "stop_4_006", "stop_sequence": 6, "lat": 9.9450, "lon": -84.0900},
    ],
    
    "Route-5": [
        {"stop_id": "stop_5_001", "stop_sequence": 1, "lat": 9.9200, "lon": -84.0700},
        {"stop_id": "stop_5_002", "stop_sequence": 2, "lat": 9.9210, "lon": -84.0710},
        {"stop_id": "stop_5_003", "stop_sequence": 3, "lat": 9.9220, "lon": -84.0720},
        {"stop_id": "stop_5_004", "stop_sequence": 4, "lat": 9.9230, "lon": -84.0730},
        {"stop_id": "stop_5_005", "stop_sequence": 5, "lat": 9.9240, "lon": -84.0740},
        {"stop_id": "stop_5_006", "stop_sequence": 6, "lat": 9.9250, "lon": -84.0750},
        {"stop_id": "stop_5_007", "stop_sequence": 7, "lat": 9.9260, "lon": -84.0760},
        {"stop_id": "stop_5_008", "stop_sequence": 8, "lat": 9.9270, "lon": -84.0770},
        {"stop_id": "stop_5_009", "stop_sequence": 9, "lat": 9.9280, "lon": -84.0780},
    ],
    
    "Route-6": [
        {"stop_id": "stop_6_001", "stop_sequence": 1, "lat": 9.9350, "lon": -84.0600},
        {"stop_id": "stop_6_002", "stop_sequence": 2, "lat": 9.9360, "lon": -84.0610},
        {"stop_id": "stop_6_003", "stop_sequence": 3, "lat": 9.9370, "lon": -84.0620},
        {"stop_id": "stop_6_004", "stop_sequence": 4, "lat": 9.9380, "lon": -84.0630},
        {"stop_id": "stop_6_005", "stop_sequence": 5, "lat": 9.9390, "lon": -84.0640},
        {"stop_id": "stop_6_006", "stop_sequence": 6, "lat": 9.9400, "lon": -84.0650},
    ],
    
    "Route-7": [
        {"stop_id": "stop_7_001", "stop_sequence": 1, "lat": 9.9150, "lon": -84.0850},
        {"stop_id": "stop_7_002", "stop_sequence": 2, "lat": 9.9160, "lon": -84.0840},
        {"stop_id": "stop_7_003", "stop_sequence": 3, "lat": 9.9170, "lon": -84.0830},
        {"stop_id": "stop_7_004", "stop_sequence": 4, "lat": 9.9180, "lon": -84.0820},
        {"stop_id": "stop_7_005", "stop_sequence": 5, "lat": 9.9190, "lon": -84.0810},
        {"stop_id": "stop_7_006", "stop_sequence": 6, "lat": 9.9200, "lon": -84.0800},
        {"stop_id": "stop_7_007", "stop_sequence": 7, "lat": 9.9210, "lon": -84.0790},
    ],
    
    "Route-8": [
        {"stop_id": "stop_8_001", "stop_sequence": 1, "lat": 9.9500, "lon": -84.1000},
        {"stop_id": "stop_8_002", "stop_sequence": 2, "lat": 9.9510, "lon": -84.0990},
        {"stop_id": "stop_8_003", "stop_sequence": 3, "lat": 9.9520, "lon": -84.0980},
        {"stop_id": "stop_8_004", "stop_sequence": 4, "lat": 9.9530, "lon": -84.0970},
        {"stop_id": "stop_8_005", "stop_sequence": 5, "lat": 9.9540, "lon": -84.0960},
    ],
    
    "Route-9": [
        {"stop_id": "stop_9_001", "stop_sequence": 1, "lat": 9.9100, "lon": -84.0750},
        {"stop_id": "stop_9_002", "stop_sequence": 2, "lat": 9.9110, "lon": -84.0760},
        {"stop_id": "stop_9_003", "stop_sequence": 3, "lat": 9.9120, "lon": -84.0770},
        {"stop_id": "stop_9_004", "stop_sequence": 4, "lat": 9.9130, "lon": -84.0780},
        {"stop_id": "stop_9_005", "stop_sequence": 5, "lat": 9.9140, "lon": -84.0790},
        {"stop_id": "stop_9_006", "stop_sequence": 6, "lat": 9.9150, "lon": -84.0800},
        {"stop_id": "stop_9_007", "stop_sequence": 7, "lat": 9.9160, "lon": -84.0810},
        {"stop_id": "stop_9_008", "stop_sequence": 8, "lat": 9.9170, "lon": -84.0820},
    ],
    
    "Route-10": [
        {"stop_id": "stop_10_001", "stop_sequence": 1, "lat": 9.9450, "lon": -84.0550},
        {"stop_id": "stop_10_002", "stop_sequence": 2, "lat": 9.9460, "lon": -84.0560},
        {"stop_id": "stop_10_003", "stop_sequence": 3, "lat": 9.9470, "lon": -84.0570},
        {"stop_id": "stop_10_004", "stop_sequence": 4, "lat": 9.9480, "lon": -84.0580},
        {"stop_id": "stop_10_005", "stop_sequence": 5, "lat": 9.9490, "lon": -84.0590},
        {"stop_id": "stop_10_006", "stop_sequence": 6, "lat": 9.9500, "lon": -84.0600},
    ],
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


def load_all_stops_to_redis(redis_client, key_prefix: str = "route_stops:"):
    """
    Load all mock route stops into Redis for testing.
    
    Args:
        redis_client: Redis client instance
        key_prefix: Prefix for Redis keys (default: "route_stops:")
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


if __name__ == "__main__":
    """
    Standalone script to populate Redis with mock route stops.
    Usage: python mock_route_stops.py
    """
    import redis
    
    print("="*70)
    print("LOADING MOCK ROUTE STOPS TO REDIS")
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
        
        load_all_stops_to_redis(client)
        
        print("\n" + "="*70)
        print("Ready! Your Bytewax flow can now fetch stops from Redis.")
        print("="*70)
        
    except redis.ConnectionError as e:
        print(f"✗ Failed to connect to Redis: {e}")
        print("Make sure Redis is running: redis-server")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()