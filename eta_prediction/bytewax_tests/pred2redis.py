#!/usr/bin/env python3
"""
Bytewax dataflow for ETA prediction processing.

This flow:
1. Polls vehicle position data from Redis (where MQTT subscriber stores it)
2. Enriches with upcoming stops from Redis cache
3. Processes through estimate_stop_times() for ETA predictions
4. Stores predictions back to Redis under "processed:*" keys

Usage:
    pip install bytewax redis
    python -m bytewax.run bytewax_eta_flow
"""

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass
from pathlib import Path

import redis
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition

# Import the ETA estimator
import sys
sys.path.append(str(Path(__file__).parent.parent))
from eta_service.estimator import estimate_stop_times

# Import mock stops for fallback
from mock_route_stops import get_route_stops


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class RedisConfig:
    """Redis connection configuration"""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    vehicle_key_pattern: str = "vehicle:*"  # Pattern for vehicle position data
    route_stops_key_prefix: str = "route_stops:"  # Cache key for route stop sequences
    predictions_key_prefix: str = "processed:"  # Output key prefix for predictions
    poll_interval_ms: int = 1000  # How often to poll Redis (milliseconds)
    predictions_ttl: int = 300  # TTL for prediction cache (5 minutes)


# ============================================================================
# Redis Input Source
# ============================================================================

class RedisVehiclePartition(StatefulSourcePartition):
    """Partition that polls vehicle position data from Redis"""
    
    def __init__(self, config: RedisConfig):
        self.config = config
        self.client = None
        self._setup_client()
    
    def _setup_client(self):
        """Initialize Redis client and connect"""
        try:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True
            )
            self.client.ping()
            print(f"✓ Connected to Redis at {self.config.host}:{self.config.port}")
            print(f"✓ Polling pattern: {self.config.vehicle_key_pattern}\n")
        except redis.ConnectionError as e:
            print(f"✗ Failed to connect to Redis: {e}")
            raise
    
    def next_batch(self):
        """Get next batch of vehicle updates from Redis"""
        batch = []
        
        try:
            keys = self.client.keys(self.config.vehicle_key_pattern)
            
            for key in keys:
                try:
                    value = self.client.get(key)
                    
                    if value:
                        data = json.loads(value)
                        data['_redis_key'] = key
                        data['_fetched_at'] = datetime.now(timezone.utc).isoformat()
                        
                        ttl = self.client.ttl(key)
                        if ttl > 0:
                            data['_ttl_seconds'] = ttl
                        
                        batch.append(data)
                
                except json.JSONDecodeError as e:
                    print(f"⚠️  Failed to decode JSON from key {key}: {e}")
                except Exception as e:
                    print(f"⚠️  Error processing key {key}: {e}")
            
            if batch:
                print(f"📦 Fetched {len(batch)} vehicle records from Redis")
        
        except Exception as e:
            print(f"✗ Error in next_batch: {e}")
        
        return batch
    
    def next_awake(self):
        """Return when to wake up next (must be timezone-aware)"""
        return datetime.now(timezone.utc) + timedelta(milliseconds=self.config.poll_interval_ms)

    def snapshot(self):
        """Save state for recovery"""
        return None
    
    def close(self):
        """Cleanup when source is closed"""
        if self.client:
            self.client.close()
            print("✓ Disconnected from Redis")


class RedisVehicleSource(FixedPartitionedSource):
    """Source that creates Redis polling partitions"""
    
    def __init__(self, config: RedisConfig):
        self.config = config
    
    def list_parts(self):
        """List available partitions"""
        return ["redis-vehicles-0"]
    
    def build_part(self, step_id, for_part, resume_state):
        """Build partition for reading"""
        return RedisVehiclePartition(self.config)


# ============================================================================
# Processing Functions
# ============================================================================

def validate_vehicle_data(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Validate vehicle data has required fields for ETA estimation.
    Returns None if data is invalid (will be filtered out).
    """
    required_fields = ['vehicle_id', 'lat', 'lon', 'speed', 'timestamp', 'route']
    
    if not all(field in data for field in required_fields):
        missing = [f for f in required_fields if f not in data]
        print(f"⚠️  Invalid vehicle data: missing fields {missing}")
        return None
    
    # Validate data types and ranges
    try:
        float(data['lat'])
        float(data['lon'])
        float(data['speed'])
    except (ValueError, TypeError):
        print(f"⚠️  Invalid vehicle data: non-numeric lat/lon/speed")
        return None
    
    return data


def enrich_with_stops(data: Dict[str, Any], redis_config: RedisConfig) -> Optional[Dict[str, Any]]:
    """
    Enrich vehicle data with upcoming stops from Redis cache.
    Falls back to mock data if Redis cache is empty.
    
    Expected Redis cache structure:
    Key: "route_stops:{route_id}"
    Value: JSON array of stops with fields: stop_id, stop_sequence, lat, lon
    
    Example:
    [
        {"stop_id": "stop_001", "stop_sequence": 1, "lat": 9.9281, "lon": -84.0907},
        {"stop_id": "stop_002", "stop_sequence": 2, "lat": 9.9291, "lon": -84.0897},
        ...
    ]
    """
    route_id = data.get('route')
    
    if not route_id:
        print(f"⚠️  Vehicle {data.get('vehicle_id')} missing route_id")
        return None
    
    try:
        # Connect to Redis
        client = redis.Redis(
            host=redis_config.host,
            port=redis_config.port,
            db=redis_config.db,
            password=redis_config.password,
            decode_responses=True
        )
        
        # Get stops for this route
        stops_key = f"{redis_config.route_stops_key_prefix}{route_id}"
        stops_json = client.get(stops_key)
        
        if stops_json:
            # Found in Redis cache
            upcoming_stops = json.loads(stops_json)
        else:
            # Fallback to mock data
            print(f"ℹ️  Using mock stops for route {route_id} (Redis cache empty)")
            upcoming_stops = get_route_stops(route_id)
        
        if not upcoming_stops or not isinstance(upcoming_stops, list):
            print(f"⚠️  No stops available for route {route_id}")
            return None
        
        # Add stops to vehicle data
        data['upcoming_stops'] = upcoming_stops
        
        return data
    
    except json.JSONDecodeError as e:
        print(f"⚠️  Failed to decode stops JSON for route {route_id}: {e}")
        # Try mock data as fallback
        upcoming_stops = get_route_stops(route_id)
        if upcoming_stops:
            data['upcoming_stops'] = upcoming_stops
            return data
        return None
    except Exception as e:
        print(f"⚠️  Error enriching with stops: {e}")
        return None


def process_eta(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process vehicle data through the ETA estimator.
    
    Input: Vehicle position data enriched with upcoming stops
    Output: ETA predictions for upcoming stops
    """
    try:
        # Prepare vehicle position dict for estimator
        vehicle_position = {
            'vehicle_id': data['vehicle_id'],
            'route': data['route'],
            'lat': float(data['lat']),
            'lon': float(data['lon']),
            'speed': float(data['speed']),  # Assumed to be in m/s
            'heading': data.get('heading'),
            'timestamp': data['timestamp']
        }
        
        upcoming_stops = data.get('upcoming_stops', [])
        
        if not upcoming_stops:
            print(f"⚠️  No upcoming stops for vehicle {data['vehicle_id']}")
            return None
        
        # Get trip_id if available
        trip_id = data.get('trip_id')
        
        # Call the estimator
        result = estimate_stop_times(
            vehicle_position=vehicle_position,
            upcoming_stops=upcoming_stops,
            route_id=data['route'],
            trip_id=trip_id,
            max_stops=5  # Can be configured
        )
        
        # Add original vehicle data for context
        result['_original_vehicle_data'] = {
            'vehicle_id': data['vehicle_id'],
            'route': data['route'],
            'lat': data['lat'],
            'lon': data['lon'],
            'speed': data['speed'],
            'timestamp': data['timestamp']
        }
        
        print(f"✓ Processed ETA for vehicle {data['vehicle_id']} on route {data['route']}: "
              f"{len(result['predictions'])} stops predicted")
        
        return result
    
    except Exception as e:
        print(f"✗ Error processing ETA for vehicle {data.get('vehicle_id')}: {e}")
        import traceback
        traceback.print_exc()
        return None


def format_for_redis(result: Dict[str, Any]) -> tuple[str, str]:
    """
    Format ETA prediction result for Redis storage.
    
    Returns: (key, value) tuple
    Key format: "processed:{vehicle_id}"
    Value: JSON string of prediction result
    """
    vehicle_id = result.get('vehicle_id', 'unknown')
    key = f"processed:{vehicle_id}"
    
    # Add metadata
    result['_stored_at'] = datetime.now(timezone.utc).isoformat()
    
    value = json.dumps(result, indent=2)
    
    return (key, value)


# ============================================================================
# Redis Output Sink
# ============================================================================

class RedisETASinkPartition(StatelessSinkPartition):
    """Partition that writes ETA predictions to Redis"""
    
    def __init__(self, config: RedisConfig, worker_index: int):
        self.config = config
        self.worker_index = worker_index
        self.client = None
        self.count = 0
        self._setup_client()
    
    def _setup_client(self):
        """Initialize Redis client"""
        try:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True
            )
            self.client.ping()
            print(f"✓ Worker {self.worker_index}: Redis sink connected")
        except redis.ConnectionError as e:
            print(f"✗ Worker {self.worker_index}: Failed to connect to Redis: {e}")
            raise
    
    def write_batch(self, items: List[tuple[str, str]]) -> None:
        """Write batch of predictions to Redis"""
        for key, value in items:
            try:
                # Write to Redis with TTL
                self.client.setex(
                    key,
                    self.config.predictions_ttl,
                    value
                )
                self.count += 1
                
                if self.count % 10 == 0:
                    print(f"[Worker {self.worker_index}] Stored {self.count} predictions in Redis")
            
            except Exception as e:
                print(f"✗ Worker {self.worker_index}: Error writing to Redis: {e}")
    
    def close(self) -> None:
        """Cleanup when sink is closed"""
        if self.client:
            self.client.close()
            print(f"✓ Worker {self.worker_index}: Redis sink closed. Stored {self.count} predictions")


class RedisETASink(DynamicSink):
    """Dynamic sink that writes ETA predictions to Redis"""
    
    def __init__(self, config: RedisConfig):
        self.config = config
    
    def build(self, step_id: str, worker_index: int, worker_count: int) -> StatelessSinkPartition:
        """Build partition for writing"""
        return RedisETASinkPartition(self.config, worker_index)


# ============================================================================
# Dataflow Definition
# ============================================================================

def build_flow():
    """
    Build and return the Bytewax dataflow for ETA prediction.
    
    Pipeline:
    1. Input: Poll vehicle positions from Redis
    2. Validate: Check required fields
    3. Enrich: Add upcoming stops from Redis cache
    4. Process: Run ETA estimation
    5. Format: Prepare for Redis storage
    6. Output: Store predictions in Redis with TTL
    """
    
    # Create configuration
    config = RedisConfig(
        host="localhost",
        port=6379,
        db=0,
        password=None,
        vehicle_key_pattern="vehicle:*",
        route_stops_key_prefix="route_stops:",
        predictions_key_prefix="processed:",
        poll_interval_ms=1000,
        predictions_ttl=300  # 5 minutes
    )
    
    # Initialize dataflow
    flow = Dataflow("eta-prediction-flow")
    
    # Step 1: Input from Redis (vehicle positions)
    redis_source = RedisVehicleSource(config)
    stream = op.input("redis-vehicles", flow, redis_source)
    
    # Step 2: Validate vehicle data
    stream = op.filter_map("validate", stream, validate_vehicle_data)
    
    # Step 3: Enrich with upcoming stops
    stream = op.filter_map(
        "enrich-stops",
        stream,
        lambda data: enrich_with_stops(data, config)
    )
    
    # Step 4: Process through ETA estimator
    stream = op.filter_map("estimate-eta", stream, process_eta)
    
    # Step 5: Format for Redis storage
    stream = op.map("format-redis", stream, format_for_redis)
    
    # Step 6: Output to Redis
    redis_sink = RedisETASink(config)
    op.output("redis-predictions", stream, redis_sink)
    
    return flow


# ============================================================================
# Main Entry Point
# ============================================================================

# Bytewax looks for a variable called 'flow' at module level
flow = build_flow()

if __name__ == "__main__":
    print("\n" + "="*70)
    print("ETA PREDICTION DATAFLOW")
    print("="*70)
    print("This flow:")
    print("  1. Reads vehicle positions from Redis (vehicle:*)")
    print("  2. Enriches with upcoming stops (route_stops:{route_id})")
    print("  3. Estimates ETAs using trained models")
    print("  4. Stores predictions in Redis (processed:*)")
    print("\nPrerequisites:")
    print("  - MQTT subscriber writing to Redis (vehicle:* keys)")
    print("  - Route stops cached in Redis (route_stops:* keys)")
    print("  - Trained models in models/trained/ directory")
    print("\nTo run:")
    print("  python -m bytewax.run bytewax_eta_flow")
    print("\nWith multiple workers:")
    print("  python -m bytewax.run bytewax_eta_flow -w 2")
    print("="*70 + "\n")