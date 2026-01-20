#!/usr/bin/env python3
"""
Bytewax dataflow for low-latency ETA prediction processing.

LOW-LATENCY DESIGN:
- All data loaded from Redis cache (no database calls)
- In-memory shape caching per worker
- Shape loading happens in enrichment step before inference
- Estimator receives pre-loaded shapes for zero I/O during prediction

This flow:
1. Polls vehicle position data from Redis (where MQTT subscriber stores it)
2. Enriches with upcoming stops from Redis cache
3. Loads GTFS shapes from Redis cache with in-memory caching
4. Processes through estimate_stop_times() with pre-loaded shapes
5. Stores predictions back to Redis under "predictions:*" keys

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

# Import the ETA estimator with proper path resolution
import sys
import os

# Resolve paths relative to this file
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent

# Add project directories to Python path
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "eta_service"))
sys.path.insert(0, str(project_root / "feature_engineering"))
sys.path.insert(0, str(project_root / "models"))

from eta_service.estimator import estimate_stop_times

# Import ShapePolyline for shape processing
try:
    from feature_engineering.spatial import ShapePolyline
    SHAPE_SUPPORT = True
except ImportError:
    SHAPE_SUPPORT = False
    print("⚠️  ShapePolyline not available, shape-aware features disabled")


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
    route_shape_key_prefix: str = "route_shape:"  # Cache key for route shapes
    predictions_key_prefix: str = "predictions:"  # Output key prefix for predictions
    poll_interval_ms: int = 1000  # How often to poll Redis (milliseconds)
    predictions_ttl: int = 300  # TTL for prediction cache (5 minutes)


# ============================================================================
# Shape Loading & Caching (Zero Database Calls)
# ============================================================================

class ShapeCache:
    """
    In-memory LRU-style cache for loaded shapes.
    Persists across batches within a worker to avoid repeated Redis queries.
    """
    
    def __init__(self, max_size: int = 100):
        self.cache: Dict[str, Any] = {}
        self.access_order: List[str] = []
        self.max_size = max_size
        self.hits = 0
        self.misses = 0
    
    def get(self, route_id: str) -> Optional[Any]:
        """Get cached shape for route"""
        if route_id in self.cache:
            # Move to end (most recently used)
            self.access_order.remove(route_id)
            self.access_order.append(route_id)
            self.hits += 1
            return self.cache[route_id]
        
        self.misses += 1
        return None
    
    def set(self, route_id: str, shape: Any):
        """Cache shape for route with LRU eviction"""
        if route_id in self.cache:
            # Update existing
            self.access_order.remove(route_id)
            self.access_order.append(route_id)
            self.cache[route_id] = shape
        else:
            # Add new
            if len(self.cache) >= self.max_size:
                # Evict least recently used
                oldest = self.access_order.pop(0)
                del self.cache[oldest]
            
            self.cache[route_id] = shape
            self.access_order.append(route_id)
    
    def stats(self) -> dict:
        """Get cache statistics"""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            'size': len(self.cache),
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate_pct': round(hit_rate, 1)
        }


def load_shape_from_redis(redis_client, route_id: str, shape_cache: ShapeCache, key_prefix: str = "route_shape:") -> Optional[Any]:
    """
    Load shape data from Redis and convert to ShapePolyline.
    Uses in-memory cache to avoid repeated Redis queries.
    
    Args:
        redis_client: Redis client instance
        route_id: Route identifier
        shape_cache: ShapeCache instance for this worker
        key_prefix: Redis key prefix
    
    Returns:
        ShapePolyline object or None if not available
    """
    if not SHAPE_SUPPORT:
        return None
    
    # Check in-memory cache first (fastest path)
    cached = shape_cache.get(route_id)
    if cached is not None:
        return cached
    
    # Load from Redis
    try:
        shape_key = f"{key_prefix}{route_id}"
        shape_json = redis_client.get(shape_key)
        
        if shape_json:
            shape_data = json.loads(shape_json)
            points = [
                (pt["shape_pt_lat"], pt["shape_pt_lon"])
                for pt in shape_data["points"]
            ]
            
            shape = ShapePolyline(points)
            shape_cache.set(route_id, shape)
            
            return shape
    except Exception as e:
        print(f"⚠️  Error loading shape from Redis for route {route_id}: {e}")
    
    return None


# ============================================================================
# Redis Input Source
# ============================================================================

class RedisVehiclePartition(StatefulSourcePartition):
    """Partition that polls vehicle position data from Redis"""
    
    def __init__(self, redis_config: RedisConfig):
        self.redis_config = redis_config
        self.redis_client = None
        self.shape_cache = ShapeCache(max_size=100)  # One cache per worker
        self._setup_client()
    
    def _setup_client(self):
        """Initialize Redis client"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_config.host,
                port=self.redis_config.port,
                db=self.redis_config.db,
                password=self.redis_config.password,
                decode_responses=True
            )
            self.redis_client.ping()
            print(f"✓ Connected to Redis at {self.redis_config.host}:{self.redis_config.port}")
            print(f"✓ Polling pattern: {self.redis_config.vehicle_key_pattern}")
            print(f"✓ Shape cache enabled with max_size=100\n")
        except redis.ConnectionError as e:
            print(f"✗ Failed to connect to Redis: {e}")
            raise
    
    def next_batch(self):
        """Get next batch of vehicle updates from Redis"""
        batch = []
        
        try:
            keys = self.redis_client.keys(self.redis_config.vehicle_key_pattern)
            
            for key in keys:
                try:
                    value = self.redis_client.get(key)
                    
                    if value:
                        data = json.loads(value)
                        data['_redis_key'] = key
                        data['_fetched_at'] = datetime.now(timezone.utc).isoformat()
                        data['_redis_client'] = self.redis_client
                        data['_redis_config'] = self.redis_config
                        data['_shape_cache'] = self.shape_cache  # Pass cache to downstream
                        
                        ttl = self.redis_client.ttl(key)
                        if ttl > 0:
                            data['_ttl_seconds'] = ttl
                        
                        batch.append(data)
                
                except json.JSONDecodeError as e:
                    print(f"⚠️  Failed to decode JSON from key {key}: {e}")
                except Exception as e:
                    print(f"⚠️  Error processing key {key}: {e}")
            
            if batch:
                cache_stats = self.shape_cache.stats()
                print(f"📦 Fetched {len(batch)} vehicle records | "
                      f"Shape cache: {cache_stats['hit_rate_pct']}% hit rate "
                      f"({cache_stats['hits']}/{cache_stats['hits'] + cache_stats['misses']})")
        
        except Exception as e:
            print(f"✗ Error in next_batch: {e}")
        
        return batch
    
    def next_awake(self):
        """Return when to wake up next (must be timezone-aware)"""
        return datetime.now(timezone.utc) + timedelta(milliseconds=self.redis_config.poll_interval_ms)

    def snapshot(self):
        """Save state for recovery"""
        return None
    
    def close(self):
        """Cleanup when source is closed"""
        if self.redis_client:
            cache_stats = self.shape_cache.stats()
            print(f"\n✓ Worker closing. Final shape cache stats: {cache_stats}")
            self.redis_client.close()
            print("✓ Disconnected from Redis")


class RedisVehicleSource(FixedPartitionedSource):
    """Source that creates Redis polling partitions"""
    
    def __init__(self, redis_config: RedisConfig):
        self.redis_config = redis_config
    
    def list_parts(self):
        """List available partitions"""
        return ["redis-vehicles-0"]
    
    def build_part(self, step_id, for_part, resume_state):
        """Build partition for reading"""
        return RedisVehiclePartition(self.redis_config)


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


def enrich_with_stops_and_shape(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Enrich vehicle data with upcoming stops and pre-loaded shape from Redis cache.
    
    ZERO DATABASE CALLS - All data from Redis cache or in-memory cache.
    
    Shape loading priority:
    1. In-memory cache (zero latency)
    2. Redis cache (low latency)
    3. Mock data fallback
    
    Expected Redis cache structure:
    
    Stops - Key: "route_stops:{route_id}"
    Value: JSON array of stops with fields: stop_id, stop_sequence, lat, lon
    
    Shapes - Key: "route_shape:{route_id}"
    Value: JSON object with shape_id and points array
    """
    route_id = data.get('route')
    
    if not route_id:
        print(f"⚠️  Vehicle {data.get('vehicle_id')} missing route_id")
        return None
    
    redis_client = data.get('_redis_client')
    redis_config = data.get('_redis_config')
    shape_cache = data.get('_shape_cache')
    
    if not redis_client or not redis_config or not shape_cache:
        print(f"⚠️  Missing Redis client/config/cache for vehicle {data.get('vehicle_id')}")
        return None
    
    try:
        # Load stops from Redis
        stops_key = f"{redis_config.route_stops_key_prefix}{route_id}"
        stops_json = redis_client.get(stops_key)
        upcoming_stops = None

        if stops_json:
            upcoming_stops = json.loads(stops_json)
        
        if not upcoming_stops or not isinstance(upcoming_stops, list):
            print(f"⚠️  No stops available for route {route_id}")
            return None
        
        data['upcoming_stops'] = upcoming_stops
        
        # Load shape from Redis/cache (best-effort, not required)
        shape = load_shape_from_redis(
            redis_client, 
            route_id, 
            shape_cache,
            redis_config.route_shape_key_prefix
        )
        
        # Store shape for inference
        data['_shape'] = shape
        data['_shape_available'] = shape is not None
        
        return data
    
    except json.JSONDecodeError as e:
        print(f"⚠️  Failed to decode JSON for route {route_id}: {e}")
        return None
    except Exception as e:
        print(f"⚠️  Error enriching with stops/shape: {e}")
        import traceback
        traceback.print_exc()
        return None


def process_eta(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process vehicle data through the ETA estimator with pre-loaded shape.
    
    ZERO I/O DURING INFERENCE - Shape already loaded in enrichment step.
    
    Input: Vehicle position data enriched with upcoming stops and optional shape
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
        
        # if vehicle_position['vehicle_id'] == 'BUS-001':
        #     print("VEHICLE DATA: \n\n", vehicle_position, "\n")
        #     if upcoming_stops:
        #         print("UPCOMING STOPS: ", upcoming_stops)    


        if not upcoming_stops:
            print(f"⚠️  No upcoming stops for vehicle {data['vehicle_id']}")
            return None
        
        # Get pre-loaded shape (or None)
        shape = data.get('_shape')
        trip_id = data.get('trip_id')
        
        # Call estimator with pre-loaded shape (zero I/O!)
        result = estimate_stop_times(
            vehicle_position=vehicle_position,
            upcoming_stops=upcoming_stops,
            route_id=data['route'],
            trip_id=trip_id,
            max_stops=5,
            #model_key="xgboost_various_dataset_5_spatial-temporal_global_20251202_063133_handle_nan=drop_learning_rate=0.05_max_depth=5_n_estimators=200",
            shape=shape  # Pre-loaded shape, no database calls
        )
        
        # Add original vehicle data for context
        result['_original_vehicle_data'] = {
            'vehicle_id': data['vehicle_id'],
            'route': data['route'],
            'lat': data['lat'],
            'lon': data['lon'],
            'speed': data['speed'],
            'timestamp': data['timestamp'],
            'trip_id': trip_id
        }
        
        shape_status = "with shape" if result.get('shape_used') else "without shape"
        print(f"✓ Processed ETA for vehicle {data['vehicle_id']} on route {data['route']} {shape_status}: "
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
    Key format: "predictions:{vehicle_id}"
    Value: JSON string of prediction result
    """
    vehicle_id = result.get('vehicle_id', 'unknown')
    key = f"predictions:{vehicle_id}"
    
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
        self.shape_count = 0
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
                
                # Track shape usage
                try:
                    result = json.loads(value)
                    if result.get('shape_used'):
                        self.shape_count += 1
                except:
                    pass
                
                if self.count % 10 == 0:
                    shape_pct = (self.shape_count / self.count * 100) if self.count > 0 else 0
                    print(f"[Worker {self.worker_index}] Stored {self.count} predictions "
                          f"({self.shape_count} with shapes, {shape_pct:.1f}%)")
            
            except Exception as e:
                print(f"✗ Worker {self.worker_index}: Error writing to Redis: {e}")
    
    def close(self) -> None:
        """Cleanup when sink is closed"""
        if self.client:
            self.client.close()
            shape_pct = (self.shape_count / self.count * 100) if self.count > 0 else 0
            print(f"✓ Worker {self.worker_index}: Redis sink closed.")
            print(f"  Total predictions: {self.count}")
            print(f"  With shapes: {self.shape_count} ({shape_pct:.1f}%)")


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
    Build and return the Bytewax dataflow for low-latency ETA prediction.
    
    LOW-LATENCY DESIGN:
    - All data from Redis cache (no database calls)
    - In-memory shape caching per worker
    - Pre-loaded shapes passed to estimator
    - Zero I/O during inference
    
    Pipeline:
    1. Input: Poll vehicle positions from Redis
    2. Validate: Check required fields
    3. Enrich: Load stops and shapes from Redis/cache
    4. Process: Run ETA estimation with pre-loaded shapes
    5. Format: Prepare for Redis storage
    6. Output: Store predictions in Redis with TTL
    """
    
    # Create configuration
    redis_config = RedisConfig(
        host="localhost",
        port=6379,
        db=0,
        password=None,
        vehicle_key_pattern="vehicle:*",
        route_stops_key_prefix="route_stops:",
        route_shape_key_prefix="route_shape:",
        predictions_key_prefix="predictions:",
        poll_interval_ms=1000,
        predictions_ttl=300  # 5 minutes
    )
    
    # Initialize dataflow
    flow = Dataflow("eta-prediction-flow")
    
    # Step 1: Input from Redis (vehicle positions)
    redis_source = RedisVehicleSource(redis_config)
    stream = op.input("redis-vehicles", flow, redis_source)
    
    # Step 2: Validate vehicle data
    stream = op.filter_map("validate", stream, validate_vehicle_data)
    
    # Step 3: Enrich with stops and pre-load shapes from Redis/cache
    stream = op.filter_map("enrich-stops-shape", stream, enrich_with_stops_and_shape)
    
    # Step 4: Process through ETA estimator (zero I/O - shapes pre-loaded!)
    stream = op.filter_map("estimate-eta", stream, process_eta)
    
    # Step 5: Format for Redis storage
    stream = op.map("format-redis", stream, format_for_redis)
    
    # Step 6: Output to Redis
    redis_sink = RedisETASink(redis_config)
    op.output("redis-predictions", stream, redis_sink)
    
    return flow


# ============================================================================
# Main Entry Point
# ============================================================================

# Bytewax looks for a variable called 'flow' at module level
flow = build_flow()

if __name__ == "__main__":
    print("\n" + "="*70)
    print("LOW-LATENCY ETA PREDICTION DATAFLOW")
    print("="*70)
    print("Architecture:")
    print("  • Zero database calls during inference")
    print("  • All data from Redis cache (stops, shapes)")
    print("  • In-memory LRU shape caching per worker")
    print("  • Pre-loaded shapes passed to estimator")
    print("\nThis flow:")
    print("  1. Reads vehicle positions from Redis (vehicle:*)")
    print("  2. Enriches with stops from Redis (route_stops:{route_id})")
    print("  3. Loads shapes from Redis with caching (route_shape:{route_id})")
    print("  4. Estimates ETAs with pre-loaded shapes (zero I/O)")
    print("  5. Stores predictions in Redis (predictions:*)")
    print("\nShape Loading (Low-Latency Priority):")
    print("  1. In-memory worker cache (zero latency)")
    print("  2. Redis cache (low latency)")
    print("  3. Mock data fallback (testing)")
    print("\nPerformance Features:")
    print("  • LRU cache with 100 shape limit per worker")
    print("  • Cache hit rate monitoring")
    print("  • Shape-aware spatial features when available")
    print("  • Graceful fallback to haversine distance")
    print("\nPrerequisites:")
    print("  - MQTT subscriber writing to Redis (vehicle:* keys)")
    print("  - Route stops cached in Redis (route_stops:* keys)")
    print("  - Route shapes cached in Redis (route_shape:* keys)")
    print("  - Trained models in models/trained/ directory")
    print("\nSetup:")
    print("  1. Load cache: python mock_route_stops.py")
    print("  2. Start flow: python -m bytewax.run bytewax_eta_flow")
    print("  3. Multiple workers: python -m bytewax.run bytewax_eta_flow -w 4")
    print("\nMonitoring:")
    print("  • Watch cache hit rates in logs")
    print("  • Track shape usage percentage")
    print("  • Monitor prediction throughput")
    print("="*70 + "\n")