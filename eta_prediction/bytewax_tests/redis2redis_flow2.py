#!/usr/bin/env python3
"""
Bytewax dataflow for processing vehicle data from Redis.

This flow:
1. Polls vehicle position data from Redis (where MQTT subscriber stores it)
2. Processes vehicle location data
3. Sinks to stdout (easily replaceable)

Usage:
    pip install bytewax redis
    python -m bytewax.run bytewax_flow
"""

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass

import redis
import bytewax.operators as op
from bytewax.dataflow import Dataflow
# from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.connectors.stdio import StdOutSink

from typing import List, Any
from pathlib import Path

import bytewax.inputs as bw_inputs
import bytewax.outputs as bw_outputs


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
    key_pattern: str = "vehicle:*"  # Pattern to match vehicle keys
    poll_interval_ms: int = 100  # How often to poll Redis (milliseconds)


# ============================================================================
# Redis Input Source
# ============================================================================

class RedisPartition(bw_inputs.StatefulSourcePartition):
    """Partition that polls vehicle data from Redis"""
    
    def __init__(self, config: RedisConfig):
        self.config = config
        self.client = None
        self.seen_keys: Set[str] = set()  # Track keys we've already processed
        self._setup_client()
    
    def _setup_client(self):
        """Initialize Redis client and connect"""
        try:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True  # Automatically decode bytes to strings
            )
            # Test connection
            self.client.ping()
            print(f"✓ Connected to Redis at {self.config.host}:{self.config.port}")
            print(f"✓ Polling pattern: {self.config.key_pattern}\n")
        except redis.ConnectionError as e:
            print(f"✗ Failed to connect to Redis: {e}")
            raise
    
    def next_batch(self):
        """Get next batch of vehicle updates from Redis"""
        batch = []
        
        try:
            # Get all keys matching the pattern
            keys = self.client.keys(self.config.key_pattern)
            
            for key in keys:
                try:
                    # Get the value from Redis
                    value = self.client.get(key)
                    
                    if value:
                        # Parse JSON data
                        data = json.loads(value)
                        
                        # Add Redis metadata
                        data['_redis_key'] = key
                        data['_fetched_at'] = datetime.now(timezone.utc).isoformat()
                        
                        # Check TTL (time to live) if you want to know when key expires
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
        """Return when to wake up next"""
        # Wake up based on configured poll interval
        return datetime.now(timezone.utc) + timedelta(milliseconds=self.config.poll_interval_ms)
    
    def snapshot(self):
        """Save state for recovery"""
        return {"seen_keys": list(self.seen_keys)}
    
    def close(self):
        """Cleanup when source is closed"""
        if self.client:
            self.client.close()
            print("✓ Disconnected from Redis")


class RedisSource(bw_inputs.FixedPartitionedSource):
    """Source that creates Redis polling partitions"""
    
    def __init__(self, config: RedisConfig):
        self.config = config
    
    def list_parts(self):
        """List available partitions - just one for Redis polling"""
        return ["redis-0"]
    
    def build_part(self, step_id, for_part, resume_state):
        """Build partition for reading"""
        return RedisPartition(self.config)


# ============================================================================
# Alternative: Redis Pub/Sub Source (for real-time updates)
# ============================================================================

class RedisPubSubPartition(bw_inputs.StatefulSourcePartition):
    """
    Alternative partition that uses Redis Pub/Sub for real-time updates.
    This is useful if your MQTT subscriber publishes to a Redis channel.
    """
    
    def __init__(self, config: RedisConfig, channel: str = "vehicle_updates"):
        self.config = config
        self.channel = channel
        self.client = None
        self.pubsub = None
        self.message_queue = []
        self._setup_client()
    
    def _setup_client(self):
        """Initialize Redis Pub/Sub client"""
        try:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True
            )
            self.pubsub = self.client.pubsub()
            self.pubsub.subscribe(self.channel)
            print(f"✓ Subscribed to Redis channel: {self.channel}")
        except redis.ConnectionError as e:
            print(f"✗ Failed to connect to Redis Pub/Sub: {e}")
            raise
    
    def next_batch(self):
        """Get next batch of messages from Pub/Sub"""
        batch = []
        
        try:
            # Get messages (non-blocking)
            for _ in range(10):  # Process up to 10 messages per batch
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=0.01)
                if message and message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        data['_pubsub_channel'] = self.channel
                        data['_received_at'] = datetime.now(timezone.utc).isoformat()
                        batch.append(data)
                    except json.JSONDecodeError:
                        print(f"⚠️  Failed to decode message: {message['data']}")
                else:
                    break
        
        except Exception as e:
            print(f"✗ Error in next_batch (Pub/Sub): {e}")
        
        return batch
    
    def next_awake(self):
        """Return when to wake up next"""
        return datetime.now(timezone.utc) + timedelta(milliseconds=50)
    
    def snapshot(self):
        return None
    
    def close(self):
        if self.pubsub:
            self.pubsub.unsubscribe()
            self.pubsub.close()
        if self.client:
            self.client.close()


# ============================================================================
# Processing Functions
# ============================================================================

def parse_vehicle_data(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse and validate vehicle data.
    Returns None if data is invalid (will be filtered out).
    """
    required_fields = ['vehicle_id', 'lat', 'lon', 'speed', 'timestamp']
    
    # Validate required fields
    if not all(field in data for field in required_fields):
        print(f"⚠️  Invalid data: missing required fields")
        return None
    
    return data

import random

def enrich_vehicle_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich vehicle data with additional computed fields.
    """
    # Add processing timestamp
    data['processed_at'] = datetime.now(timezone.utc).isoformat()
    data['jae'] = "hi Jae #" + str(random.randint(1, 10))
    # Calculate speed category
    speed = data.get('speed', 0)
    if speed == 0:
        data['speed_category'] = 'stopped'
    elif speed < 20:
        data['speed_category'] = 'slow'
    elif speed < 40:
        data['speed_category'] = 'moderate'
    else:
        data['speed_category'] = 'fast'
    
    # Add occupancy level
    passengers = data.get('passengers', 0)
    if passengers < 10:
        data['occupancy'] = 'low'
    elif passengers < 30:
        data['occupancy'] = 'medium'
    else:
        data['occupancy'] = 'high'
    
    return data


def filter_moving_vehicles(data: Dict[str, Any]) -> bool:
    """
    Filter to only include moving vehicles (speed > 0).
    Return True to keep the item, False to filter it out.
    """
    return data.get('speed', 0) > 0


def format_for_output(data: Dict[str, Any]) -> str:
    """
    Format data for output (can be customized per sink).
    """
    # Create a formatted string for stdout
    output = f"""
{'='*70}
Vehicle: {data.get('vehicle_id', 'N/A')}
{'='*70}
Route:          {data.get('route', 'N/A')}
Location:       ({data.get('lat', 'N/A')}, {data.get('lon', 'N/A')})
Speed:          {data.get('speed', 'N/A')} km/h ({data.get('speed_category', 'N/A')})
Passengers:     {data.get('passengers', 'N/A')} ({data.get('occupancy', 'N/A')} occupancy)
Heading:        {data.get('heading', 'N/A')}°
Timestamp:      {data.get('timestamp', 'N/A')}
Processed:      {data.get('processed_at', 'N/A')}
Redis Key:      {data.get('_redis_key', 'N/A')}
{'='*70}
"""
    return output


# ============================================================================
# Alternative Sinks (Examples for easy swapping)
# ============================================================================

class RedisSinkPartition(bw_outputs.StatelessSinkPartition):
    def __init__(self, *, redis_host="localhost", redis_port=6379,
                 key_prefix="processed:", ttl_seconds=300,
                 publish_to_channel=False, channel_name="processed_updates"):
        self.client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.key_prefix = key_prefix
        self.ttl_seconds = ttl_seconds
        self.publish_to_channel = publish_to_channel
        self.channel_name = channel_name

    def write_batch(self, items):
        pipe = self.client.pipeline()
        for data in items:
            if not isinstance(data, dict):
                # Ignore formatted strings; Redis sink expects dicts
                continue
            vehicle_id = data.get("vehicle_id") or data.get("id")
            if not vehicle_id:
                continue
            key = f"{self.key_prefix}{vehicle_id}"
            payload = json.dumps(data)
            pipe.set(key, payload)
            if self.ttl_seconds:
                pipe.expire(key, self.ttl_seconds)
            if self.publish_to_channel:
                pipe.publish(self.channel_name, payload)
        pipe.execute()

    def close(self):
        try:
            self.client.close()
        except Exception:
            pass


class RedisSink(bw_outputs.DynamicSink):
    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def build(self, step_id, worker_index, worker_count):
        return RedisSinkPartition(**self._kwargs)

class FileSinkPartition(bw_outputs.StatelessSinkPartition[Any]):
    def __init__(self, filepath: str):
        self.filepath = filepath
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        self.file = open(filepath, "a", encoding="utf-8")
        self.count = 0
        print(f"✓ Writing to file: {filepath}")

    def write_batch(self, items: List[Any]) -> None:
        # items is a list; stringify each (JSON-ify yourself upstream if needed)
        for it in items:
            self.file.write(f"{it}\n")
            self.count += 1
        self.file.flush()

    def close(self) -> None:
        try:
            self.file.close()
        finally:
            print(f"✓ FileSink closed. Wrote {self.count} messages to {self.filepath}")




class FileSink(bw_outputs.DynamicSink[Any]):
    """One partition per worker; avoids cross-process file contention."""
    def __init__(self, basepath: str = "vehicle_data.log"):
        self.basepath = basepath

    def build(self, step_id: str, worker_index: int, worker_count: int) -> bw_outputs.StatelessSinkPartition[Any]:
        # Give each worker its own file to prevent interleaving
        if worker_count > 1:
            stem = Path(self.basepath).stem
            suf = Path(self.basepath).suffix or ".log"
            path = f"{stem}.w{worker_index}{suf}"
        else:
            path = self.basepath
        return FileSinkPartition(path)


# ============================================================================
# Dataflow Definition
# ============================================================================

def build_flow():
    """
    Build and return the Bytewax dataflow.
    
    Pipeline:
    1. Input: Poll from Redis (where MQTT subscriber stores vehicle data)
    2. Parse: Validate and parse vehicle data
    3. Filter: Remove invalid data (and optionally stopped vehicles)
    4. Enrich: Add computed fields
    5. Format: Prepare for output
    6. Output: Sink to destination (stdout/file/redis)
    """
    
    # Create configuration
    config = RedisConfig(
        host="localhost",
        port=6379,
        db=0,
        password=None,
        key_pattern="vehicle:*",  # Adjust this to match your Redis key pattern
        poll_interval_ms=1000  # Poll every second
    )
    
    # Initialize dataflow
    flow = Dataflow("vehicle-processing-redis")
    
    # Step 1: Input from Redis
    # Choose ONE of the following:
    
    # Option A: Poll Redis keys (default - gets all current vehicle positions)
    redis_source = RedisSource(config)
    stream = op.input("redis-input", flow, redis_source)
    
    # Option B: Use Redis Pub/Sub (uncomment if your MQTT subscriber publishes to a channel)
    # class RedisPubSubSource(FixedPartitionedSource):
    #     def __init__(self, config, channel):
    #         self.config = config
    #         self.channel = channel
    #     def list_parts(self):
    #         return ["pubsub-0"]
    #     def build_part(self, step_id, for_part, resume_state):
    #         return RedisPubSubPartition(self.config, self.channel)
    # 
    # pubsub_source = RedisPubSubSource(config, "vehicle_updates")
    # stream = op.input("redis-pubsub-input", flow, pubsub_source)
    
    # Step 2: Parse and validate (filter_map removes None values)
    stream = op.filter_map("parse", stream, parse_vehicle_data)
    
    # Step 3: (OPTIONAL) Filter moving vehicles only - uncomment to enable
    # stream = op.filter("filter-moving", stream, filter_moving_vehicles)
    
    # Step 4: Enrich with computed fields
    # stream = op.map("enrich", stream, enrich_vehicle_data)
    
    # Step 5: Format for output
    # stream = op.map("format", stream, format_for_output)
    
    # Step 6: Output - Choose ONE of the following:
    
    # Option 1: Stdout (default - good for testing)
    # op.output("stdout-output", stream, StdOutSink())
    
    # Option 2: File output (uncomment to use)
    # file_sink = FileSink("vehicle_data.log")
    # op.output("file-output", stream, file_sink)

    # Step 5 (A): formatted branch only for stdout
    formatted = op.map("format", stream, format_for_output)
    op.output("stdout-output", formatted, StdOutSink())

    # Enrich 
    stream = op.map("enrich", stream, enrich_vehicle_data)

    # Step 5 (B): raw dicts to Redis (no formatting)
    redis_sink = RedisSink(key_prefix="processed:", ttl_seconds=300, publish_to_channel=False)
    op.output("redis-output", stream, redis_sink)
    
    # Option 3: Redis output - write processed data back to Redis
    # redis_sink = RedisSink(key_prefix="processed:", ttl_seconds=300)
    # op.output("redis-output", stream, redis_sink)
    
    return flow


# ============================================================================
# Main Entry Point
# ============================================================================

# Bytewax looks for a variable called 'flow' at module level
flow = build_flow()

if __name__ == "__main__":
    print("Dataflow built successfully!")
    print("\n" + "="*70)
    print("REDIS POLLING MODE")
    print("="*70)
    print("This flow polls Redis for vehicle position data.")
    print("Make sure your MQTT subscriber is writing to Redis keys like 'vehicle:*'")
    print("\nTo run this flow, use:")
    print("  python -m bytewax.run bytewax_flow")
    print("\nOr with multiple workers:")
    print("  python -m bytewax.run bytewax_flow -w 2")
    print("="*70)