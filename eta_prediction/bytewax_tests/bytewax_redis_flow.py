"""
Bytewax dataflow for processing MQTT vehicle data.

This flow:
1. Polls messages from RabbitMQ/MQTT
2. Processes vehicle location data
3. Sinks to stdout (easily replaceable)

Usage:
    pip install bytewax paho-mqtt
    python -m bytewax.run bytewax_flow
"""

from typing import List, Any
from pathlib import Path


import json
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from queue import Queue, Empty

import paho.mqtt.client as mqtt
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import SimplePollingSource, FixedPartitionedSource, StatefulSourcePartition
from bytewax.connectors.stdio import StdOutSink
from bytewax.outputs import StatelessSinkPartition, DynamicSink

# ============================================================================
# Configuration
# ============================================================================

@dataclass
class MQTTConfig:
    """MQTT connection configuration"""
    host: str = "localhost"
    port: int = 1883
    username: str = "admin"
    password: str = "admin"
    topic: str = "transit/vehicles/bus/#"
    qos: int = 1


# ============================================================================
# MQTT Input Source
# ============================================================================

class MQTTPartition(StatefulSourcePartition):
    """Partition that reads from MQTT broker"""
    
    def __init__(self, config: MQTTConfig):
        self.config = config
        self.message_queue = Queue()
        self.client = None
        self._connected = False
        self._setup_client()
    
    def _setup_client(self):
        """Initialize MQTT client and connect"""
        self.client = mqtt.Client(client_id=f"bytewax-{id(self)}")
        self.client.username_pw_set(self.config.username, self.config.password)
        self.client.on_message = self._on_message
        self.client.on_connect = self._on_connect
        
        try:
            self.client.connect(self.config.host, self.config.port, 60)
            self.client.loop_start()
            print(f"✓ Connected to MQTT broker at {self.config.host}:{self.config.port}")
        except Exception as e:
            print(f"✗ Failed to connect to MQTT broker: {e}")
            raise
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connected to broker"""
        if rc == 0:
            self._connected = True
            client.subscribe(self.config.topic, qos=self.config.qos)
            print(f"✓ Subscribed to topic: {self.config.topic}\n")
        else:
            print(f"✗ Connection failed with code: {rc}")
    
    def _on_message(self, client, userdata, msg):
        """Callback when message received"""
        try:
            # payload = json.loads(msg.payload.decode('utf-8'))
            # # Add metadata
            # payload['_topic'] = msg.topic
            # payload['_received_at'] = datetime.utcnow().isoformat()

            payload = json.loads(msg.payload.decode("utf-8"))
            # Add metadata (use timezone-aware UTC)
            payload["_topic"] = msg.topic
            payload["_received_at"] = datetime.now(timezone.utc).isoformat()


            self.message_queue.put(payload)
        except Exception as e:
            print(f"✗ Error processing message: {e}")
    
    def next_batch(self):
        """Get next batch of messages"""
        batch = []
        try:
            # Try to get up to 10 messages without blocking
            for _ in range(10):
                try:
                    msg = self.message_queue.get_nowait()
                    batch.append(msg)
                except Empty:
                    break
        except Exception as e:
            print(f"✗ Error in next_batch: {e}")
        
        return batch
    
    # def next_awake(self):
    #     """Return when to wake up next"""
    #     # Wake up every 100ms to check for messages
    #     return datetime.now() + timedelta(milliseconds=100)
    
    def next_awake(self):
        """Return when to wake up next (must be timezone-aware)."""
        # Wake up every 100ms to check for messages
        return datetime.now(timezone.utc) + timedelta(milliseconds=100)

    def snapshot(self):
        """Save state for recovery - MQTT doesn't support replay"""
        return None
    
    def close(self):
        """Cleanup when source is closed"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            print("✓ Disconnected from MQTT broker")


class MQTTSource(FixedPartitionedSource):
    """Source that creates MQTT partitions"""
    
    def __init__(self, config: MQTTConfig):
        self.config = config
    
    def list_parts(self):
        """List available partitions - just one for MQTT"""
        return ["mqtt-0"]
    
    def build_part(self, step_id, for_part, resume_state):
        """Build partition for reading"""
        return MQTTPartition(self.config)


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


def enrich_vehicle_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich vehicle data with additional computed fields.
    """
    # Add processing timestamp
    data['processed_at'] = datetime.utcnow().isoformat()
    
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
{'='*70}
"""
    return output


# ============================================================================
# Alternative Sinks (Examples for easy swapping)
# ============================================================================

class RedisSink:
    """
    Example: Sink to Redis
    
    To use:
    1. pip install redis
    2. Uncomment the redis import and client initialization
    3. Change op.output() to use this sink
    """
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.count = 0
        print(f"✓ RedisSink initialized for {redis_host}:{redis_port}")
        # TODO: Uncomment to use Redis
        # import redis
        # self.client = redis.Redis(
        #     host=redis_host, 
        #     port=redis_port, 
        #     decode_responses=True
        # )
    
    def write(self, item: str):
        """Write to Redis"""
        self.count += 1
        # Example implementation:
        # data = json.loads(item) if isinstance(item, str) else item
        # vehicle_id = data.get('vehicle_id')
        # self.client.set(f"vehicle:{vehicle_id}", json.dumps(data))
        # self.client.expire(f"vehicle:{vehicle_id}", 300)  # 5 minute TTL
        print(f"[Redis #{self.count}] Would write: {item[:100]}...")  # Placeholder
    
    def close(self):
        print(f"✓ RedisSink closed. Processed {self.count} messages")



class FileSinkPartition(StatelessSinkPartition[Any]):
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




class FileSink(DynamicSink[Any]):
    """One partition per worker; avoids cross-process file contention."""
    def __init__(self, basepath: str = "vehicle_data.log"):
        self.basepath = basepath

    def build(self, step_id: str, worker_index: int, worker_count: int) -> StatelessSinkPartition[Any]:
        # Give each worker its own file to prevent interleaving
        if worker_count > 1:
            stem = Path(self.basepath).stem
            suf = Path(self.basepath).suffix or ".log"
            path = f"{stem}.w{worker_index}{suf}"
        else:
            path = self.basepath
        return FileSinkPartition(path)


# class FileSink(StatelessSink):
#     """Sink to file"""
    
#     def __init__(self, filepath: str):
#         self.filepath = filepath
#         self.file = open(filepath, 'a')
#         self.count = 0
#         print(f"✓ Writing to file: {filepath}")
    
#     def write_batch(self, item: str):
#         """Write to file"""
#         self.count += 1
#         self.file.write(item + "\n")
#         self.file.flush()
    
#     def close(self):
#         """Cleanup"""
#         if hasattr(self, 'file'):
#             self.file.close()
#             print(f"✓ FileSink closed. Wrote {self.count} messages to {self.filepath}")
    
#     def __del__(self):
#         """Cleanup on deletion"""
#         self.close()


# ============================================================================
# Dataflow Definition
# ============================================================================

def build_flow():
    """
    Build and return the Bytewax dataflow.
    
    Pipeline:
    1. Input: Read from MQTT
    2. Parse: Validate and parse vehicle data
    3. Filter: Remove invalid data (and optionally stopped vehicles)
    4. Enrich: Add computed fields
    5. Format: Prepare for output
    6. Output: Sink to destination (stdout/file/redis)
    """
    
    # Create configuration
    config = MQTTConfig(
        host="localhost",
        port=1883,
        username="admin",
        password="admin",
        topic="transit/vehicles/bus/#",
        qos=1
    )
    
    # Initialize dataflow
    flow = Dataflow("vehicle-processing")
    
    # Step 1: Input from MQTT
    mqtt_source = MQTTSource(config)
    stream = op.input("mqtt-input", flow, mqtt_source)
    
    # Step 2: Parse and validate (filter_map removes None values)
    stream = op.filter_map("parse", stream, parse_vehicle_data)
    
    # Step 3: (OPTIONAL) Filter moving vehicles only - uncomment to enable
    # stream = op.filter("filter-moving", stream, filter_moving_vehicles)
    
    # Step 4: Enrich with computed fields
    stream = op.map("enrich", stream, enrich_vehicle_data)
    
    # Step 5: Format for output
    stream = op.map("format", stream, format_for_output)
    
    # Step 6: Output - Choose ONE of the following:
    
    # Option 1: Stdout (default - good for testing)
    # op.output("stdout-output", stream, StdOutSink())
    
    # Option 2: File output (uncomment to use)
    file_sink = FileSink("vehicle_data.log")
    op.output("file-output", stream, file_sink)
    
    # Option 3: Redis output (implement Redis connection first)
    # redis_sink = RedisSink()
    # op.output("redis-output", stream, redis_sink)
    
    return flow


# ============================================================================
# Main Entry Point
# ============================================================================

# Bytewax looks for a variable called 'flow' at module level
flow = build_flow()

