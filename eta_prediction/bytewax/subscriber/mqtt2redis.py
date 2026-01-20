#!/usr/bin/env python3
"""
MQTT to Redis Subscriber Bridge for ETA Prediction Pipeline
Subscribes to MQTT vehicle topics and stores enriched data in Redis.

Usage:
    pip install paho-mqtt redis
    python mqtt_to_redis_subscriber.py
"""

import json
import os
import time
import sys
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import redis

# ============================================================================
# Configuration (from environment variables with defaults)
# ============================================================================

# MQTT Configuration
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "admin")
MQTT_PASS = os.getenv("MQTT_PASS", "admin")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "transit/vehicles/bus/#")

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
REDIS_KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "vehicle:")
REDIS_TTL = int(os.getenv("REDIS_TTL", "300"))

# Optional: Publish to Redis channel for real-time subscribers
REDIS_PUBSUB_ENABLED = os.getenv("REDIS_PUBSUB_ENABLED", "false").lower() == "true"
REDIS_PUBSUB_CHANNEL = os.getenv("REDIS_PUBSUB_CHANNEL", "vehicle_updates")

# Data validation - ensure all required fields are present
REQUIRED_FIELDS = [
    'vehicle_id',
    'lat',
    'lon',
    'speed',
    'timestamp'
]

# Optional fields that enhance predictions (but won't reject messages if missing)
RECOMMENDED_FIELDS = [
    'route_id',
    'trip_id',
    'stop_id',
    'stop_lat',
    'stop_lon',
    'stop_sequence',
    'heading',
    'bearing'
]

# ============================================================================
# Redis Client
# ============================================================================

redis_client = None

def connect_redis():
    """Connect to Redis"""
    global redis_client
    
    try:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        
        # Test connection
        redis_client.ping()
        print(f"✓ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        return True
        
    except redis.ConnectionError as e:
        print(f"✗ Failed to connect to Redis: {e}")
        print(f"  Make sure Redis is running: docker-compose up -d redis")
        return False
    except Exception as e:
        print(f"✗ Redis connection error: {e}")
        return False

def validate_vehicle_data(data):
    """
    Validate that vehicle data contains required fields for ETA prediction.
    Returns (is_valid, missing_fields, missing_recommended)
    """
    missing_required = [field for field in REQUIRED_FIELDS if field not in data]
    missing_recommended = [field for field in RECOMMENDED_FIELDS if field not in data]
    
    is_valid = len(missing_required) == 0
    
    return is_valid, missing_required, missing_recommended

def enrich_vehicle_data(data):
    """
    Enrich vehicle data with additional metadata and transformations.
    """
    # Ensure consistent field naming
    if 'route' in data and 'route_id' not in data:
        data['route_id'] = data['route']
    
    # Add UTC timestamp if not present or normalize it
    if 'timestamp' in data:
        try:
            # Parse and normalize to ISO format with timezone
            ts = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            data['timestamp'] = ts.isoformat()
        except:
            # If parsing fails, use current time
            data['timestamp'] = datetime.now(timezone.utc).isoformat()
    else:
        data['timestamp'] = datetime.now(timezone.utc).isoformat()
    
    # Ensure heading and bearing are both present (some systems use one or the other)
    if 'heading' in data and 'bearing' not in data:
        data['bearing'] = data['heading']
    elif 'bearing' in data and 'heading' not in data:
        data['heading'] = data['bearing']
    
    # Add processing metadata
    data['_mqtt_received_at'] = datetime.now(timezone.utc).isoformat()
    data['_data_quality'] = 'complete' if all(f in data for f in RECOMMENDED_FIELDS) else 'partial'
    
    return data

def store_in_redis(vehicle_id, data):
    """Store vehicle data in Redis with enrichment"""
    try:
        # Validate data
        is_valid, missing_required, missing_recommended = validate_vehicle_data(data)
        
        if not is_valid:
            print(f"⚠️  Invalid data for {vehicle_id}: missing required fields {missing_required}")
            return False
        
        # Warn about missing recommended fields
        if missing_recommended:
            print(f"ℹ️  {vehicle_id} missing recommended fields: {missing_recommended}")
        
        # Enrich data
        enriched_data = enrich_vehicle_data(data)
        
        # Create Redis key
        key = f"{REDIS_KEY_PREFIX}{vehicle_id}"
        
        # Convert data to JSON string
        json_data = json.dumps(enriched_data)
        
        # Store with TTL (expires after REDIS_TTL seconds)
        redis_client.setex(key, REDIS_TTL, json_data)
        
        # Optional: Publish to Redis Pub/Sub channel for real-time subscribers
        if REDIS_PUBSUB_ENABLED:
            redis_client.publish(REDIS_PUBSUB_CHANNEL, json_data)
        
        return True
        
    except Exception as e:
        print(f"✗ Error storing data in Redis: {e}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================================
# MQTT Callbacks
# ============================================================================

message_count = 0
error_count = 0
warning_count = 0
start_time = None

def on_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT broker"""
    global start_time
    
    if rc == 0:
        print(f"✓ Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
        
        # Subscribe to topic
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"✓ Subscribed to topic: {MQTT_TOPIC}")
        print("\n" + "="*70)
        print("MQTT → Redis Bridge Active (ETA Prediction Pipeline)")
        print("="*70)
        print("Features:")
        print("  • Data validation (required & recommended fields)")
        print("  • Timestamp normalization")
        print("  • Field enrichment (heading/bearing consistency)")
        print("  • Quality scoring")
        print("="*70)
        print(f"Listening for messages... (press Ctrl+C to stop)\n")
        
        start_time = time.time()
        
    else:
        print(f"✗ Failed to connect to MQTT broker, return code: {rc}")
        print("  Return codes: 0=Success, 1=Protocol version, 2=Invalid client ID")
        print("  3=Server unavailable, 4=Bad credentials, 5=Not authorized")
        sys.exit(1)

def on_message(client, userdata, msg):
    """Callback when a message is received"""
    global message_count, error_count, warning_count
    
    try:
        # Parse the message
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        
        # Get vehicle ID
        vehicle_id = data.get('vehicle_id')
        
        if not vehicle_id:
            print(f"⚠️  Message missing vehicle_id: {msg.topic}")
            error_count += 1
            return
        
        # Add MQTT metadata
        data['_mqtt_topic'] = msg.topic
        
        # Store in Redis (with validation and enrichment)
        if store_in_redis(vehicle_id, data):
            message_count += 1
            
            # Detailed output for first few messages, then summary
            if message_count <= 5:
                print(f"✓ [{message_count}] Stored {vehicle_id}")
                print(f"    Route: {data.get('route_id', 'N/A')}")
                print(f"    Position: ({data.get('lat'):.4f}, {data.get('lon'):.4f})")
                print(f"    Speed: {data.get('speed')} km/h")
                print(f"    Next Stop: {data.get('stop_id', 'N/A')}")
                print(f"    Quality: {data.get('_data_quality', 'unknown')}")
            elif message_count % 10 == 0:
                elapsed = time.time() - start_time if start_time else 0
                rate = message_count / elapsed if elapsed > 0 else 0
                print(f"📊 [{message_count}] messages | "
                      f"{rate:.1f} msg/sec | "
                      f"{error_count} errors | "
                      f"{warning_count} warnings")
            else:
                # Compact output for routine messages
                quality_icon = "✓" if data.get('_data_quality') == 'complete' else "⚠"
                print(f"{quality_icon} [{message_count}] {vehicle_id} → "
                      f"{data.get('route_id', 'N/A'):10s} | "
                      f"Speed: {data.get('speed', 0):5.1f} km/h | "
                      f"Stop: {data.get('stop_id', 'N/A')}")
        else:
            error_count += 1
            
    except json.JSONDecodeError as e:
        print(f"✗ Failed to decode JSON from topic {msg.topic}: {e}")
        error_count += 1
    except Exception as e:
        print(f"✗ Error processing message: {e}")
        import traceback
        traceback.print_exc()
        error_count += 1

def on_disconnect(client, userdata, rc):
    """Callback when disconnected from MQTT broker"""
    if rc != 0:
        print(f"\n✗ Unexpected disconnection from MQTT broker (code: {rc})")
        print("  Attempting to reconnect...")

# ============================================================================
# Main Function
# ============================================================================

def print_config():
    """Print current configuration"""
    print("="*70)
    print("MQTT to Redis Subscriber - ETA Prediction Pipeline")
    print("="*70)
    print(f"MQTT Broker:     {MQTT_HOST}:{MQTT_PORT}")
    print(f"MQTT Topic:      {MQTT_TOPIC}")
    print(f"Redis Server:    {REDIS_HOST}:{REDIS_PORT}")
    print(f"Redis Key:       {REDIS_KEY_PREFIX}<vehicle_id>")
    print(f"Redis TTL:       {REDIS_TTL} seconds")
    print(f"Pub/Sub:         {'Enabled' if REDIS_PUBSUB_ENABLED else 'Disabled'}")
    if REDIS_PUBSUB_ENABLED:
        print(f"Pub/Sub Channel: {REDIS_PUBSUB_CHANNEL}")
    print()
    print("Required Fields: " + ", ".join(REQUIRED_FIELDS))
    print("Recommended:     " + ", ".join(RECOMMENDED_FIELDS))
    print("="*70)
    print()

def print_stats():
    """Print final statistics"""
    print("\n" + "="*70)
    print("FINAL STATISTICS")
    print("="*70)
    print(f"Total messages processed: {message_count}")
    print(f"Total errors:             {error_count}")
    print(f"Total warnings:           {warning_count}")
    
    if message_count > 0:
        success_rate = ((message_count / (message_count + error_count)) * 100) if (message_count + error_count) > 0 else 0
        print(f"Success rate:             {success_rate:.1f}%")
    
    if start_time:
        elapsed = time.time() - start_time
        rate = message_count / elapsed if elapsed > 0 else 0
        print(f"Average rate:             {rate:.2f} messages/second")
        print(f"Running time:             {elapsed:.1f} seconds")
    print("="*70)

def main():
    """Main function"""
    print_config()
    
    # Connect to Redis first
    print("Connecting to Redis...")
    if not connect_redis():
        print("\n✗ Cannot start without Redis connection")
        sys.exit(1)
    
    print()
    
    # Create MQTT client
    print("Setting up MQTT client...")
    mqtt_client = mqtt.Client(client_id="mqtt-redis-bridge-eta")
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    
    # Set up callbacks
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect
    
    try:
        # Connect to MQTT broker
        print(f"Connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT}...")
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        
        # Start the loop (blocking)
        mqtt_client.loop_forever()
        
    except KeyboardInterrupt:
        print_stats()
        
    except ConnectionRefusedError:
        print(f"\n✗ Connection refused to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
        print("  Make sure RabbitMQ is running: docker-compose up -d rabbitmq")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Cleanup
        mqtt_client.disconnect()
        if redis_client:
            redis_client.close()
        print("\n✓ Subscriber stopped cleanly\n")

if __name__ == "__main__":
    main()