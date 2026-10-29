#!/usr/bin/env python3
"""
Simple tester to verify ETA predictions are being stored in Redis.

This script:
1. Connects to Redis
2. Reads predictions under 'predictions:*' keys
3. Prints predictions as they appear / get updated

Usage:
    python test_redis_predictions.py                  # One-time snapshot
    python test_redis_predictions.py --continuous     # Keep monitoring
    python test_redis_predictions.py --vehicle bus_42 # Specific vehicle
"""

import json
import time
import argparse
from datetime import datetime
from typing import Dict, Any
import redis


class PredictionMonitor:
    """Monitor ETA predictions in Redis and print them."""

    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379, redis_db: int = 0):
        self.client = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            decode_responses=True,
        )
        # Track last-seen "version" per key so we also catch updates to existing keys
        self.last_seen: Dict[str, str] = {}

    def connect(self) -> bool:
        """Test Redis connection"""
        try:
            self.client.ping()
            print("✓ Connected to Redis")
            return True
        except redis.ConnectionError as e:
            print(f"✗ Failed to connect to Redis: {e}")
            return False

    def _print_prediction(self, key: str, data: Any):
        """Pretty-print a single prediction entry."""
        print("=" * 80)
        print(f"{datetime.now().isoformat()}  |  KEY: {key}")
        print("-" * 80)

        # If JSON-parsed dict, pretty-print it
        if isinstance(data, dict):
            print(json.dumps(data, indent=2, sort_keys=True))
        else:
            # Raw string
            print(data)

        print("=" * 80)
        print()

    def snapshot(self, pattern: str):
        """One-time snapshot of all predictions matching the pattern."""
        keys = self.client.keys(pattern)
        print(f"\n📊 Found {len(keys)} prediction keys in Redis (pattern: '{pattern}')\n")

        if not keys:
            print("No predictions found.")
            return

        for key in sorted(keys):
            value = self.client.get(key)
            if not value:
                continue

            try:
                data = json.loads(value)
            except json.JSONDecodeError:
                data = value  # print raw

            self._print_prediction(key, data)

    def monitor(self, pattern: str, interval: int = 2):
        """Continuously monitor Redis for new or updated predictions."""
        print(f"\n🔍 Monitoring Redis for predictions (pattern: '{pattern}')...")
        print("Press Ctrl+C to stop.\n")

        try:
            while True:
                keys = self.client.keys(pattern)

                for key in keys:
                    value = self.client.get(key)
                    if not value:
                        continue

                    # Try to parse JSON so we can use 'computed_at' as a version
                    computed_at = None
                    parsed = None
                    try:
                        parsed = json.loads(value)
                        if isinstance(parsed, dict):
                            computed_at = str(parsed.get("computed_at") or "")
                    except json.JSONDecodeError:
                        parsed = value

                    # Fallback: if no computed_at, use the raw string as version
                    version = computed_at or value

                    # Only print if we've never seen this version for this key
                    if self.last_seen.get(key) == version:
                        continue

                    self.last_seen[key] = version
                    self._print_prediction(key, parsed)

                time.sleep(interval)

        except KeyboardInterrupt:
            print("\n\n✓ Monitoring stopped")


def main():
    parser = argparse.ArgumentParser(
        description="Test ETA predictions stored in Redis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_redis_predictions.py                    # One-time snapshot
  python test_redis_predictions.py --continuous       # Keep monitoring
  python test_redis_predictions.py --vehicle bus_42   # Specific vehicle
  python test_redis_predictions.py -c -i 5            # Monitor every 5 seconds
        """,
    )

    parser.add_argument(
        "--continuous",
        "-c",
        action="store_true",
        help="Continuously monitor for predictions",
    )

    parser.add_argument(
        "--interval",
        "-i",
        type=int,
        default=2,
        help="Polling interval in seconds for continuous mode (default: 2)",
    )

    parser.add_argument(
        "--vehicle",
        "-v",
        type=str,
        help="Monitor specific vehicle ID (uses predictions:<vehicle_id> pattern)",
    )

    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Redis host (default: localhost)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=6379,
        help="Redis port (default: 6379)",
    )

    parser.add_argument(
        "--db",
        type=int,
        default=0,
        help="Redis database (default: 0)",
    )

    args = parser.parse_args()

    pattern = f"predictions:{args.vehicle}" if args.vehicle else "predictions:*"

    monitor = PredictionMonitor(
        redis_host=args.host,
        redis_port=args.port,
        redis_db=args.db,
    )

    if not monitor.connect():
        return 1

    if args.continuous:
        monitor.monitor(pattern=pattern, interval=args.interval)
    else:
        monitor.snapshot(pattern=pattern)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
