#!/usr/bin/env python3
"""
Setup script for Prefect ETA deployment.

This script:
1. Registers the EtaRuntimeSettings block type
2. Creates the eta-runtime-pool work pool
3. Creates the eta-runtime-default settings block
4. Deploys the eta-runtime flow

Run this after the Prefect server is up and before starting workers.
"""

import os
import sys
import time
import subprocess
from pathlib import Path

# Add the prefect directory to path for imports
PREFECT_DIR = Path(__file__).resolve().parent
REPO_ROOT = PREFECT_DIR.parent
sys.path.insert(0, str(PREFECT_DIR))
sys.path.insert(0, str(REPO_ROOT))


def wait_for_server(url: str, max_attempts: int = 30, delay: float = 2.0) -> bool:
    """Wait for Prefect server to be ready."""
    import httpx

    print(f"Waiting for Prefect server at {url}...")
    for attempt in range(max_attempts):
        try:
            response = httpx.get(f"{url}/health", timeout=5.0)
            if response.status_code == 200:
                print(f"Prefect server is ready (attempt {attempt + 1})")
                return True
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_attempts}: Server not ready ({e})")
        time.sleep(delay)

    print("Prefect server did not become ready in time")
    return False


def register_block_type():
    """Register the EtaRuntimeSettings block type."""
    print("\n=== Registering EtaRuntimeSettings block type ===")
    try:
        # Import after path setup
        from runtime_config import EtaRuntimeSettings

        # Registering is automatic when the block class is defined and used
        # But we can explicitly register it
        EtaRuntimeSettings.register_type_and_schema()
        print("Block type 'eta-runtime-settings' registered successfully")
        return True
    except Exception as e:
        print(f"Block type registration: {e}")
        # This might fail if already registered, which is fine
        return True


def create_work_pool(pool_name: str, pool_type: str = "process"):
    """Create a work pool if it doesn't exist."""
    print(f"\n=== Creating work pool '{pool_name}' ===")

    result = subprocess.run(
        ["prefect", "work-pool", "create", pool_name, "--type", pool_type],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(f"Work pool '{pool_name}' created successfully")
        return True
    elif "already exists" in result.stderr.lower() or "already exists" in result.stdout.lower():
        print(f"Work pool '{pool_name}' already exists")
        return True
    else:
        print(f"Failed to create work pool: {result.stderr or result.stdout}")
        return False


def create_settings_block(
    block_name: str,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    model_key: str | None = None,
):
    """Create the EtaRuntimeSettings block."""
    print(f"\n=== Creating settings block '{block_name}' ===")

    try:
        from runtime_config import EtaRuntimeSettings

        # Check if block already exists
        try:
            existing = EtaRuntimeSettings.load(block_name)
            print(f"Block '{block_name}' already exists, updating...")
        except ValueError:
            existing = None

        # Create/update the block
        block = EtaRuntimeSettings(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=0,
            poll_interval_seconds=1.0,
            model_key=model_key,
            predictions_ttl_seconds=300,
            max_stops_per_vehicle=5,
        )

        block.save(block_name, overwrite=True)
        print(f"Settings block '{block_name}' saved successfully")
        print(f"  Redis: {redis_host}:{redis_port}")
        print(f"  Model key: {model_key or '(auto-detect)'}")
        return True

    except Exception as e:
        print(f"Failed to create settings block: {e}")
        import traceback
        traceback.print_exc()
        return False


def deploy_flow():
    """Deploy the eta-runtime flow from prefect.yaml."""
    print("\n=== Deploying eta-runtime flow ===")

    result = subprocess.run(
        ["prefect", "deploy", "--name", "eta-runtime"],
        capture_output=True,
        text=True,
        cwd=str(PREFECT_DIR)
    )

    if result.returncode == 0:
        print("Flow 'eta-runtime' deployed successfully")
        print(result.stdout)
        return True
    else:
        print(f"Failed to deploy flow: {result.stderr or result.stdout}")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Setup Prefect ETA deployment")
    parser.add_argument(
        "--api-url",
        default=os.environ.get("PREFECT_API_URL", "http://localhost:4200/api"),
        help="Prefect API URL"
    )
    parser.add_argument(
        "--redis-host",
        default=os.environ.get("REDIS_HOST", "localhost"),
        help="Redis host for the settings block"
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.environ.get("REDIS_PORT", "6379")),
        help="Redis port for the settings block"
    )
    parser.add_argument(
        "--model-key",
        default=os.environ.get("MODEL_KEY"),
        help="Model key for the settings block"
    )
    parser.add_argument(
        "--skip-wait",
        action="store_true",
        help="Skip waiting for server"
    )
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="Skip flow deployment"
    )

    args = parser.parse_args()

    # Set the API URL for all Prefect operations
    os.environ["PREFECT_API_URL"] = args.api_url
    print(f"Using Prefect API at: {args.api_url}")

    # Wait for server
    if not args.skip_wait:
        api_base = args.api_url.replace("/api", "")
        if not wait_for_server(api_base):
            sys.exit(1)

    # Install httpx for the wait function (needed for block registration too)
    try:
        import httpx
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "httpx"], check=True)

    # Register block type
    if not register_block_type():
        print("Warning: Block type registration had issues, continuing...")

    # Create work pool
    if not create_work_pool("eta-runtime-pool"):
        sys.exit(1)

    # Create settings block
    if not create_settings_block(
        "eta-runtime-default",
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        model_key=args.model_key,
    ):
        sys.exit(1)

    # Deploy flow
    if not args.skip_deploy:
        if not deploy_flow():
            sys.exit(1)

    print("\n=== Setup complete! ===")
    print("You can now start a worker with:")
    print("  prefect worker start --pool eta-runtime-pool")
    print("\nThen trigger a flow run with:")
    print("  prefect deployment run 'prefect-eta-runtime/eta-runtime'")


if __name__ == "__main__":
    main()
