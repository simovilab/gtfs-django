# Prefect ETA Runtime - Setup Guide

This guide explains how to run the ETA prediction streaming runtime using Prefect on Docker from scratch.

## Overview

The **Streaming Runtime** flow continuously:
1. Polls Redis for vehicle position snapshots (`vehicle:*` keys)
2. Fetches route stop data (`route_stops:<route_id>` keys)
3. Runs ETA predictions using trained ML models
4. Writes predictions back to Redis (`predictions:<vehicle_id>` keys)

## Prerequisites

- **Docker** and **Docker Compose** installed
- The repository cloned to your machine
- Trained models in `models/trained/` directory (the repo should include these)

Verify Docker is running:
```bash
docker --version
docker compose version
```

## Quick Start

From the project root (`eta_prediction/`), run these commands in order:

```bash
# Step 1: Start infrastructure
docker compose -f docker-compose.prefect.yml up -d prefect-server redis

# Step 2: Run one-time setup
docker compose -f docker-compose.prefect.yml run --rm prefect-init

# Step 3: Start the worker
docker compose -f docker-compose.prefect.yml up -d eta-worker

# Step 4: Open Prefect UI
open http://localhost:4200  # macOS
# or visit http://localhost:4200 in your browser
```

## Detailed Steps

### Step 1: Start Prefect Server and Redis

```bash
docker compose -f docker-compose.prefect.yml up -d prefect-server redis
```

**What this does:**
- Starts the **Prefect Server** on port `4200` - this provides the API and web dashboard
- Starts **Redis** on port `6380` (external) / `6379` (internal) - this stores vehicle data and predictions

**Wait for services to be healthy:**
```bash
docker compose -f docker-compose.prefect.yml ps
```

You should see both services with `(healthy)` status. The Prefect server may take 30-60 seconds to become healthy.

### Step 2: Run One-Time Setup

```bash
docker compose -f docker-compose.prefect.yml run --rm prefect-init
```

**What this does:**
- **Registers the block type**: Creates the `EtaRuntimeSettings` custom block type in Prefect
- **Creates the work pool**: Sets up `eta-runtime-pool` where flow runs are queued
- **Creates the settings block**: Configures `eta-runtime-default` with Redis connection (`redis:6379` for Docker networking)
- **Deploys the flow**: Registers the `eta-runtime` deployment from `prefect.yaml`

**Expected output:**
```
=== Registering EtaRuntimeSettings block type ===
Block type 'eta-runtime-settings' registered successfully

=== Creating work pool 'eta-runtime-pool' ===
Work pool 'eta-runtime-pool' created successfully

=== Creating settings block 'eta-runtime-default' ===
Settings block 'eta-runtime-default' saved successfully
  Redis: redis:6379
  Model key: (auto-detect)

=== Deploying eta-runtime flow ===
Flow 'eta-runtime' deployed successfully

=== Setup complete! ===
```

This step only needs to be run once. Running it again is safe - it will update existing resources.

### Step 3: Start the Worker

```bash
docker compose -f docker-compose.prefect.yml up -d eta-worker
```

**What this does:**
- Starts a Prefect worker that polls `eta-runtime-pool` for flow runs
- When a flow run is scheduled, the worker executes it

**Verify the worker is running:**
```bash
docker compose -f docker-compose.prefect.yml logs eta-worker --tail 10
```

You should see:
```
Worker 'eta-worker-1' started!
```

### Step 4: Trigger a Flow Run

**Option A: From Prefect UI**
1. Open http://localhost:4200
2. Go to **Deployments** in the left sidebar
3. Click on **prefect-eta-runtime/eta-runtime**
4. Click **Run** → **Quick Run**

**Option B: From CLI**
```bash
docker compose -f docker-compose.prefect.yml exec eta-worker \
  prefect deployment run 'prefect-eta-runtime/eta-runtime'
```

**Option C: Run with limited iterations (for testing)**
```bash
docker compose -f docker-compose.prefect.yml exec eta-worker \
  prefect deployment run 'prefect-eta-runtime/eta-runtime' \
  --param iterations=5
```

By default, the flow runs forever (`iterations=0`). Use `--param iterations=N` to run N polling cycles then stop.

## Monitoring

### View Worker Logs
```bash
# Follow logs in real-time
docker compose -f docker-compose.prefect.yml logs -f eta-worker

# Last 50 lines
docker compose -f docker-compose.prefect.yml logs eta-worker --tail 50
```

### View All Service Logs
```bash
docker compose -f docker-compose.prefect.yml logs -f
```

### Prefect UI
- **Dashboard**: http://localhost:4200
- **Flow Runs**: See all running/completed/failed runs
- **Deployments**: Manage and trigger deployments
- **Work Pools**: Monitor worker health

### Check Redis Data
```bash
# List vehicle keys
docker compose -f docker-compose.prefect.yml exec redis redis-cli KEYS "vehicle:*"

# List prediction keys
docker compose -f docker-compose.prefect.yml exec redis redis-cli KEYS "predictions:*"

# View a specific prediction
docker compose -f docker-compose.prefect.yml exec redis redis-cli GET "predictions:<vehicle_id>"
```

## Stopping Services

### Stop the Worker Only
```bash
docker compose -f docker-compose.prefect.yml stop eta-worker
```

### Stop All Services
```bash
docker compose -f docker-compose.prefect.yml down
```

### Stop and Remove All Data (clean slate)
```bash
docker compose -f docker-compose.prefect.yml down -v
```

This removes:
- Prefect server state (deployments, flow runs, blocks)
- Redis data (vehicle snapshots, predictions)

You'll need to run the setup again after this.

## Ports Reference

| Service | Internal Port | External Port | URL |
|---------|--------------|---------------|-----|
| Prefect Server | 4200 | 4200 | http://localhost:4200 |
| Redis | 6379 | 6380 | `redis://localhost:6380` |

**Note**: The worker connects to Redis using the Docker internal hostname `redis:6379`. If connecting from your host machine (e.g., for testing), use `localhost:6380`.

## Troubleshooting

### "Work pool not found" error
Run the setup again:
```bash
docker compose -f docker-compose.prefect.yml run --rm prefect-init
```

### Worker not picking up runs
Check if the worker is connected:
```bash
docker compose -f docker-compose.prefect.yml logs eta-worker --tail 20
```

Restart the worker:
```bash
docker compose -f docker-compose.prefect.yml restart eta-worker
```

### "No vehicle snapshots found" in logs
This is normal if there's no vehicle data in Redis. The flow is working correctly - it's just waiting for data.

To add test data:
```bash
docker compose -f docker-compose.prefect.yml exec redis redis-cli SET "vehicle:test1" \
  '{"vehicle_id":"test1","lat":34.0522,"lon":-118.2437,"speed":25.0,"route":"171","timestamp":"2026-01-21T12:00:00Z","trip_id":"trip1"}'

docker compose -f docker-compose.prefect.yml exec redis redis-cli SET "route_stops:171" \
  '[{"stop_id":"stop1","stop_name":"First Stop","lat":34.0525,"lon":-118.2440,"sequence":1}]'
```

### Prefect server unhealthy
Wait longer (can take 60+ seconds on first start) or check logs:
```bash
docker compose -f docker-compose.prefect.yml logs prefect-server --tail 30
```

### Connection refused to Prefect API
Ensure the Prefect server is running:
```bash
curl http://localhost:4200/api/health
```

Should return `true`.

## Configuration

### Modify Runtime Settings
The flow uses settings from the `eta-runtime-default` block. To modify:

```bash
docker compose -f docker-compose.prefect.yml run --rm prefect-init \
  python /app/prefect/setup_prefect.py \
  --api-url=http://prefect-server:4200/api \
  --redis-host=redis \
  --redis-port=6379 \
  --model-key=<your-model-key>
```

Or edit via the Prefect UI:
1. Go to **Blocks** in the sidebar
2. Find `eta-runtime-default`
3. Edit and save

### Flow Parameters
When triggering a run, you can override these parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `iterations` | 0 (forever) | Number of polling cycles |
| `poll_interval_seconds` | 1.0 | Seconds between polls |
| `max_vehicle_batch` | None | Max vehicles per cycle |
| `max_stops_per_vehicle` | 5 | Stops sent to estimator |
| `predictions_ttl_seconds` | 300 | Redis TTL for predictions |

Example:
```bash
docker compose -f docker-compose.prefect.yml exec eta-worker \
  prefect deployment run 'prefect-eta-runtime/eta-runtime' \
  --param iterations=10 \
  --param poll_interval_seconds=2.0
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network                           │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Prefect    │    │    Redis     │    │  ETA Worker  │  │
│  │   Server     │◄───┤              │◄───┤              │  │
│  │   :4200      │    │    :6379     │    │  (polls for  │  │
│  │              │    │              │───►│   jobs)      │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         ▲                                       │          │
│         │                                       │          │
│         └───────────────────────────────────────┘          │
│              Worker reports status to server               │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
   http://localhost:4200  (Prefect UI)
```

## Files Reference

| File | Purpose |
|------|---------|
| `docker-compose.prefect.yml` | Docker services definition |
| `prefect/Dockerfile` | Worker container image |
| `prefect/setup_prefect.py` | One-time setup script |
| `prefect/prefect.yaml` | Deployment definitions |
| `prefect/prefect_eta_flow.py` | The streaming runtime flow |
| `prefect/runtime_config.py` | Configuration block and helpers |
