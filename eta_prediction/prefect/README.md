# Prefect Streaming Runtime

A minimal Prefect-based replacement for the Bytewax `pred2redis.py` flow. It
keeps polling Redis for `vehicle:*` snapshots, fetches cached
`route_stops:<route_id>` blobs, runs `eta_service.estimator.estimate_stop_times`,
and stores the response under `predictions:<vehicle_id>`.

## Layout
- `prefect_eta_flow.py` – Prefect flow/tasks + CLI entrypoint
- `runtime_config.py` – Prefect Block + helper used to hydrate runtime parameters
- `runtime_deployment.py` – helper CLI for managing Prefect blocks/deployments
- `Dockerfile` – container image that bundles Prefect + runtime dependencies
- `pyproject.toml` – standalone environment (`prefect`, `redis`) so you can run
  `uv` directly inside this folder

## Running the Flow
```bash
cd eta_prediction/prefect
uv sync  # installs Prefect + redis bindings
uv run python prefect_eta_flow.py \
  --runtime-block eta-runtime/dev \
  --iterations 1 \
  --poll-interval 1.0
```

Key flags:
- `--iterations 0` (default) keeps the loop running forever; use `--iterations 1`
  for a single poll – handy for tests
- `--max-batch N` limits how many `vehicle:*` keys are processed each cycle
- `--max-stops N` caps how many upcoming stops are sent to the estimator
- `--model-key` selects the model registry entry (defaults to the global xgboost model)
- `--runtime-block` loads redis/model defaults from a Prefect `EtaRuntimeSettings` block
- `--notification-block` (repeatable) wires Slack/Webhook blocks for failure + zero-prediction alerts
- `--profiling-artifact-key-prefix` customizes the key that appears in the Prefect Artifacts UI

## Flow steps (mirrors Bytewax)
1. `fetch_vehicle_snapshots` task scans `vehicle:*`, decodes JSON, and performs
   the same field validation the Bytewax flow did.
2. `predict_for_snapshots` keeps a tiny in-memory cache of
   `route_stops:<route_id>` blobs, sends the top N stops plus the vehicle payload
   to `estimate_stop_times`, and annotates metadata (`source=prefect`, timestamps).
   Idempotency is enforced here by skipping any snapshot whose `timestamp`
   already matches the stored prediction.
3. `write_predictions_to_redis` writes each result to
   `predictions:<vehicle_id>` with the configurable TTL (default 5 min).

Because this version is pure Python you can run it anywhere Prefect can run,
without caring about Bytewax workers or CLI tooling.

## Profiling output
Each run resets simple profilers and writes CSV summaries into
`prefect/profiling/`:

- `redis_fetch_times.csv` – time spent (ms) fetching each `vehicle:*` key
- `eta_inference_times.csv` – duration (ms) of every `estimate_stop_times()` call
- `redis_write_times.csv` – persistence latency (ms) for each prediction write
- `pipeline_latency_times.csv` – end-to-end latency (ms) from Redis read through Redis write

Each CSV includes overall min/max/avg plus 10 frequency bins so you can monitor
the runtime without re-running ad-hoc timers. Starting with this iteration, the
CSV contents are also published as Prefect Markdown artifacts (key prefix
`eta-runtime-*` by default) so you can inspect the data directly from the Prefect UI.

## Runtime configuration with Prefect Blocks
- `runtime_config.EtaRuntimeSettings` is a Prefect Block that stores Redis host,
  port, DB, polling interval, model key, TTLs, notification block names, and the
  preferred profiling artifact prefix. Runtime code merges any flow parameters on
  top of the block so deployments stay environment agnostic.
- `runtime_deployment.py save-block` is a helper CLI you can run after `uv sync`
  to materialize the block:
  ```bash
  uv run python runtime_deployment.py save-block \
    --block-name eta-runtime/dev \
    --redis-host redis \
    --redis-port 6379 \
    --redis-password-secret redis/primary \
    --model-key xgboost_various_dataset_5_spatial-temporal_global_20251202_063133_handle_nan=drop_learning_rate=0.05_max_depth=5_n_estimators=200 \
    --notification-block slack/eta-alerts \
    --profiling-artifact-key-prefix eta-runtime-dev
  ```
  The command can be re-run with `--overwrite` any time you need to adjust
  defaults. Secrets/notifications referenced in the block are standard Prefect
  blocks (e.g., `prefect block register -m prefect.blocks.system` for `Secret`
  + `EnvironmentVariables`, `prefect block register -m prefect.blocks.notifications`
  for Slack/webhook blocks).

## Containerized runtime
You can run everything (block registration, flow tests, Prefect agent) from a
single Docker image so teammates do not need to recreate Python environments:

```bash
# Build once after cloning
docker build -t eta-prefect -f prefect/Dockerfile .

# Register the custom Prefect Blocks (PYTHONPATH is already set inside the image)
docker run --rm -it \
  -v $(pwd):/app \
  eta-prefect \
  prefect block register -m runtime_config

# Save/update the runtime defaults inside Prefect
docker run --rm -it \
  --network host \  # or set REDIS_HOST=host.docker.internal on macOS/Windows
  -v $(pwd):/app \
  -e PREFECT_API_URL=${PREFECT_API_URL:-http://127.0.0.1:4200/api} \
  eta-prefect \
  python prefect/runtime_deployment.py save-block \
    --block-name eta-runtime/dev \
    --redis-host redis \
    --redis-port 6379 \
    --notification-block slack/eta-alerts \
    --profiling-artifact-key-prefix eta-runtime-dev \
    --overwrite

# Run the streaming loop inside the container
docker run --rm -it \
  --network host \
  -v $(pwd):/app \
  -e PREFECT_API_URL=${PREFECT_API_URL:-http://127.0.0.1:4200/api} \
  eta-prefect \
  python prefect/prefect_eta_flow.py \
    --runtime-block eta-runtime/dev \
    --iterations 1
```

To run a Prefect agent in Docker, point it at your work pool:
```bash
docker run --rm -it \
  --network host \
  -v $(pwd):/app \
  -e PREFECT_API_URL=${PREFECT_API_URL:-http://127.0.0.1:4200/api} \
  eta-prefect \
  prefect agent start -p eta-runtime --name eta-runtime-container
```

The container ships with `MODEL_REGISTRY_DIR=/app/models/trained` and
`PYTHONPATH=/app` so local models + repo modules are always available. Mount the
repo (`-v $(pwd):/app`) so the runtime picks up your latest changes and model
artifacts.

## Deployment + agent workflow
1. Create runtime secrets and environment-variable blocks so agents stay
   stateless:
   ```bash
   prefect block create secret --name redis/primary --secret REDIS_PASSWORD=...
   prefect block create environment-variables --name env/runtime \
     --variables MODEL_REGISTRY_DIR=/opt/models/trained
   ```
2. Provision work pool + process agent:
   ```bash
   prefect work-pool create eta-runtime --type process
   prefect agent start -p eta-runtime --name eta-runtime-agent
   ```
   Agents inherit environment variables from the Pool configuration and can be
   inspected with `prefect work-pool inspect eta-runtime` or via the Prefect UI
   under *Work Pools* → *eta-runtime*.
3. Register the deployment so the work pool can pick up runs:
   ```bash
   uv run python runtime_deployment.py deploy \
     --block-name eta-runtime/dev \
     --work-pool eta-runtime \
     --env-block env/runtime \
     --tag streaming
   prefect deployment run 'prefect-eta-runtime/eta-runtime'
   ```
   `runtime_deployment.py deploy` wires the Process infrastructure (working dir,
   env vars) and attaches tags/parameters. You can confirm the deployment from
   `prefect deployment ls` or the UI.
4. Start/stop agents via `prefect agent start/stop` (systemd, Docker, k8s) and
   monitor health from the agent logs or the *Workers* tab for the work pool.
   Prefect notifications fire when the flow fails or when zero predictions are
   produced for `zero_prediction_alert_threshold` consecutive iterations.

### Troubleshooting
- Prefect 3.x currently requires Python 3.13 or lower. If uv created a 3.14
  virtualenv you'll see the "Timed out while attempting to connect to ephemeral
  Prefect API server" message. Fix by re-syncing the env after this repo update:
  ```bash
  cd eta_prediction/prefect
  uv sync --python 3.12  # or any installed 3.11/3.12/3.13 interpreter
  ```
  Then re-run `uv run python prefect_eta_flow.py ...`.
