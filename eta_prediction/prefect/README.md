# Prefect Streaming Runtime

A minimal Prefect-based replacement for the Bytewax `pred2redis.py` flow. It
keeps polling Redis for `vehicle:*` snapshots, fetches cached
`route_stops:<route_id>` blobs, runs `eta_service.estimator.estimate_stop_times`,
and stores the response under `predictions:<vehicle_id>`.

## Layout
- `prefect_eta_flow.py` – Prefect flow/tasks + CLI entrypoint
- `pyproject.toml` – standalone environment (`prefect`, `redis`) so you can run
  `uv` directly inside this folder

## Running the Flow
```bash
cd eta_prediction/prefect
uv sync  # installs Prefect + redis bindings
uv run python prefect_eta_flow.py \
  --redis-host localhost \
  --redis-port 6379 \
  --poll-interval 1.0 \
  --max-batch 25 \
  --model-key xgboost_various_dataset_5_spatial-temporal_global_20251202_063133_handle_nan=drop_learning_rate=0.05_max_depth=5_n_estimators=200
```

Key flags:
- `--iterations 0` (default) keeps the loop running forever; use `--iterations 1`
  for a single poll – handy for tests
- `--max-batch N` limits how many `vehicle:*` keys are processed each cycle
- `--max-stops N` caps how many upcoming stops are sent to the estimator
- `--model-key` selects the model registry entry (defaults to the global xgboost model)

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
the runtime without re-running ad-hoc timers.

### Troubleshooting
- Prefect 3.x currently requires Python 3.13 or lower. If uv created a 3.14
  virtualenv you'll see the "Timed out while attempting to connect to ephemeral
  Prefect API server" message. Fix by re-syncing the env after this repo update:
  ```bash
  cd eta_prediction/prefect
  uv sync --python 3.12  # or any installed 3.11/3.12/3.13 interpreter
  ```
  Then re-run `uv run python prefect_eta_flow.py ...`.
