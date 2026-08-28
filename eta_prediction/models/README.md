# ETA Prediction Models

Comprehensive modeling stack that ingests feature-engineered GTFS-RT datasets, trains multiple ETA estimators, evaluates them with consistent metrics, and persists artifacts to a registry for online or batch inference.

---

## Architecture & Workflow

1. **Dataset management** – `common/data.py` loads `datasets/*.parquet`, performs cleaning, and produces temporal or route-based splits. All models rely on `ETADataset.clean_data`, `temporal_split`, and `route_split` to prevent leakage and keep outliers consistent.
2. **Training entry points** – Each model family exposes a `train_*` routine (e.g., `historical_mean/train.py`, `polyreg_time/train.py`, `xgb/train.py`) that handles filtering, splitting, fitting, and evaluation.
3. **Evaluation & metrics** – `common/metrics.py` computes the canonical metric suite (MAE/RMSE/R²/bias/coverage). `evaluation/leaderboard.py` and `evaluation/roll_validate.py` enable cross-model comparison and walk-forward validation over time.
4. **Registry** – `common/registry.py` persists `{model_key}.pkl` artifacts plus `{model_key}_meta.json` metadata while maintaining `trained/registry.json`. `common/keys.py` standardizes identifiers (dataset, feature groups, scope) for reproducibility.
5. **Inference** – Each model directory contains a `predict.py` that loads registry artifacts, validates required features, and formats outputs for downstream services.

---

## Shared Utilities (`models/common/`)

| Module | Responsibilities |
| --- | --- |
| `data.py` | Defines `ETADataset`, cleaning rules (drop missing targets, enforce distance thresholds), splits (`temporal_split`, `route_split`), and feature-selection helpers such as `prepare_features_target`. |
| `keys.py` | Generates descriptive model keys and experiment identifiers (dataset, feature groups, version, route scope) and parses keys to recover metadata when routing predictions. |
| `metrics.py` | Implements MAE/RMSE/MAPE/quantile/bias plus `compute_all_metrics`, segmentation tooling (`error_analysis`), and prediction intervals from residuals. |
| `registry.py` | Saves/loads pickled estimators + JSON metadata, lists/filter models, selects best models per metric/route, and handles deletion/cleanup. |
| `utils.py` | Logging setup, clipping/smoothing helpers, lag feature generators, formatted metric tables, and convenience functions like `train_test_summary`. |

---

## Model Families

### Historical Mean (`historical_mean/`)
- Groups ETAs by configurable dimensions (default `['route_id', 'stop_sequence', 'hour']`) and stores lookup tables with coverage metrics.
- Training pipeline filters datasets, performs temporal splits, and reports validation/test performance before saving to the registry.
- Prediction API reports whether the output was backed by historical data or fell back to the global mean, making it ideal for baseline comparisons and cold-start monitoring.

### Polynomial Regression – Distance (`polyreg_distance/`)
- Fits Ridge-regularized polynomial features on `distance_to_stop`. Supports route-specific models (`route_specific=True`) with an optional global fallback.
- Metadata records polynomial degree, regularization strength, and coefficient samples for transparency.
- Prediction helper exposes coefficients and supports batch inference with automatic route routing.

### Polynomial Regression – Time Enhanced (`polyreg_time/`)
- Extends distance regression with optional temporal (`hour`, `day_of_week`, `is_peak_hour`, etc.), spatial (segment progress + identifiers), and weather inputs.
- Uses `ColumnTransformer` + `PolynomialFeatures` for distance and `StandardScaler` for dense features, with configurable NaN strategies (`drop`, `impute`, or strict `error`).
- Provides coefficient-based feature importance summaries to highlight influential covariates per dataset/route.

### Exponentially Weighted Moving Average (`ewma/`)
- Maintains streaming EWMA statistics per `(route_id, stop_sequence [, hour])` key with configurable `alpha` and minimum observation thresholds.
- Offers online learning through `predict_and_update`, enabling real-time adaptation when ground truth becomes available.
- Ideal for highly non-stationary congestion patterns where recency outweighs historical aggregates.

### XGBoost Gradient Boosted Trees (`xgb/`)
- `XGBTimeModel` mirrors the feature-flag system from the time polynomial model (temporal/spatial toggles) but leverages `xgboost.XGBRegressor` for nonlinear interactions.
- Cleans datasets using the same missing-value audits, exposes feature importance from the trained booster, and supports hyper-parameter tuning (`max_depth`, `n_estimators`, `learning_rate`, subsampling knobs).
- Prediction API aligns with the polynomial time model so switching model keys requires no payload changes.

---

## Training Orchestration (`train_all_models.py`)

`python models/train_all_models.py --dataset sample_dataset [--by-route] [--models ...] [--no-save]`

- Loads the dataset once, optionally filtered by route, and delegates to each `train_*` routine. Default models: historical mean, distance polyreg, time polyreg, EWMA, and XGBoost.
- **Global mode** – trains one model per type and prints MAE/RMSE/R² summaries.
- **Route-specific mode** – iterates each route present in the dataset, trains the selected model types, and prints per-route performance plus correlations between trip volume and error.
- Pass `--no-save` for dry runs; otherwise results land in the registry with enriched metadata (sample counts, coverage, and configurations).

---

## Evaluation Toolkit (`models/evaluation/`)

- **Leaderboard (`leaderboard.py`)** – Loads trained models from the registry, evaluates each on a consistent test split (temporal or route-based), and prints a ranked table with MAE/RMSE/R², coverage, and bias. Use `quick_compare([...], dataset)` for one-liners.
- **Rolling validation (`roll_validate.py`)** – Implements walk-forward validation across sliding temporal windows to measure stability over time. Accepts custom `train_fn`/`predict_fn` callables so any model type can be stress-tested.
- **Plotting helpers** – Optional Matplotlib visualizations to inspect MAE drift, coverage trends, or metric distributions across windows.

---

## Model Registry & Keys

- `ModelKey.generate(...)` assembles identifiers of the form `polyreg_time_sample_temporal-spatial_global_20250126_143022_degree=2`. Keys capture dataset, feature groups, scope (global vs. `route_{id}`), timestamp, and supplemental hyper-parameters to simplify filtering and reproducibility.
- `ModelRegistry.save_model` writes `{model_key}.pkl` (pickled estimator) and `{model_key}_meta.json` (config + metrics) while updating `trained/registry.json`. Metadata contains dataset info, route scope, sample counts, evaluation results, and custom training attributes.
- Consumers can:
  ```python
  from models.common.registry import get_registry
  registry = get_registry()
  df = registry.list_models(model_type='polyreg_time')
  best_key = registry.get_best_model(metric='test_mae_seconds', route_id='global')
  model = registry.load_model(best_key)
  meta = registry.load_metadata(best_key)
  ```
- `check_registry.py` provides diagnostics (permissions, file counts, distribution of model types) and an optional save/load smoke test.

---

## Prediction Interfaces

Each model package ships with a `predict.py` tailored to its feature requirements:

| Module | Required Inputs | Notes |
| --- | --- | --- |
| `historical_mean.predict.predict_eta` | `route_id`, `stop_sequence`, `hour` (+ optional weekday/peak flags) | Returns coverage flag and fallback status. |
| `polyreg_distance.predict.predict_eta` | `distance_to_stop` (+ `route_id` for route-specific models) | Exposes polynomial coefficients for transparency. |
| `polyreg_time.predict.predict_eta` | `distance_to_stop` plus any temporal/spatial/weather signals enabled during training | `features_used` documents the expected payload. |
| `ewma.predict.predict_eta` | `route_id`, `stop_sequence` (+ optional `hour`) | Supports `predict_and_update` for online learning. |
| `xgb.predict.predict_eta` | Same schema as the time polynomial model | Uses XGBoost’s native handling for missing optional fields. |

Batch helpers (`batch_predict`) are available in every module when you need to score pandas DataFrames efficiently.

---

## Example Workflow

```python
from models.train_all_models import train_all_models
from models.common.registry import get_registry
from models.evaluation.leaderboard import quick_compare
from models.xgb.predict import predict_eta as predict_xgb

# 1. Train baselines on pre-built parquet
train_all_models(
    dataset_name="sample_dataset",
    by_route=False,
    model_types=["historical_mean", "polyreg_time", "xgboost"]
)

# 2. Inspect registry and select best global model
registry = get_registry()
best_key = registry.get_best_model(metric="test_mae_seconds", route_id="global")

# 3. Run inference
prediction = predict_xgb(
    model_key=best_key,
    distance_to_stop=1200,
    hour=8,
    is_peak_hour=True
)
print(prediction["eta_formatted"])

# 4. Compare candidates on a hold-out split
candidate_keys = registry.list_models().head(4)["model_key"].tolist()
leaderboard_df = quick_compare(candidate_keys, dataset_name="sample_dataset")
```

---

## Directory Structure

```
models/
├── common/                # Dataset + registry + metric utilities
├── evaluation/            # Leaderboard + rolling validation + plotting
├── historical_mean/       # Baseline mean model (train/predict)
├── polyreg_distance/      # Distance-only polynomial regression
├── polyreg_time/          # Distance + temporal/spatial regression
├── ewma/                  # Exponential smoothing model with online updates
├── xgb/                   # Gradient-boosted tree regressor
├── train_all_models.py    # CLI orchestrator
├── example_workflow.py    # Import barrel + quick-start helpers
├── check_registry.py      # Diagnostics for trained/ registry folder
└── trained/               # Auto-created artifacts (PKL + JSON + registry index)
```

Place datasets under `datasets/{name}.parquet`, run the feature-engineering builder beforehand, and execute the modeling scripts within the same Django/ORM environment as the ETA service so shared settings and caches are available.
