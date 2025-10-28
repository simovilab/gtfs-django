# ETA Prediction Models

Complete modeling framework for transit ETA prediction using GTFS-RT data.

## Directory Structure

```
models/
├── README.md                          # This file
├── train_all_models.py               # Main training script
├── common/                           # Shared utilities
│   ├── data.py                       # Dataset loading and preprocessing
│   ├── keys.py                       # Model key generation
│   ├── metrics.py                    # Evaluation metrics
│   ├── registry.py                   # Model storage and retrieval
│   └── utils.py                      # Helper functions
├── evaluation/                       # Model evaluation tools
│   ├── leaderboard.py               # Model comparison
│   └── roll_validate.py             # Rolling window validation
├── historical_mean/                  # Historical mean baseline
│   ├── train.py                     
│   └── predict.py
├── polyreg_distance/                # Polynomial regression (distance)
│   ├── train.py
│   └── predict.py
├── polyreg_time/                    # Polynomial regression (time)
│   ├── train.py
│   └── predict.py
├── ewma/                            # Exponential weighted moving average
│   ├── train.py
│   └── predict.py
└── trained/                         # Saved models (created automatically)
    ├── {model_key}.pkl
    ├── {model_key}_meta.json
    └── registry.json
```

## Usage

### 1. Train All Baseline Models

```bash
# Train all baseline models on sample dataset
python train_all_models.py --dataset sample_dataset --mode baseline

# Train baseline + advanced configurations
python train_all_models.py --dataset sample_dataset --mode all

# Train without saving (dry run)
python train_all_models.py --dataset sample_dataset --no-save
```

### 2. Train Individual Models

```python
from models.polyreg_time.train import train_polyreg_time

# Train polynomial regression with time features
result = train_polyreg_time(
    dataset_name="sample_dataset",
    poly_degree=2,
    include_temporal=True,
    include_operational=True
)

print(f"Test MAE: {result['metrics']['test_mae_minutes']:.2f} minutes")
```

### 3. Make Predictions

```python
from models.polyreg_time.predict import predict_eta

# Single prediction
prediction = predict_eta(
    model_key=f'{MODEL_KEY}',
    distance_to_stop=1500.0,  # meters
    hour=8,
    is_peak_hour=True,
    current_speed_kmh=25.0
)

print(f"ETA: {prediction['eta_formatted']}")
```

### 4. Compare Models

```python
from models.evaluation.leaderboard import quick_compare

model_keys = [
    "historical_mean_...",
    "polyreg_distance_...",
    "polyreg_time_...",
    "ewma_..."
]

results = quick_compare(model_keys, "sample_dataset")
```

## Model Types

### 1. Historical Mean (`historical_mean/`)

**Description**: Baseline model using historical average ETAs grouped by route, stop, and time features.

**Example**:
```python
from models.historical_mean.train import train_historical_mean

result = train_historical_mean(
    group_by=['route_id', 'stop_sequence', 'hour', 'day_of_week']
)
```

---

### 2. Polynomial Regression - Distance (`polyreg_distance/`)

**Description**: Polynomial regression on distance to stop with optional route-specific models.

**Example**:
```python
from models.polyreg_distance.train import train_polyreg_distance

result = train_polyreg_distance(
    degree=2,
    route_specific=True
)
```

---

### 3. Polynomial Regression - Time (`polyreg_time/`)

**Description**: Enhanced polynomial regression combining distance with temporal and operational features.

**Example**:
```python
from models.polyreg_time.train import train_polyreg_time

result = train_polyreg_time(
    poly_degree=2,
    include_temporal=True,
    include_operational=True,
    include_weather=False
)
```

---

### 4. EWMA (`ewma/`)

**Description**: Exponentially weighted moving average that adapts predictions based on recent observations.

**Example**:
```python
from models.ewma.train import train_ewma

result = train_ewma(
    alpha=0.3,  # Higher = faster adaptation
    group_by=['route_id', 'stop_sequence', 'hour']
)
```

## Evaluation

### Metrics

All models are evaluated on:

- **MAE (Mean Absolute Error)**: Primary metric, in seconds and minutes
- **RMSE (Root Mean Squared Error)**: Penalizes large errors
- **R²**: Goodness of fit
- **Within Threshold**: % predictions within 60s, 120s, 300s
- **Bias**: Over/under-prediction tendency
- **Quantile Errors**: 50th, 90th, 95th percentile errors

### Rolling Window Validation

Test models on sequential time windows:

```python
from models.evaluation.roll_validate import quick_rolling_validate
from models.ewma.train import EWMAModel

results = quick_rolling_validate(
    model_class=EWMAModel,
    model_params={'alpha': 0.3},
    train_window_days=7
)
```

### Leaderboard

Compare multiple models:

```python
from models.evaluation.leaderboard import ModelLeaderboard

leaderboard = ModelLeaderboard()
df = leaderboard.compare_models(
    model_keys=['model1', 'model2', 'model3'],
    dataset_name="sample_dataset"
)
leaderboard.print_leaderboard(df)
```

## Model Registry

All trained models are stored in the registry with metadata:

```python
from models.common.registry import get_registry

registry = get_registry()

# List all models
models = registry.list_models()

# Load a model
model = registry.load_model("polyreg_time_...")

# Get metadata
metadata = registry.load_metadata("polyreg_time_...")

# Get best model by metric
best_key = registry.get_best_model(metric='test_mae_seconds')
```

## Feature Engineering Integration

Models use features from the `feature_engineering` module:

**Temporal features** (`temporal.py`):
- hour, day_of_week, is_weekend, is_holiday, is_peak_hour

**Spatial features** (`spatial.py`):
- distance_to_stop, bearing_to_stop, progress_on_segment

**Operational features** (`operational.py`):
- headway_seconds, avg_speed_last_10min, vehicles_on_route

**Weather features** (`weather.py`):
- temperature_c, precipitation_mm, wind_speed_kmh

See `feature_engineering/README.md` for details.

## Dataset Requirements

Models expect datasets built with `dataset_builder.py` script through the `build_eta_sample` Django command:

**Minimum Required Columns**:
- `trip_id`, `route_id`, `vehicle_id`, `stop_id`, `stop_sequence`
- `vp_ts`, `vp_lat`, `vp_lon`
- `stop_lat`, `stop_lon`, `distance_to_stop`
- `time_to_arrival_seconds` (target)

**Recommended Columns**:
- `hour`, `day_of_week`, `is_peak_hour`
- `headway_seconds`, `current_speed_kmh`


