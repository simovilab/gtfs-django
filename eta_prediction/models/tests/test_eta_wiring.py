"""Phase E — ETA suite wiring & validation.

Confirms the trained-model registry and dataset/feature contract still work
after the schema-alignment + S3 refactor (Phases A–D). The registry and
estimator are decoupled from the Django models, so these guard the remaining
coupling point: the dataset feature contract and the train->register->predict
path. Uses the committed ``datasets/sample.parquet`` as the canonical
builder-output fixture.
"""

from __future__ import annotations

from models.common.data import ETADataset, load_dataset, prepare_features_target
from models.common.registry import ModelRegistry
from models.historical_mean.train import HistoricalMeanModel

TARGET = "time_to_arrival_seconds"


def test_sample_dataset_satisfies_feature_contract():
    ds = load_dataset("sample")
    # Target present
    assert TARGET in ds.df.columns
    # Every model-consumed feature group is fully present in the builder output —
    # pins builder <-> ETADataset.FEATURE_GROUPS agreement.
    for group in ("position", "spatial", "kinematic", "status", "temporal"):
        expected = set(ETADataset.FEATURE_GROUPS[group])
        present = set(ds.get_features([group]))
        assert expected == present, f"{group} contract drift: missing {expected - present}"


def test_dropped_legacy_columns_absent():
    # progress_on_segment + weather were removed from the builder.
    ds = load_dataset("sample")
    for col in ("progress_on_segment", "temperature_c", "precipitation_mm", "wind_speed_kmh"):
        assert col not in ds.df.columns, f"legacy column {col} unexpectedly present"


def test_prepare_features_target_shapes():
    ds = load_dataset("sample")
    feats = ds.get_features(["temporal", "position", "spatial", "kinematic", "status"])
    X, y = prepare_features_target(ds.df, feats, target_col=TARGET)
    assert list(X.columns) == feats
    assert len(X) == len(y) == len(ds.df)
    assert X.isna().sum().sum() == 0  # fill_na default


def test_train_register_predict_roundtrip(tmp_path):
    ds = load_dataset("sample")
    df = ds.df

    model = HistoricalMeanModel(group_by=["route_id", "stop_sequence", "hour"])
    model.fit(df, target_col=TARGET)

    reg = ModelRegistry(base_dir=str(tmp_path))
    reg.save_model(
        model_key="smoke_hist_v0",
        model=model,
        metadata={"model_type": "historical_mean", "dataset": "sample", "route_id": None},
    )

    loaded = reg.load_model("smoke_hist_v0")
    preds = loaded.predict(df.head(10))
    assert len(preds) == 10
    assert reg.load_metadata("smoke_hist_v0")["model_type"] == "historical_mean"
    assert "smoke_hist_v0" in reg.list_models()["model_key"].values
