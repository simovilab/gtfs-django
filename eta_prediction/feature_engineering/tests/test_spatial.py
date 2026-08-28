# tests/test_spatial.py
import pytest

from feature_engineering.spatial import (
    calculate_distance_features_with_shape,
    ShapePolyline,
)


def approx_equal(a, b, tol):
    return abs(a - b) <= tol


def test_basic_distance_and_progress_without_shape():
    vp = {"lat": 0.0, "lon": 0.0}
    stop = {"stop_id": "A", "lat": 0.0, "lon": 1.0, "stop_order": 2}
    next_stop = {"stop_id": "B", "lat": 0.0, "lon": 2.0}

    feats = calculate_distance_features_with_shape(
        vp,
        stop,
        next_stop,
        shape=None,
        vehicle_stop_order=2,
        total_segments=10,
    )

    assert approx_equal(feats["distance_to_stop"], 111_320, 600)
    assert 0.0 <= feats["progress_on_segment"] <= 1.0
    assert 0.15 <= feats["progress_ratio"] <= 0.25  # 2/10 with some within segment progress


def test_progress_clamped_when_outside_segment():
    stop = {"stop_id": "A", "lat": 0.0, "lon": 0.0, "stop_order": 5}
    next_stop = {"stop_id": "B", "lat": 0.0, "lon": 1.0}

    vp_before = {"lat": 0.0, "lon": -1.0}
    feats_before = calculate_distance_features_with_shape(
        vp_before,
        stop,
        next_stop,
        shape=None,
        vehicle_stop_order=5,
        total_segments=8,
    )
    assert feats_before["progress_on_segment"] == 0.0

    vp_past = {"lat": 0.0, "lon": 3.0}
    feats_past = calculate_distance_features_with_shape(
        vp_past,
        stop,
        next_stop,
        shape=None,
        vehicle_stop_order=5,
        total_segments=8,
    )
    assert feats_past["progress_on_segment"] == 0.0


def test_zero_length_segment_defaults():
    stop = {"stop_id": "S", "lat": 9.9, "lon": -84.0, "stop_order": 3}
    next_stop = {"stop_id": "S", "lat": 9.9, "lon": -84.0}
    vp = {"lat": 9.9, "lon": -84.001}

    feats = calculate_distance_features_with_shape(
        vp,
        stop,
        next_stop,
        shape=None,
        vehicle_stop_order=3,
        total_segments=5,
    )

    assert feats["progress_on_segment"] == 0.0
    assert feats["distance_to_next_stop"] == 0.0
    assert approx_equal(feats["progress_ratio"], 3 / 5, 0.01)


def test_shape_based_progress_used_when_available():
    shape = ShapePolyline([
        (0.0, 0.0),
        (0.0, 1.0),
        (0.0, 2.0),
    ])
    vp = {"lat": 0.0, "lon": 0.5}
    stop = {"stop_id": "B", "lat": 0.0, "lon": 1.0, "stop_order": 1}
    next_stop = {"stop_id": "C", "lat": 0.0, "lon": 2.0}

    feats = calculate_distance_features_with_shape(
        vp,
        stop,
        next_stop,
        shape=shape,
        vehicle_stop_order=1,
        total_segments=2,
    )

    assert 0.24 <= feats["shape_progress"] <= 0.26
    assert feats["progress_ratio"] == pytest.approx(feats["shape_progress"])
    assert feats["shape_distance_to_stop"] > 0
    assert feats["cross_track_error"] < 50


def test_shape_progress_overrides_progress_on_segment():
    shape = ShapePolyline([
        (0.0, 0.0),
        (0.5, 0.5),
        (1.0, 1.0),
    ])
    vp = {"lat": 0.25, "lon": 0.25}
    stop = {"stop_id": "B", "lat": 0.5, "lon": 0.5, "stop_order": 1}
    next_stop = {"stop_id": "C", "lat": 1.0, "lon": 1.0}

    feats = calculate_distance_features_with_shape(
        vp,
        stop,
        next_stop,
        shape=shape,
        vehicle_stop_order=1,
        total_segments=2,
    )

    assert feats["progress_on_segment"] == pytest.approx(0.0)
    assert 0.24 <= feats["shape_progress"] <= 0.26
