# tests/test_spatial.py
import math
import pytest

from feature_engineering.spatial import (
    calculate_distance_features,
    get_route_features,
)

# --- helpers ---
def approx_equal(a, b, tol):
    return abs(a - b) <= tol


def test_distance_and_bearing_basic_equator_east():
    vp = {"lat": 0.0, "lon": 0.0, "bearing": 90.0}
    stop = {"stop_id": "A", "lat": 0.0, "lon": 1.0}
    feats = calculate_distance_features(vp, stop, None)

    # Distance 1 degree of lon at equator ~ 111.32 km
    assert approx_equal(feats["distance_to_stop"], 111_320, 600)  # ±600 m tolerance
    # Bearing from (0,0) to (0,1) ~ 90°
    assert approx_equal(feats["bearing_to_stop"], 90.0, 2.0)
    # With vbearing=90 and threshold 35°, approaching should be True
    assert feats["is_approaching"] is True
    # No next_stop provided
    assert feats["distance_to_next_stop"] is None
    assert feats["segment_id"] is None
    assert feats["progress_on_segment"] is None


def test_is_approaching_false_when_heading_away():
    vp = {"lat": 0.0, "lon": 0.0, "bearing": 270.0}  # heading west
    stop = {"stop_id": "A", "lat": 0.0, "lon": 1.0}  # east
    feats = calculate_distance_features(vp, stop, None)
    assert feats["is_approaching"] is False


def test_progress_on_segment_midpoint():
    # Segment along equator from lon 0 -> 1 (east)
    stop = {"stop_id": "A", "lat": 0.0, "lon": 0.0}
    next_stop = {"stop_id": "B", "lat": 0.0, "lon": 1.0}
    vp = {"lat": 0.0, "lon": 0.5, "bearing": 90.0}  # halfway between

    feats = calculate_distance_features(vp, stop, next_stop)
    assert 0.4 <= feats["progress_on_segment"] <= 0.6  # ~0.5 within slack
    # Distance to next ~ 0.5 deg lon at equator ~ 55.66 km
    assert approx_equal(feats["distance_to_next_stop"], 55_660, 600)


def test_progress_is_clamped_to_bounds():
    stop = {"stop_id": "A", "lat": 0.0, "lon": 0.0}
    next_stop = {"stop_id": "B", "lat": 0.0, "lon": 1.0}

    # Before starting segment (far behind the first stop)
    vp_before = {"lat": 0.0, "lon": -1.0, "bearing": 90.0}
    feats_before = calculate_distance_features(vp_before, stop, next_stop)
    assert feats_before["progress_on_segment"] == 0.0  # clamped

    # Way past the next stop
    vp_past = {"lat": 0.0, "lon": 3.0, "bearing": 90.0}
    feats_past = calculate_distance_features(vp_past, stop, next_stop)
    assert feats_past["progress_on_segment"] == 0.0  # proxy negative → clamped to 0.0


def test_segment_id_stability():
    stop = {"stop_id": "S1", "lat": 0.0, "lon": 0.0}
    next_stop = {"stop_id": "S2", "lat": 0.0, "lon": 1.0}
    vp = {"lat": 0.0, "lon": 0.1}

    f1 = calculate_distance_features(vp, stop, next_stop)
    f2 = calculate_distance_features(vp, stop, next_stop)
    assert f1["segment_id"] == f2["segment_id"]
    assert isinstance(f1["segment_id"], str)
    assert len(f1["segment_id"]) == 12  # short sha1 hex as implemented


def test_zero_length_segment_progress_zero():
    # stop == next_stop (data quirk)
    stop = {"stop_id": "S", "lat": 9.9, "lon": -84.0}
    next_stop = {"stop_id": "S", "lat": 9.9, "lon": -84.0}
    vp = {"lat": 9.9, "lon": -84.001}

    feats = calculate_distance_features(vp, stop, next_stop)
    assert feats["progress_on_segment"] == 0.0
    assert feats["distance_to_next_stop"] >= 0.0


def test_get_route_features_with_provided_stops():
    # Three stops along the equator: (0,0) → (0,1) → (0,2)
    stops = [
        {"stop_id": "A", "lat": 0.0, "lon": 0.0},
        {"stop_id": "B", "lat": 0.0, "lon": 1.0},
        {"stop_id": "C", "lat": 0.0, "lon": 2.0},
    ]
    feats = get_route_features("routeX", stops_in_order=stops)

    assert feats["total_stops"] == 3

    # Each leg ~111.32 km → total ~222.64 km
    assert approx_equal(feats["route_length_km"], 222.64, 1.5)  # ±1.5 km tolerance

    # Avg spacing ~111.32 km in meters
    assert approx_equal(feats["avg_stop_spacing"], 111_320, 800)


def test_get_route_features_insufficient_stops():
    feats0 = get_route_features("r0", stops_in_order=[])
    assert feats0["total_stops"] == 0
    assert feats0["route_length_km"] == 0.0
    assert feats0["avg_stop_spacing"] == 0.0

    feats1 = get_route_features("r1", stops_in_order=[{"stop_id": "A", "lat": 9.9, "lon": -84.1}])
    assert feats1["total_stops"] == 1
    assert feats1["route_length_km"] == 0.0
    assert feats1["avg_stop_spacing"] == 0.0
