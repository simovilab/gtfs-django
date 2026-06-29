"""Tests for the collector's S3 dual-write sink (Phase D1).

No DB access: exercises _maybe_sink_vp_to_s3 against a local tmp base_uri.
"""

from __future__ import annotations

import datetime as dt

from django.test import override_settings

from rt_pipeline import tasks
from rt_pipeline.storage import read_vehicle_positions


def _records():
    t = dt.datetime(2025, 1, 2, 8, 0, tzinfo=dt.timezone.utc)
    return [
        {
            "feed_name": "mbta",
            "vehicle_id": "v1",
            "ts": t,
            "lat": 42.0,
            "lon": -71.0,
            "bearing": 90.0,
            "speed": 10.0,
            "route_id": "Green-D",
            "trip_id": "t1",
            "current_stop_sequence": 1,
            "current_status": "IN_TRANSIT_TO",
            "stop_id": "s1",
            "ingested_at": t,
        }
    ]


def test_sink_disabled_is_noop(tmp_path):
    base = str(tmp_path / "vp")
    with override_settings(S3_VP_SINK_ENABLED=False, S3_VP_BASE_URI=base):
        assert tasks._maybe_sink_vp_to_s3(_records()) == 0
    assert read_vehicle_positions(base_uri=base).empty


def test_sink_enabled_writes(tmp_path):
    base = str(tmp_path / "vp")
    with override_settings(S3_VP_SINK_ENABLED=True, S3_VP_BASE_URI=base):
        assert tasks._maybe_sink_vp_to_s3(_records()) == 1
    out = read_vehicle_positions(base_uri=base)
    assert len(out) == 1
    assert out.iloc[0]["route_id"] == "Green-D"


def test_sink_empty_records_noop(tmp_path):
    with override_settings(S3_VP_SINK_ENABLED=True, S3_VP_BASE_URI=str(tmp_path / "vp")):
        assert tasks._maybe_sink_vp_to_s3([]) == 0
