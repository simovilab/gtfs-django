# feature_engineering/tests/test_temporal.py
from datetime import datetime, timezone, date
import pytest

import feature_engineering.temporal as temporal


class DummyCalendar:
    """Minimal calendar stub that supports `date in cal` checks."""
    def __init__(self, holiday_dates):
        self._dates = set(holiday_dates)

    def __contains__(self, d):
        return d in self._dates


def test_naive_timestamp_assumed_utc_and_converted_to_mbta_local():
    # Oct 8, 2025 12:30:00 UTC → America/New_York (EDT, UTC-4) = 08:30
    ts = datetime(2025, 10, 8, 12, 30, 0)  # naive -> treated as UTC
    feats = temporal.extract_temporal_features(ts, tz="America/New_York", region="US_MA")
    assert feats["hour"] == 8
    assert feats["day_of_week"] == 2  # 0=Mon -> Wed=2
    assert feats["is_weekend"] is False
    # 08:30 → morning bin per spec
    assert feats["time_of_day_bin"] == "morning"
    # Weekday 8am → peak
    assert feats["is_peak_hour"] is True


def test_aware_utc_timestamp_converts_to_local_consistently():
    # Same instant as previous test, but explicitly aware in UTC
    ts = datetime(2025, 10, 8, 12, 30, 0, tzinfo=timezone.utc)
    feats = temporal.extract_temporal_features(ts, tz="America/New_York", region="US_MA")
    assert feats["hour"] == 8
    assert feats["day_of_week"] == 2
    assert feats["is_peak_hour"] is True

@pytest.mark.parametrize(
    "hour,expected_bin",
    [
        (5, "morning"),
        (9, "morning"),
        (10, "midday"),
        (13, "midday"),
        (14, "afternoon"),
        (17, "afternoon"),
        (18, "evening"),
        (23, "evening"),
        (0, "evening"),
        (3, "evening"),
        (4, "evening"),
    ],
)

def test_time_of_day_bins(hour, expected_bin):
    # Build a timestamp that is ALREADY in the target local tz (America/New_York).
    # That way, `hour` is the local hour we want to assert on.
    ny_tz = temporal.zoneinfo.ZoneInfo("America/New_York")
    local_ts = datetime(2025, 9, 30, hour, 0, 0, tzinfo=ny_tz)  # Tuesday
    feats = temporal.extract_temporal_features(local_ts, tz="America/New_York", region="US_MA")
    assert feats["time_of_day_bin"] == expected_bin

def test_peak_hours_windows_and_weekend_rule():
    # Weekday 17:00 local → peak
    ts_local_peak = datetime(2025, 9, 30, 17, 0, 0, tzinfo=temporal.zoneinfo.ZoneInfo("America/New_York"))  # Tue
    feats = temporal.extract_temporal_features(ts_local_peak, tz="America/New_York", region="US_MA")
    assert feats["is_peak_hour"] is True

    # Weekday 20:00 local → not peak
    ts_local_offpeak = datetime(2025, 9, 30, 20, 0, 0, tzinfo=temporal.zoneinfo.ZoneInfo("America/New_York"))
    feats2 = temporal.extract_temporal_features(ts_local_offpeak, tz="America/New_York", region="US_MA")
    assert feats2["is_peak_hour"] is False

    # Weekend 08:00 local → not peak even though within 7–9
    ts_weekend = datetime(2025, 10, 4, 8, 0, 0, tzinfo=temporal.zoneinfo.ZoneInfo("America/New_York"))  # Sat
    feats3 = temporal.extract_temporal_features(ts_weekend, tz="America/New_York", region="US_MA")
    assert feats3["day_of_week"] == 5 and feats3["is_weekend"] is True
    assert feats3["is_peak_hour"] is False


def test_cr_timezone_conversion_and_weekday():
    # Costa Rica is UTC-6 all year (no DST). 14:00 UTC -> 08:00 local
    ts_utc = datetime(2025, 8, 12, 14, 0, 0, tzinfo=timezone.utc)  # Tue
    feats = temporal.extract_temporal_features(ts_utc, tz="America/Costa_Rica", region="CR")
    assert feats["hour"] == 8
    assert feats["day_of_week"] == 1  # Tuesday
    assert feats["is_weekend"] is False


def test_holiday_true_via_monkeypatched_calendar(monkeypatch):
    # Pick an arbitrary date and mark it as a holiday through a stub calendar
    holiday_d = date(2025, 7, 4)
    def fake_get_cal(region: str):
        return DummyCalendar({holiday_d})

    monkeypatch.setattr(temporal, "_get_holiday_calendar", fake_get_cal)

    ts_local = datetime(2025, 7, 4, 9, 0, 0, tzinfo=temporal.zoneinfo.ZoneInfo("America/New_York"))
    feats = temporal.extract_temporal_features(ts_local, tz="America/New_York", region="US_MA")
    assert feats["is_holiday"] is True


def test_holiday_false_when_calendar_missing(monkeypatch):
    # Simulate absence/failure of holidays package: calendar returns None
    monkeypatch.setattr(temporal, "_get_holiday_calendar", lambda region: None)
    ts_local = datetime(2025, 7, 4, 9, 0, 0, tzinfo=temporal.zoneinfo.ZoneInfo("America/New_York"))
    feats = temporal.extract_temporal_features(ts_local, tz="America/New_York", region="US_MA")
    assert feats["is_holiday"] is False
