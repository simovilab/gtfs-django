from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch
import pytest

from feature_engineering.weather import fetch_weather, OPEN_METEO_URL


class FakeCache:
    """Tiny in-memory cache with the subset of the Django cache API we use."""
    def __init__(self):
        self._d = {}
    def get(self, key, default=None):
        return self._d.get(key, default)
    def set(self, key, value, timeout=None):
        self._d[key] = value
    def clear(self):
        self._d.clear()


def _mock_response(payload: dict, status_code: int = 200):
    resp = Mock()
    resp.status_code = status_code
    resp.json = Mock(return_value=payload)
    resp.raise_for_status = Mock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    return resp


@pytest.fixture()
def fake_cache(monkeypatch):
    # Patch the cache object inside the weather module (so no Django needed)
    from feature_engineering import weather
    fc = FakeCache()
    monkeypatch.setattr(weather, "cache", fc, raising=True)
    return fc


def test_fetch_weather_success_exact_hour(fake_cache):
    fake_cache.clear()
    lat, lon = 9.935, -84.091
    ts = datetime(2025, 1, 1, 12, 34, tzinfo=timezone.utc)
    target_str = ts.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00")

    payload = {
        "hourly": {
            "time": [target_str],
            "temperature_2m": [23.4],
            "precipitation": [0.2],
            "wind_speed_10m": [12.0],
            "weather_code": [80],
            "visibility": [9800.0],
        }
    }

    with patch("feature_engineering.weather.requests.get") as mget:
        mget.return_value = _mock_response(payload)
        out = fetch_weather(lat, lon, ts)

        assert out == {
            "temperature_c": 23.4,
            "precipitation_mm": 0.2,
            "wind_speed_kmh": 12.0,
            "weather_code": 80,
            "visibility_m": 9800.0,
        }

        assert mget.call_count == 1
        args, kwargs = mget.call_args
        assert args[0] == OPEN_METEO_URL
        params = kwargs["params"]
        assert params["latitude"] == lat
        assert params["longitude"] == lon
        assert params["timezone"] == "UTC"
        assert "temperature_2m" in params["hourly"]
        assert "start" in params and "end" in params

        start_dt = datetime.fromisoformat(params["start"].replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(params["end"].replace("Z", "+00:00"))
        assert start_dt.strftime("%Y-%m-%dT%H:00") == target_str
        assert end_dt - start_dt == timedelta(hours=1)


def test_fetch_weather_naive_timestamp_treated_as_utc(fake_cache):
    fake_cache.clear()
    lat, lon = 9.0, -84.0
    naive_ts = datetime(2025, 1, 1, 7, 15)  # treated as UTC in implementation
    target_str = naive_ts.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00")

    payload = {
        "hourly": {
            "time": [target_str],
            "temperature_2m": [30.0],
            "precipitation": [0.0],
            "wind_speed_10m": [5.0],
            "weather_code": [0],
            "visibility": [10000.0],
        }
    }

    with patch("feature_engineering.weather.requests.get") as mget:
        mget.return_value = _mock_response(payload)
        out = fetch_weather(lat, lon, naive_ts)

    assert out["temperature_c"] == 30.0
    assert mget.call_count == 1


def test_fetch_weather_caches_result(fake_cache):
    fake_cache.clear()
    lat, lon = 10.0, -84.0
    ts = datetime(2025, 2, 2, 3, 59, tzinfo=timezone.utc)
    target_str = ts.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00")

    payload = {
        "hourly": {
            "time": [target_str],
            "temperature_2m": [21.1],
            "precipitation": [1.0],
            "wind_speed_10m": [8.0],
            "weather_code": [51],
            "visibility": [7500.0],
        }
    }

    with patch("feature_engineering.weather.requests.get") as mget:
        mget.return_value = _mock_response(payload)

        out1 = fetch_weather(lat, lon, ts)
        assert mget.call_count == 1
        assert out1["temperature_c"] == 21.1

        # Second call should hit the cache (no extra HTTP)
        mget.side_effect = Exception("Should not be called due to cache")
        out2 = fetch_weather(lat, lon, ts)
        assert out2 == out1
        assert mget.call_count == 1


def test_fetch_weather_missing_hour_returns_nones(fake_cache):
    fake_cache.clear()
    lat, lon = 9.5, -83.9
    ts = datetime(2025, 3, 3, 10, 10, tzinfo=timezone.utc)
    wrong_hour = ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    payload = {
        "hourly": {
            "time": [wrong_hour.strftime("%Y-%m-%dT%H:00")],
            "temperature_2m": [25.0],
            "precipitation": [0.0],
            "wind_speed_10m": [10.0],
            "weather_code": [1],
            "visibility": [9000.0],
        }
    }

    with patch("feature_engineering.weather.requests.get") as mget:
        mget.return_value = _mock_response(payload)
        out = fetch_weather(lat, lon, ts)

    assert out == {
        "temperature_c": None,
        "precipitation_mm": None,
        "wind_speed_kmh": None,
        "weather_code": None,
        "visibility_m": None,
    }


def test_fetch_weather_network_error_returns_nones_and_caches(fake_cache):
    fake_cache.clear()
    lat, lon = 9.9, -84.2
    ts = datetime(2025, 4, 4, 6, 0, tzinfo=timezone.utc)

    with patch("feature_engineering.weather.requests.get") as mget:
        from requests import RequestException
        mget.side_effect = RequestException("boom")
        out1 = fetch_weather(lat, lon, ts)

    assert out1 == {
        "temperature_c": None,
        "precipitation_mm": None,
        "wind_speed_kmh": None,
        "weather_code": None,
        "visibility_m": None,
    }

    # Second call should return from cache without HTTP
    with patch("feature_engineering.weather.requests.get") as mget2:
        out2 = fetch_weather(lat, lon, ts)
        assert mget2.call_count == 0
        assert out2 == out1
