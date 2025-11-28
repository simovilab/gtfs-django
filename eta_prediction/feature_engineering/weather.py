from datetime import datetime, timedelta, timezone
import requests
from django.core.cache import cache

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_weather(lat: float, lon: float, timestamp: datetime) -> dict:
    """
    Returns a dict with:
        - temperature_c: float
        - precipitation_mm: float
        - wind_speed_kmh: float
        - weather_code: int (WMO code)
        - visibility_m: float

    Caching:
        Key:  weather:{lat}:{lon}:{timestamp_hour_utc_iso}
        TTL:  3600s
    """
    # Normalize timestamp to UTC and truncate to the start of the hour
    if timestamp.tzinfo is None:
        ts_utc = timestamp.replace(tzinfo=timezone.utc)
    else:
        ts_utc = timestamp.astimezone(timezone.utc)
    ts_hour = ts_utc.replace(minute=0, second=0, microsecond=0)

    cache_key = f"weather:{lat:.5f}:{lon:.5f}:{ts_hour.isoformat()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    # Query a 1-hour window [ts_hour, ts_hour+1h) so the array has exactly one item
    ts_end = ts_hour + timedelta(hours=1)
    target_time_str = ts_hour.strftime("%Y-%m-%dT%H:00")

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join([
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "weather_code",
            "visibility"
        ]),
        # Use explicit start/end to avoid a full-day fetch; keep timezone consistent
        "start": ts_hour.isoformat().replace("+00:00", "Z"),
        "end": ts_end.isoformat().replace("+00:00", "Z"),
        "timezone": "UTC",
        "windspeed_unit": "kmh",     # ensures wind speed is km/h
        "precipitation_unit": "mm"   # (default is mm, set explicitly for clarity)
    }

    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=8)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException:
        # Network/API error — return Nones (caller can decide fallback)
        result = {
            "temperature_c": None,
            "precipitation_mm": None,
            "wind_speed_kmh": None,
            "weather_code": None,
            "visibility_m": None,
        }
        cache.set(cache_key, result, timeout=3600)
        return result

    hourly = data.get("hourly") or {}
    times = hourly.get("time") or []

    # Find the index for the exact hour
    try:
        idx = times.index(target_time_str)
    except ValueError:
        # Hour not found (e.g., API model coverage gap) — return Nones
        result = {
            "temperature_c": None,
            "precipitation_mm": None,
            "wind_speed_kmh": None,
            "weather_code": None,
            "visibility_m": None,
        }
        cache.set(cache_key, result, timeout=3600)
        return result

    def _get(series_name, default=None):
        series = hourly.get(series_name)
        if not series or idx >= len(series):
            return default
        return series[idx]

    result = {
        "temperature_c": _get("temperature_2m"),
        "precipitation_mm": _get("precipitation"),
        "wind_speed_kmh": _get("wind_speed_10m"),
        "weather_code": _get("weather_code"),
        "visibility_m": _get("visibility"),
    }

    cache.set(cache_key, result, timeout=3600)
    return result
