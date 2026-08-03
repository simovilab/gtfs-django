from datetime import datetime, timedelta
import math
import pytz
from django.core.serializers.json import DjangoJSONEncoder
import json


def gtfs_time(value):
    """Convert GTFS HH:MM:SS strings (including >24h) to timedelta."""
    if not value:
        return None
    if isinstance(value, timedelta):
        return value

    try:
        hours, minutes, seconds = map(int, str(value).split(":"))
    except (ValueError, AttributeError):
        return None

    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def gtfs_date(value):
    """Convert GTFS YYYYMMDD strings to date objects for Django DateField."""
    if not value:
        return None
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
        return value

    try:
        return datetime.strptime(str(value), "%Y%m%d").date()
    except (ValueError, TypeError):
        return None


def gtfs_timestamp(value, timezone=pytz.UTC) -> datetime | None:
    """Convert GTFS unix timestamp values to timezone-aware datetime."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value

    try:
        return datetime.fromtimestamp(int(value), tz=timezone)
    except (ValueError, TypeError, OverflowError):
        return None


def normalize_gtfs_value(value):
    """Convert null-like CSV values to None before model instantiation."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.lower() in {"", "nan", "none", "null"}:
            return None
        return cleaned
    return value


def channel_safe_payload(payload):
    """Convert payloads to JSON-safe primitives for channel layer transport."""
    return json.loads(json.dumps(payload, cls=DjangoJSONEncoder))
