"""GTFS-specific Django model fields.

These fields implement the GTFS Schedule field type semantics (validation and
convenient parsing/normalization) while storing values using built-in Django
field types.

Note:
        Some GTFS types already exist in Django (e.g. URLField, EmailField). This
        module focuses on types that either don't exist in Django or need GTFS-
        specific behavior (e.g. GTFS time allowing hours >= 24).
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator, RegexValidator
from django.db import models

try:
    from zoneinfo import ZoneInfo
    from zoneinfo import ZoneInfoNotFoundError
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]
    ZoneInfoNotFoundError = Exception  # type: ignore[assignment]


__all__ = [
    "ColorField",
    "CurrencyAmountField",
    "CurrencyCodeField",
    "EnumCharField",
    "EnumIntegerField",
    "GTFSEmailField",
    "GTFSIDField",
    "GTFSTextField",
    "GTFSTimeField",
    "GTFSLocalTimeField",
    "LanguageCodeField",
    "LatitudeField",
    "LongitudeField",
    "PhoneNumberField",
    "ServiceDateField",
    "GTFSTimezoneField",
    "gtfs_time_to_seconds",
    "normalize_gtfs_time",
]


_HEX_COLOR_RE = re.compile(r"^[0-9A-Fa-f]{6}$")
_CURRENCY_CODE_RE = re.compile(r"^[A-Z]{3}$")

# A pragmatic (not fully exhaustive) BCP 47 validator:
# - Primary language: 2-3 letters
# - Subsequent subtags: 2-8 alphanumerics
_BCP47_RE = re.compile(r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$")

# Basic phone validator allowing common separators; GTFS spec doesn't fully define
# a strict format, so keep it permissive.
_PHONE_RE = re.compile(r"^[0-9+()\-\.\s]{1,32}$")


def normalize_gtfs_time(value: str, *, max_hour: int | None) -> str:
    """Normalize a GTFS time string.

    Accepts `H:MM:SS` or `HH:MM:SS` (and also hours with more digits).
    Returns a normalized string with zero-padded HH when hour < 100.
    """

    if value is None:
        raise ValidationError("Time value cannot be null.")

    value = str(value).strip()
    match = re.fullmatch(r"(?P<h>\d{1,3}):(?P<m>\d{2}):(?P<s>\d{2})", value)
    if not match:
        raise ValidationError("Enter a valid time in HH:MM:SS (H:MM:SS also allowed).")

    hour = int(match.group("h"))
    minute = int(match.group("m"))
    second = int(match.group("s"))
    if minute > 59 or second > 59:
        raise ValidationError("Minutes and seconds must be between 00 and 59.")

    if max_hour is not None and hour > max_hour:
        raise ValidationError(f"Hour must be <= {max_hour}.")

    if hour < 100:
        return f"{hour:02d}:{minute:02d}:{second:02d}"
    return f"{hour}:{minute:02d}:{second:02d}"


def gtfs_time_to_seconds(value: str) -> int:
    """Convert a (normalized or not) GTFS time into seconds since 00:00:00."""

    normalized = normalize_gtfs_time(value, max_hour=None)
    hour_s, minute_s, second_s = normalized.split(":")
    return int(hour_s) * 3600 + int(minute_s) * 60 + int(second_s)


def gtfs_time_to_timedelta(value: str) -> timedelta:
    """Convert a GTFS time string into a `datetime.timedelta`."""

    seconds = gtfs_time_to_seconds(value)
    return timedelta(seconds=seconds)


def format_gtfs_timedelta(value: timedelta) -> str:
    """Format a `datetime.timedelta` as a GTFS time string (H:MM:SS)."""

    total_seconds = int(value.total_seconds())
    if total_seconds < 0:
        raise ValidationError("Duration cannot be negative.")
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours < 100:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours}:{minutes:02d}:{seconds:02d}"


class ColorField(models.CharField):
    """GTFS `Color` (six-digit hexadecimal, without leading '#')."""

    default_validators = [
        RegexValidator(
            regex=_HEX_COLOR_RE,
            message="Enter a 6-digit hex color without '#', e.g. '0039A6'.",
            code="invalid",
        )
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 6)
        super().__init__(*args, **kwargs)

    def to_python(self, value: Any) -> Any:
        value = super().to_python(value)
        if isinstance(value, str):
            return value.upper()
        return value

    def get_prep_value(self, value: Any) -> Any:
        value = super().get_prep_value(value)
        if isinstance(value, str):
            return value.upper()
        return value


class CurrencyCodeField(models.CharField):
    """GTFS `Currency code` (ISO 4217 alphabetic code, e.g. 'EUR')."""

    default_validators = [
        RegexValidator(
            regex=_CURRENCY_CODE_RE,
            message="Enter a valid ISO 4217 currency code like 'USD' or 'EUR'.",
            code="invalid",
        )
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 3)
        super().__init__(*args, **kwargs)

    def to_python(self, value: Any) -> Any:
        value = super().to_python(value)
        if isinstance(value, str):
            return value.upper()
        return value

    def get_prep_value(self, value: Any) -> Any:
        value = super().get_prep_value(value)
        if isinstance(value, str):
            return value.upper()
        return value


class CurrencyAmountField(models.DecimalField):
    """GTFS `Currency amount`.

    GTFS references ISO 4217 for the number of minor units (decimal places) for
    a given currency, but that depends on the accompanying currency code.
    This field stores amounts as Decimal and lets you choose `decimal_places`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_digits", 18)
        kwargs.setdefault("decimal_places", 6)
        super().__init__(*args, **kwargs)


class ServiceDateField(models.DateField):
    """GTFS `Date` stored as a Django DateField.

    Accepts an 8-digit string in `YYYYMMDD` format in addition to `datetime.date`.
    Serializes as `YYYYMMDD`.
    """

    def to_python(self, value: Any) -> Any:
        if isinstance(value, str):
            value = value.strip()
            if re.fullmatch(r"\d{8}", value):
                try:
                    return date(
                        int(value[0:4]),
                        int(value[4:6]),
                        int(value[6:8]),
                    )
                except ValueError as exc:
                    raise ValidationError(
                        "Enter a valid date in YYYYMMDD format."
                    ) from exc
        return super().to_python(value)

    def value_to_string(self, obj: Any) -> str:
        value = self.value_from_object(obj)
        if value is None:
            return ""
        if isinstance(value, date):
            return value.strftime("%Y%m%d")
        return str(value)


class GTFSEmailField(models.EmailField):
    """GTFS `Email`.

    Django already provides EmailField; this is a small convenience wrapper.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 254)
        super().__init__(*args, **kwargs)


class EnumIntegerField(models.IntegerField):
    """GTFS `Enum` stored as an integer.

    Requires `choices`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        choices = kwargs.get("choices")
        if not choices:
            raise TypeError("EnumIntegerField requires non-empty 'choices'.")
        super().__init__(*args, **kwargs)


class EnumCharField(models.CharField):
    """GTFS `Enum` stored as a string.

    Requires `choices`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        choices = kwargs.get("choices")
        if not choices:
            raise TypeError("EnumCharField requires non-empty 'choices'.")
        super().__init__(*args, **kwargs)


class GTFSIDField(models.CharField):
    """GTFS `ID`.

    Spec allows any UTF-8 sequence; printable ASCII is recommended, not required.
    If you want to enforce printable ASCII, pass `enforce_printable_ascii=True`.
    """

    def __init__(
        self,
        *args: Any,
        enforce_printable_ascii: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("max_length", 255)
        super().__init__(*args, **kwargs)

        if enforce_printable_ascii:
            self.validators.append(
                RegexValidator(
                    regex=re.compile(r"^[\x20-\x7E]+$"),
                    message="Enter printable ASCII characters only.",
                    code="invalid",
                )
            )


class LanguageCodeField(models.CharField):
    """GTFS `Language code` (BCP 47)."""

    default_validators = [
        RegexValidator(
            regex=_BCP47_RE,
            message="Enter a valid IETF BCP 47 language tag (e.g. 'en' or 'en-US').",
            code="invalid",
        )
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 35)
        super().__init__(*args, **kwargs)


class LatitudeField(models.DecimalField):
    """GTFS `Latitude` (WGS84, -90..90)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_digits", 9)
        kwargs.setdefault("decimal_places", 6)
        super().__init__(*args, **kwargs)
        self.validators.extend(
            [
                MinValueValidator(Decimal("-90.0")),
                MaxValueValidator(Decimal("90.0")),
            ]
        )


class LongitudeField(models.DecimalField):
    """GTFS `Longitude` (WGS84, -180..180)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_digits", 9)
        kwargs.setdefault("decimal_places", 6)
        super().__init__(*args, **kwargs)
        self.validators.extend(
            [
                MinValueValidator(Decimal("-180.0")),
                MaxValueValidator(Decimal("180.0")),
            ]
        )


class PhoneNumberField(models.CharField):
    """GTFS `Phone number`.

    This keeps validation permissive to accommodate international formats.
    """

    default_validators = [
        RegexValidator(
            regex=_PHONE_RE,
            message="Enter a valid phone number.",
            code="invalid",
        )
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 32)
        super().__init__(*args, **kwargs)


class _BaseGTFSTimeField(models.CharField):
    def __init__(
        self,
        *args: Any,
        max_hour: int | None,
        **kwargs: Any,
    ) -> None:
        self._max_hour = max_hour
        kwargs.setdefault("max_length", 11)
        super().__init__(*args, **kwargs)

    def to_python(self, value: Any) -> Any:
        value = super().to_python(value)
        if value in (None, ""):
            return value
        if isinstance(value, str):
            return normalize_gtfs_time(value, max_hour=self._max_hour)
        return value

    def get_prep_value(self, value: Any) -> Any:
        value = super().get_prep_value(value)
        if value in (None, ""):
            return value
        if isinstance(value, str):
            return normalize_gtfs_time(value, max_hour=self._max_hour)
        return value


class GTFSTimeField(models.DurationField):
    """GTFS `Time` stored as a `datetime.timedelta`.

    GTFS Time is a duration since the start of the service day and may exceed
    24 hours, so this maps naturally to Django's DurationField.

    Accepted input forms:
    - `H:MM:SS` / `HH:MM:SS` strings (hours may be >= 24)
    - `datetime.timedelta`
    """

    def to_python(self, value: Any) -> Any:
        if value in (None, ""):
            return None
        if isinstance(value, timedelta):
            return value
        if isinstance(value, str):
            return gtfs_time_to_timedelta(value)
        return super().to_python(value)

    def get_prep_value(self, value: Any) -> Any:
        if value in (None, ""):
            return None
        if isinstance(value, str):
            value = gtfs_time_to_timedelta(value)
        if isinstance(value, timedelta) and value.total_seconds() < 0:
            raise ValidationError("Duration cannot be negative.")
        return super().get_prep_value(value)

    def value_to_string(self, obj: Any) -> str:
        value = self.value_from_object(obj)
        if value is None:
            return ""
        if isinstance(value, timedelta):
            return format_gtfs_timedelta(value)
        return str(value)


class GTFSLocalTimeField(_BaseGTFSTimeField):
    """GTFS `Local time`.

    Represents wall-clock time, so hours are limited to 0..23.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, max_hour=23, **kwargs)


class GTFSTextField(models.CharField):
    """GTFS `Text` (human-readable string)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 255)
        super().__init__(*args, **kwargs)


def _validate_timezone(value: str) -> None:
    if value is None or value == "":
        return
    if " " in value:
        raise ValidationError("Timezone names must not contain spaces.")
    if ZoneInfo is None:
        return
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise ValidationError(
            "Enter a valid IANA timezone, e.g. 'America/Los_Angeles'."
        ) from exc


class GTFSTimezoneField(models.CharField):
    """GTFS `Timezone` (IANA tz database name)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 64)
        super().__init__(*args, **kwargs)
        self.validators.append(_validate_timezone)
