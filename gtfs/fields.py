"""GTFS-specific Django model fields.

These fields implement the GTFS Schedule field type semantics (validation and
convenient parsing/normalization) while storing values using built-in Django
field types.
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
    "GTFSColorField",
    "GTFSCurrencyCodeField",
    "GTFSCurrencyAmountField",
    "GTFSEnumField",
    "GTFSEmailField",
    "GTFSIDField",
    "GTFSTextField",
    "GTFSTimeField",
    "GTFSLocalTimeField",
    "GTFSLanguageCodeField",
    "GTFSLatitudeField",
    "GTFSLongitudeField",
    "GTFSPhoneNumberField",
    "GTFSDateField",
    "GTFSTimezoneField",
    "GTFSNonNegativeIntegerField",
    "GTFSNonZeroIntegerField",
    "GTFSPositiveIntegerField",
    "GTFSNonNegativeFloatField",
    "GTFSPositiveFloatField",
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


class GTFSColorField(models.CharField):
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


class GTFSCurrencyCodeField(models.CharField):
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


class GTFSCurrencyAmountField(models.DecimalField):
    """GTFS `Currency amount`.

    GTFS references ISO 4217 for the number of minor units (decimal places) for
    a given currency, but that depends on the accompanying currency code.
    This field stores amounts as Decimal and lets you choose `decimal_places`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_digits", 18)
        kwargs.setdefault("decimal_places", 6)
        super().__init__(*args, **kwargs)


class GTFSDateField(models.DateField):
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


class GTFSEnumField(models.IntegerField):
    """GTFS `Enum` stored as an integer.

    Requires `choices`.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        choices = kwargs.get("choices")
        if not choices:
            raise TypeError("GTFSEnumField requires non-empty 'choices'.")
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
        self.enforce_printable_ascii = enforce_printable_ascii
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

    def deconstruct(self) -> tuple[str | None, str, list[Any], dict[str, Any]]:
        name, path, args, kwargs = super().deconstruct()
        if self.enforce_printable_ascii:
            kwargs["enforce_printable_ascii"] = True
        return name, path, args, kwargs


class GTFSLanguageCodeField(models.CharField):
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


class GTFSLatitudeField(models.DecimalField):
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


class GTFSLongitudeField(models.DecimalField):
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


class GTFSFloatField(models.DecimalField):
    """GTFS `Float`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_digits", 9)
        kwargs.setdefault("decimal_places", 6)
        super().__init__(*args, **kwargs)


class GTFSIntegerField(models.IntegerField):
    """GTFS `Integer`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class GTFSNonNegativeIntegerField(models.IntegerField):
    """GTFS integer constrained to values greater than or equal to zero."""

    default_validators = [MinValueValidator(0)]


def _validate_non_zero_integer(value: int) -> None:
    if value == 0:
        raise ValidationError("Value must not be zero.")


class GTFSNonZeroIntegerField(models.IntegerField):
    """GTFS integer constrained to non-zero values."""

    default_validators = [_validate_non_zero_integer]


class GTFSPositiveIntegerField(models.IntegerField):
    """GTFS integer constrained to values greater than zero."""

    default_validators = [MinValueValidator(1)]


class GTFSNonNegativeFloatField(models.DecimalField):
    """GTFS decimal constrained to values greater than or equal to zero."""

    default_validators = [MinValueValidator(Decimal("0"))]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_digits", 18)
        kwargs.setdefault("decimal_places", 6)
        super().__init__(*args, **kwargs)


def _validate_positive_float(value: Decimal) -> None:
    if value <= 0:
        raise ValidationError("Value must be greater than zero.")


class GTFSPositiveFloatField(models.DecimalField):
    """GTFS decimal constrained to values greater than zero."""

    default_validators = [_validate_positive_float]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_digits", 18)
        kwargs.setdefault("decimal_places", 6)
        super().__init__(*args, **kwargs)


class GTFSPhoneNumberField(models.CharField):
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

    def deconstruct(self) -> tuple[str | None, str, list[Any], dict[str, Any]]:
        name, path, args, kwargs = super().deconstruct()
        # max_hour is a required keyword-only argument, so always return it.
        kwargs["max_hour"] = self._max_hour
        return name, path, args, kwargs

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


class GTFSLocalTimeField(_BaseGTFSTimeField):
    """GTFS `Local time`.

    Represents wall-clock time, so hours are limited to 0..23.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, max_hour=23, **kwargs)

    def deconstruct(self) -> tuple[str | None, str, list[Any], dict[str, Any]]:
        name, path, args, kwargs = super().deconstruct()
        # max_hour is fixed to 23 by __init__, so it is not a field argument.
        del kwargs["max_hour"]
        return name, path, args, kwargs


class GTFSTextField(models.CharField):
    """GTFS `Text` (human-readable string)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 255)
        super().__init__(*args, **kwargs)


class GTFSTimezoneField(models.CharField):
    """GTFS `Timezone` (IANA tz database name)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 64)
        super().__init__(*args, **kwargs)
        self.validators.append(_validate_timezone)


class GTFSURLField(models.URLField):
    """GTFS `URL` (web address)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("max_length", 2083)
        super().__init__(*args, **kwargs)


"""
Color - A color encoded as a six-digit hexadecimal number. Refer to https://htmlcolorcodes.com to generate a valid value (the leading "#" must not be included).
Example: FFFFFF for white, 000000 for black or 0039A6 for the A,C,E lines in NYMTA.
Currency code - An ISO 4217 alphabetical currency code. For the list of current currency, refer to https://en.wikipedia.org/wiki/ISO_4217#Active_codes.
Example: CAD for Canadian dollars, EUR for euros or JPY for Japanese yen.
Currency amount - A decimal value indicating a currency amount. The number of decimal places is specified by ISO 4217 for the accompanying Currency code. All financial calculations should be processed as decimal, currency, or another equivalent type suitable for financial calculations depending on the programming language used to consume data. Processing currency amounts as float is discouraged due to gains or losses of money during calculations.
Date - Service day in the YYYYMMDD format. Since time within a service day may be above 24:00:00, a service day may contain information for the subsequent day(s).
Example: 20180913 for September 13th, 2018.
Email - An email address.
Example: example@example.com
Enum - An option from a set of predefined constants defined in the "Description" column.
Example: The route_type field contains a 0 for tram, a 1 for subway...
ID - An ID field value is an internal ID, not intended to be shown to riders, and is a sequence of any UTF-8 characters. Using only printable ASCII characters is recommended. An ID is labeled "unique ID" when it must be unique within a file. IDs defined in one .txt file are often referenced in another .txt file. IDs that reference an ID in another table are labeled "foreign ID".
Example: The stop_id field in stops.txt is a "unique ID". The parent_station field in stops.txt is a "foreign ID referencing stops.stop_id".
Language code - An IETF BCP 47 language code. For an introduction to IETF BCP 47, refer to https://www.rfc-editor.org/info/rfc5646/ and http://www.w3.org/International/articles/language-tags/.
Example: en for English, en-US for American English or de for German.
Latitude - WGS84 latitude in decimal degrees. The value must be greater than or equal to -90.0 and less than or equal to 90.0.
Example: 41.890169 for the Colosseum in Rome.
Longitude - WGS84 longitude in decimal degrees. The value must be greater than or equal to -180.0 and less than or equal to 180.0.
Example: 12.492269 for the Colosseum in Rome.
Float - A floating point number.
Integer - An integer.
Phone number - A phone number.
Time - Time in the HH:MM:SS format (H:MM:SS is also accepted). The time is measured from "noon minus 12h" of the service day (effectively midnight except for days on which daylight savings time changes occur). For times occurring after midnight on the service day, enter the time as a value greater than 24:00:00 in HH:MM:SS.
Example: 14:30:00 for 2:30PM or 25:35:00 for 1:35AM on the next day.
Local time - Time in the HH:MM:SS format (H:MM:SS is also accepted). Represents a wall-clock time shown in the local time of the specified location.
Text - A string of UTF-8 characters, which is aimed to be displayed and which must therefore be human readable.
Timezone - TZ timezone from the https://www.iana.org/time-zones. Timezone names never contain the space character but may contain an underscore. Refer to http://en.wikipedia.org/wiki/List_of_tz_zones for a list of valid values.
Example: Asia/Tokyo, America/Los_Angeles or Africa/Cairo.
URL - A fully qualified URL that includes http:// or https://, and any special characters in the URL must be correctly escaped. See the following http://www.w3.org/Addressing/URL/4_URI_Recommentations.html for a description of how to create fully qualified URL values.
"""
