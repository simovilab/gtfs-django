import pytest
from datetime import timedelta
from django.core.exceptions import ValidationError

from gtfs.fields import (
    ColorField,
    CurrencyCodeField,
    GTFSTimeField,
    GTFSLocalTimeField,
    LanguageCodeField,
    ServiceDateField,
    GTFSTimezoneField,
)


def test_color_field_normalizes_and_validates():
    field = ColorField()
    assert field.clean("0039a6", None) == "0039A6"
    with pytest.raises(ValidationError):
        field.clean("#0039A6", None)
    with pytest.raises(ValidationError):
        field.clean("ZZZZZZ", None)


def test_currency_code_field_normalizes_and_validates():
    field = CurrencyCodeField()
    assert field.clean("eur", None) == "EUR"
    with pytest.raises(ValidationError):
        field.clean("EURO", None)


def test_service_date_field_parses_yyyymmdd():
    field = ServiceDateField()
    parsed = field.clean("20180913", None)
    assert parsed.year == 2018 and parsed.month == 9 and parsed.day == 13


def test_gtfs_time_field_allows_over_24_hours_and_normalizes():
    field = GTFSTimeField()
    assert field.clean("1:05:00", None) == timedelta(hours=1, minutes=5)
    assert field.clean("25:35:00", None) == timedelta(hours=25, minutes=35)


def test_gtfs_local_time_field_rejects_over_23_hours():
    field = GTFSLocalTimeField()
    assert field.clean("23:59:59", None) == "23:59:59"
    with pytest.raises(ValidationError):
        field.clean("24:00:00", None)


def test_language_code_field_validates_basic_bcp47():
    field = LanguageCodeField()
    assert field.clean("en", None) == "en"
    assert field.clean("en-US", None) == "en-US"
    with pytest.raises(ValidationError):
        field.clean("en_US", None)


def test_timezone_field_rejects_spaces():
    field = GTFSTimezoneField()
    with pytest.raises(ValidationError):
        field.clean("America/Los Angeles", None)

    # Only test actual zone resolution if the runtime has tzdata available.
    try:
        from zoneinfo import ZoneInfo

        ZoneInfo("UTC")
    except Exception:
        return

    assert field.clean("UTC", None) == "UTC"
