"""Regression tests: GTFS fields must survive Django's deconstruct()/clone() round trip.

Django rebuilds fields from deconstruct() when cloning them and when loading
migration state. A field whose rebuilt copy differs from the original makes
makemigrations detect changes on every run.
"""

import pytest
from django.core.exceptions import ValidationError

from gtfs.fields import (
    GTFSIDField,
    GTFSLocalTimeField,
    GTFSNonNegativeFloatField,
    GTFSNonNegativeIntegerField,
    GTFSNonZeroIntegerField,
    GTFSPositiveFloatField,
    GTFSPositiveIntegerField,
    _BaseGTFSTimeField,
)

SIGN_FIELDS = [
    GTFSNonNegativeIntegerField,
    GTFSNonZeroIntegerField,
    GTFSPositiveIntegerField,
    GTFSNonNegativeFloatField,
    GTFSPositiveFloatField,
]


def _extra_validator(value):
    """User-supplied validator used to check that explicit validators survive."""


@pytest.mark.parametrize("field_class", SIGN_FIELDS)
def test_clone_preserves_deconstruct(field_class):
    field = field_class()
    clone = field.clone()

    assert clone.deconstruct() == field.deconstruct()
    assert clone.clone().deconstruct() == field.deconstruct()


@pytest.mark.parametrize("field_class", SIGN_FIELDS)
def test_builtin_validator_is_not_serialized(field_class):
    _, _, _, kwargs = field_class().deconstruct()

    assert "validators" not in kwargs


@pytest.mark.parametrize("field_class", SIGN_FIELDS)
def test_user_validators_survive_round_trip(field_class):
    field = field_class(validators=[_extra_validator])
    clone = field.clone()

    assert clone.deconstruct()[3]["validators"] == [_extra_validator]
    assert len(clone.validators) == len(field.validators)


NON_ASCII_ID = "\u00f1"


def test_id_field_keeps_printable_ascii_option_on_clone():
    field = GTFSIDField(enforce_printable_ascii=True)
    clone = field.clone()

    assert clone.deconstruct() == field.deconstruct()
    assert clone.deconstruct()[3]["enforce_printable_ascii"] is True
    with pytest.raises(ValidationError):
        clone.clean(NON_ASCII_ID, None)


def test_id_field_default_accepts_utf8_and_omits_option():
    field = GTFSIDField()
    clone = field.clone()

    assert "enforce_printable_ascii" not in field.deconstruct()[3]
    assert clone.clean(NON_ASCII_ID, None) == NON_ASCII_ID


def test_id_field_validators_are_not_duplicated_on_clone():
    field = GTFSIDField(enforce_printable_ascii=True)

    assert len(field.clone().validators) == len(field.validators)


@pytest.mark.parametrize("max_hour", [None, 30])
def test_base_time_field_round_trips_max_hour(max_hour):
    field = _BaseGTFSTimeField(max_hour=max_hour)
    clone = field.clone()

    assert clone.deconstruct() == field.deconstruct()
    assert clone.deconstruct()[3]["max_hour"] == max_hour


def test_local_time_field_clone_keeps_limit_without_serializing_it():
    field = GTFSLocalTimeField()
    clone = field.clone()

    assert "max_hour" not in field.deconstruct()[3]
    assert clone.deconstruct() == field.deconstruct()
    with pytest.raises(ValidationError):
        clone.clean("24:00:00", None)
