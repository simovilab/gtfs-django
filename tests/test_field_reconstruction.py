"""Regression tests: GTFS fields must survive Django's deconstruct()/clone() round trip.

Django rebuilds fields from deconstruct() when cloning them and when loading
migration state. A field whose rebuilt copy differs from the original makes
makemigrations detect changes on every run.
"""

import pytest

from gtfs.fields import (
    GTFSNonNegativeFloatField,
    GTFSNonNegativeIntegerField,
    GTFSNonZeroIntegerField,
    GTFSPositiveFloatField,
    GTFSPositiveIntegerField,
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
