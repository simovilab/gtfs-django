from decimal import Decimal

import yaml
from django.core.exceptions import ValidationError
from django.db import models
import pytest

from gtfs.builder.builder import _enum_choices
from gtfs.builder.mappings import CLASS_NAMES, FIELD_NAMES
from gtfs.fields import (
    GTFSNonNegativeFloatField,
    GTFSNonNegativeIntegerField,
    GTFSNonZeroIntegerField,
    GTFSPositiveFloatField,
    GTFSPositiveIntegerField,
    GTFSTextField,
)
from gtfs import fields as gtfs_fields
from gtfs.builder import models as gtfs_models


def test_generated_models_cover_all_schedule_files_and_fields():
    with open("gtfs/builder/schedule.yaml", encoding="utf-8") as schedule_file:
        schedule = yaml.safe_load(schedule_file)

    files = schedule["field_definitions"]["files"]
    descriptions = {
        file["name"]: file["description"]
        for file in schedule["dataset_files"]["files"]
    }
    class_names = dict(CLASS_NAMES)
    field_names = dict(FIELD_NAMES)
    assert len(files) == 32
    assert set(class_names) == {file["file"] for file in files}

    for file in files:
        model = getattr(gtfs_models, class_names[file["file"]])
        assert model._meta.abstract
        assert descriptions[file["file"]] in model.__doc__
        assert f"Maps to {file['file']}." in model.__doc__
        assert "This model does not include primary keys." in model.__doc__
        assert "\\n" not in model.__doc__
        assert "\n\n" in model.__doc__

        for field in file["fields"]:
            generated_field = model._meta.get_field(field["name"])
            field_class = field_names[field["type"]]
            if field["type"] == "Enum":
                choices = _enum_choices(field)
                values = [
                    choice[0]
                    for option in choices
                    for choice in (
                        option[1]
                        if len(option) == 2 and isinstance(option[1], tuple)
                        else (option,)
                    )
                ]
                if any(value is not None and not isinstance(value, int) for value in values):
                    field_class = "GTFSTextField"

            expected_class = (
                getattr(gtfs_fields, field_class)
                if field_class.startswith("GTFS")
                else getattr(models, field_class)
            )
            assert isinstance(generated_field, expected_class), (
                file["file"],
                field["name"],
            )


def test_schedule_model_primary_keys_are_left_to_subclasses():
    for model in (gtfs_models.BaseAgency, gtfs_models.BaseStop, gtfs_models.BaseShape):
        assert all(not field.primary_key for field in model._meta.fields)
        assert not model._meta.unique_together
    assert not hasattr(gtfs_models, "BaseFeedMessage")


def test_enum_choices_preserve_values_empty_and_groups():
    choices = _enum_choices(
        {
            "name": "sample",
            "options": [
                {"value": "0 or empty", "description": "First"},
                {"value": "empty", "description": "No limit"},
                {
                    "category": "Category",
                    "values": [{"value": "1", "description": "Grouped"}],
                },
            ],
        }
    )
    assert choices == (
        (0, "First"),
        (None, "No limit"),
        ("Category", ((1, "Grouped"),)),
    )
    transfers = gtfs_models.BaseFareAttribute._meta.get_field("transfers")
    assert transfers.null is True
    table_name = gtfs_models.BaseTranslation._meta.get_field("table_name")
    assert table_name.choices[0][0] == "agency"
    assert isinstance(table_name, GTFSTextField)


def test_numeric_sign_field_validators():
    assert GTFSNonNegativeIntegerField().clean("0", None) == 0
    with pytest.raises(ValidationError):
        GTFSNonNegativeIntegerField().clean("-1", None)

    assert GTFSNonZeroIntegerField().clean("-1", None) == -1
    with pytest.raises(ValidationError):
        GTFSNonZeroIntegerField().clean("0", None)

    assert GTFSPositiveIntegerField().clean("1", None) == 1
    with pytest.raises(ValidationError):
        GTFSPositiveIntegerField().clean("0", None)

    assert GTFSNonNegativeFloatField().clean("0", None) == Decimal("0")
    with pytest.raises(ValidationError):
        GTFSNonNegativeFloatField().clean("-0.1", None)

    assert GTFSPositiveFloatField().clean("0.1", None) == Decimal("0.1")
    with pytest.raises(ValidationError):
        GTFSPositiveFloatField().clean("0", None)
