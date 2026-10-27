"""Generate GTFS Schedule model classes from the specification YAML."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from .mappings import CLASS_NAMES, FIELD_NAMES

BUILDER_DIR = Path(__file__).resolve().parent
SCHEDULE_PATH = BUILDER_DIR / "schedule.yaml"
TEMPLATE_PATH = BUILDER_DIR / "models.py.jinja"
OUTPUT_PATH = BUILDER_DIR / "models.py"
_INTEGER_ENUM_VALUE = re.compile(r"^(-?\d+)(?:\s*(?:or\s+empty|\(or\s+empty\)))?$", re.IGNORECASE)


def _enum_value(value: Any) -> Any:
    if value is None or (isinstance(value, str) and value.strip().lower() == "empty"):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        match = _INTEGER_ENUM_VALUE.fullmatch(value.strip())
        if match:
            return int(match.group(1))
    return value


def _enum_choices(field: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    options = field.get("options")
    if not options:
        raise ValueError(f"Enum field {field['name']!r} has no options.")

    choices: list[tuple[Any, ...]] = []
    for option in options:
        if "values" in option:
            values = option["values"]
            if not values:
                raise ValueError(
                    f"Enum field {field['name']!r} has an empty option group."
                )
            group_choices = tuple(
                (_enum_value(value["value"]), value["description"])
                for value in values
            )
            choices.append((option["category"], group_choices))
        else:
            if "value" not in option:
                raise ValueError(
                    f"Enum field {field['name']!r} has an option without a value."
                )
            choices.append(
                (_enum_value(option["value"]), option["description"])
            )
    return tuple(choices)


def _validate_mappings(
    files: list[dict[str, Any]],
    class_names: dict[str, str],
    field_names: dict[str, str],
) -> None:
    file_names = [file["file"] for file in files]
    if len(file_names) != len(set(file_names)):
        raise ValueError("The YAML defines the same field-definition file more than once.")
    if set(file_names) != set(class_names):
        missing = sorted(set(file_names) - set(class_names))
        extra = sorted(set(class_names) - set(file_names))
        raise ValueError(
            f"CLASS_NAMES does not match the YAML files (missing={missing}, extra={extra})."
        )

    for file in files:
        names = [field["name"] for field in file["fields"]]
        if len(names) != len(set(names)):
            raise ValueError(f"{file['file']} defines a field more than once.")
        unknown_types = sorted(
            {field["type"] for field in file["fields"]} - set(field_names)
        )
        if unknown_types:
            raise ValueError(
                f"FIELD_NAMES does not map field types in {file['file']}: "
                f"{unknown_types}."
            )

        primary_key = file.get("primary_key")
        if isinstance(primary_key, str) and primary_key not in {"*", "none"}:
            primary_key = [primary_key]
        if isinstance(primary_key, list):
            nested_names = {
                child["name"]
                for field in file["fields"]
                for child in field.get("children", [])
            }
            unknown_fields = sorted(set(primary_key) - set(names) - nested_names)
            if unknown_fields:
                raise ValueError(
                    f"{file['file']} has primary-key fields not defined in its fields: "
                    f"{unknown_fields}."
                )


def _prepare_files(
    files: list[dict[str, Any]],
    class_names: dict[str, str],
    field_names: dict[str, str],
    file_descriptions: dict[str, str],
) -> list[dict[str, Any]]:
    prepared_files = []
    for source_file in files:
        file = dict(source_file)
        file["class_name"] = class_names[file["file"]]
        file["description"] = file_descriptions[file["file"]]

        prepared_fields = []
        for source_field in file["fields"]:
            field = dict(source_field)
            field_class = field_names[field["type"]]
            if field["type"] == "Enum":
                field["choices"] = _enum_choices(field)
                choice_values = [
                    choice[0]
                    for option in field["choices"]
                    for choice in (
                        option[1] if len(option) == 2 and isinstance(option[1], tuple)
                        else (option,)
                    )
                ]
                if any(value is not None and not isinstance(value, int) for value in choice_values):
                    field_class = "GTFSTextField"
                field["allow_empty_enum"] = None in choice_values
            else:
                field["allow_empty_enum"] = False

            field["field_name"] = field_class
            field["field_module"] = (
                "gtfs_fields" if field_class.startswith("GTFS") else "models"
            )
            prepared_fields.append(field)
        file["fields"] = prepared_fields
        prepared_files.append(file)
    return prepared_files


def generate_models() -> Path:
    """Render the GTFS Schedule models beside this builder."""
    schedule = yaml.safe_load(SCHEDULE_PATH.read_text(encoding="utf-8"))
    files = schedule["field_definitions"]["files"]
    dataset_files = schedule["dataset_files"]["files"]
    class_names = dict(CLASS_NAMES)
    field_names = dict(FIELD_NAMES)
    file_descriptions = {
        file["name"]: file["description"] for file in dataset_files
    }

    if len(file_descriptions) != len(dataset_files):
        raise ValueError("dataset_files contains duplicate file names.")

    if len(class_names) != len(CLASS_NAMES):
        raise ValueError("CLASS_NAMES contains duplicate file names.")
    if len(field_names) != len(FIELD_NAMES):
        raise ValueError("FIELD_NAMES contains duplicate field types.")
    if len(set(class_names.values())) != len(class_names):
        raise ValueError("CLASS_NAMES contains duplicate class names.")

    _validate_mappings(files, class_names, field_names)
    if set(file_descriptions) != set(class_names):
        missing = sorted(set(class_names) - set(file_descriptions))
        extra = sorted(set(file_descriptions) - set(class_names))
        raise ValueError(
            "dataset_files descriptions do not match CLASS_NAMES "
            f"(missing={missing}, extra={extra})."
        )
    environment = Environment(
        loader=FileSystemLoader(BUILDER_DIR),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    environment.filters["python_repr"] = repr
    template = environment.get_template(TEMPLATE_PATH.name)
    rendered = template.render(
        files=_prepare_files(files, class_names, field_names, file_descriptions),
    )
    OUTPUT_PATH.write_text(rendered, encoding="utf-8")
    return OUTPUT_PATH


if __name__ == "__main__":
    print(f"Generated {generate_models()}")
