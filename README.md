# GTFS Django

[![PyPI version](https://badge.fury.io/py/gtfs-django.svg)](https://badge.fury.io/py/gtfs-django)
[![Python versions](https://img.shields.io/pypi/pyversions/gtfs-django.svg)](https://pypi.org/project/gtfs-django/)
[![Django versions](https://img.shields.io/pypi/frameworkversions/django/gtfs-django.svg)](https://pypi.org/project/gtfs-django/)
[![License](https://img.shields.io/pypi/l/gtfs-django.svg)](https://github.com/simovilab/gtfs-django/blob/main/LICENSE)

A Django package that provides **abstract models** for [GTFS](https://gtfs.org/) (General Transit Feed Specification) data, covering both static _Schedule_ feeds and dynamic _Realtime_ feed messages.

Because all models are abstract, this package creates no database tables of its own. You subclass the models in your own Django app, giving you full control over your schema — adding foreign keys, extra fields, constraints, or any other customisations your project requires.

The package `gtfs-django` also includes custom Django model fields that handle GTFS-specific data types and formats, such as time fields that support hours greater than 24, geographic coordinate fields, and validated fields for color codes, language tags, and more.

Finally, `gtfs-django` provides utilities for importing GTFS data from ZIP files, parsing GTFS Realtime protobuf messages, and validating GTFS feed contents against the specification.

## Requirements

- **Python**: 3.12+
- **Django**: 5.2+

## Installation

```bash
pip install gtfs-django
```

```bash
uv add gtfs-django
```

## Quick Start

### 1. Add to your Django settings

```python
INSTALLED_APPS = [
    # ...
    "gtfs",
]
```

### 2. Subclass the abstract models in your app

Add auxiliary tables or new fields as needed.

```python
from django.db import models
from gtfs.models import BaseAgency, BaseRoute, BaseTrip, BaseStop, BaseStopTime


class Feed(models.Model):
    """Represents a versioned GTFS feed download."""
    name = models.CharField(max_length=255)
    downloaded_at = models.DateTimeField(auto_now_add=True)


class Agency(BaseAgency):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)


class Route(BaseRoute):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    agency = models.ForeignKey(Agency, on_delete=models.CASCADE)


class Trip(BaseTrip):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)


class Stop(BaseStop):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)


class StopTime(BaseStopTime):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
```

### 3. Run migrations for your app

```bash
python manage.py makemigrations <your_app_name>
python manage.py migrate
```

## Abstract Models

### GTFS Schedule

| Model               | GTFS file             | Description                      |
| ------------------- | --------------------- | -------------------------------- |
| `BaseAgency`        | `agency.txt`          | Transit agencies                 |
| `BaseStop`          | `stops.txt`           | Stops and stations               |
| `BaseRoute`         | `routes.txt`          | Routes                           |
| `BaseTrip`          | `trips.txt`           | Trips                            |
| `BaseStopTime`      | `stop_times.txt`      | Arrival/departure times per stop |
| `BaseCalendar`      | `calendar.txt`        | Weekly service schedules         |
| `BaseCalendarDate`  | `calendar_dates.txt`  | Service exceptions               |
| `BaseShape`         | `shapes.txt`          | Route shapes                     |
| `BaseTimeframe`     | `timeframes.txt`       | Fare timeframes                   |
| `BaseRiderCategory` | `rider_categories.txt` | Fare rider categories             |
| `BaseFareMedia`     | `fare_media.txt`       | Fare media                        |
| `BaseFareProduct`   | `fare_products.txt`    | Fare products                     |
| `BaseFareLegRule`   | `fare_leg_rules.txt`   | Fare leg rules                    |
| `BaseFareLegJoinRule` | `fare_leg_join_rules.txt` | Fare leg join rules          |
| `BaseFareTransferRule` | `fare_transfer_rules.txt` | Fare transfer rules          |
| `BaseArea`          | `areas.txt`            | Areas                             |
| `BaseStopArea`      | `stop_areas.txt`       | Stop-to-area assignments          |
| `BaseNetwork`       | `networks.txt`         | Route networks                    |
| `BaseRouteNetwork`  | `route_networks.txt`   | Route-to-network assignments      |
| `BaseFrequency`     | `frequencies.txt`      | Frequency-based service           |
| `BaseTransfer`      | `transfers.txt`        | Transfers between stops           |
| `BasePathway`       | `pathways.txt`         | Station pathways                  |
| `BaseLevel`         | `levels.txt`           | Station levels                    |
| `BaseLocationGroup` | `location_groups.txt`  | On-demand location groups         |
| `BaseLocationGroupStop` | `location_group_stops.txt` | Group-to-stop assignments   |
| `BaseLocation`      | `locations.geojson`    | On-demand service areas           |
| `BaseBookingRule`   | `booking_rules.txt`    | On-demand booking rules            |
| `BaseTranslation`   | `translations.txt`     | Field translations                |
| `BaseFareAttribute` | `fare_attributes.txt` | Fare definitions                 |
| `BaseFareRule`      | `fare_rules.txt`      | Fare applicability rules         |
| `BaseFeedInfo`      | `feed_info.txt`       | Feed metadata                    |
| `BaseAttribution`   | `attributions.txt`     | Attribution information          |

### GTFS Realtime

| Model                 | Description                        |
| --------------------- | ---------------------------------- |
| `BaseFeedMessage`     | Feed message header                |
| `BaseTripUpdate`      | Real-time trip schedule updates    |
| `BaseStopTimeUpdate`  | Per-stop arrival/departure updates |
| `BaseVehiclePosition` | Live vehicle locations             |
| `BaseAlert`           | Service alerts and disruptions     |

## Custom Fields

`gtfs.fields` provides Django model fields that encode GTFS type semantics:

| Field                     | GTFS type         | Notes                            |
| ------------------------- | ----------------- | -------------------------------- |
| `GTFSColorField`          | `Color`           | 6-digit hex, without `#`         |
| `GTFSCurrencyCodeField`   | `Currency code`   | ISO 4217, e.g. `USD`             |
| `GTFSCurrencyAmountField` | `Currency amount` | Decimal                          |
| `GTFSDateField`           | `Date`            | Accepts/serialises `YYYYMMDD`    |
| `GTFSTimeField`           | `Time`            | `timedelta`; supports hours ≥ 24 |
| `GTFSLocalTimeField`      | `Local time`      | String `HH:MM:SS`; hours 0-23    |
| `GTFSIDField`             | `ID`              | CharField, optionally ASCII-only |
| `GTFSTextField`           | `Text`            | CharField with 255-char default  |
| `GTFSTimezoneField`       | `Timezone`        | Validated IANA timezone name     |
| `GTFSLanguageCodeField`   | `Language code`   | BCP 47 tag, e.g. `en-US`         |
| `GTFSLatitudeField`       | `Latitude`        | Decimal, -90 to 90               |
| `GTFSLongitudeField`      | `Longitude`       | Decimal, -180 to 180             |
| `GTFSPhoneNumberField`    | `Phone number`    | Permissive international format  |
| `GTFSEnumField`           | `Enum`            | Integer with required `choices`  |
| `GTFSNonNegativeIntegerField` | `Non-negative integer` | Integer ≥ 0                |
| `GTFSNonZeroIntegerField` | `Non-zero integer` | Integer other than 0             |
| `GTFSPositiveIntegerField` | `Positive integer` | Integer > 0                     |
| `GTFSNonNegativeFloatField` | `Non-negative float` | Decimal ≥ 0                   |
| `GTFSPositiveFloatField`  | `Positive float`  | Decimal > 0                      |

## Development

```bash
git clone https://github.com/simovilab/gtfs-django.git
cd gtfs-django

# Install with development dependencies
uv sync

# Run tests
pytest
```

### Regenerating Schedule models

`gtfs/builder/schedule.yaml` is the source of truth for the GTFS Schedule
models. After changing it or `gtfs/builder/mappings.py`, generate
`gtfs/builder/models.py` from the repository root:

```bash
uv run python -m gtfs.builder.builder
```

The generated file contains Schedule models only. Review and migrate its
contents into `gtfs/models.py` manually; the generator does not modify that
file.

## Contributing

Contributions are welcome. For major changes please open an issue first.

1. Follow Django coding standards
2. Add tests for new features
3. Update documentation as needed
4. Ensure compatibility with Django 5.2+

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.

## Credits

Developed by [SIMOVI Lab](https://github.com/simovilab).

---

For more information about GTFS, visit [gtfs.org](https://gtfs.org/).
