---
icon: lucide/rocket
---

# gtfs-django

A Django app that provides **abstract models**, **custom fields** and **utility functions** for [GTFS](https://gtfs.org/) (General Transit Feed Specification) data, covering both static *Schedule* feeds and dynamic *Realtime* feed messages.

Because all models are abstract, this package creates no database tables of its own. You subclass the models in your own Django app, giving you full control over your schema — adding foreign keys, extra fields, constraints, or any other customisations your project requires.

The package `gtfs-django` also includes custom Django model fields that handle GTFS-specific data types and formats, such as time fields that support hours greater than 24, geographic coordinate fields, and validated fields for color codes, language tags, and more.

Finally, `gtfs-django` provides utilities for importing GTFS data from ZIP files, parsing GTFS Realtime protobuf messages, and validating GTFS feed contents against the specification.

## Requirements

- **Python**: 3.12+
- **Django**: 5.2+

## Installation

=== "uv"

    ``` c
    uv add gtfs-django
    ```

=== "pip"

    ``` c++
    pip install gtfs-django
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

| Model | GTFS file | Description |
|---|---|---|
| `BaseAgency` | `agency.txt` | Transit agencies |
| `BaseStop` | `stops.txt` | Stops and stations |
| `BaseRoute` | `routes.txt` | Routes |
| `BaseTrip` | `trips.txt` | Trips |
| `BaseStopTime` | `stop_times.txt` | Arrival/departure times per stop |
| `BaseCalendar` | `calendar.txt` | Weekly service schedules |
| `BaseCalendarDate` | `calendar_dates.txt` | Service exceptions |
| `BaseShape` | `shapes.txt` | Route shapes |
| `BaseFareAttribute` | `fare_attributes.txt` | Fare definitions |
| `BaseFareRule` | `fare_rules.txt` | Fare applicability rules |
| `BaseFeedInfo` | `feed_info.txt` | Feed metadata |

### GTFS Realtime

| Model | Description |
|---|---|
| `BaseFeedMessage` | Feed message header |
| `BaseTripUpdate` | Real-time trip schedule updates |
| `BaseStopTimeUpdate` | Per-stop arrival/departure updates |
| `BaseVehiclePosition` | Live vehicle locations |
| `BaseAlert` | Service alerts and disruptions |

## Custom Fields

`gtfs.fields` provides Django model fields that encode GTFS type semantics:

| Field | GTFS type | Notes |
|---|---|---|
| `ColorField` | `Color` | 6-digit hex, without `#` |
| `CurrencyCodeField` | `Currency code` | ISO 4217, e.g. `USD` |
| `CurrencyAmountField` | `Currency amount` | Decimal |
| `ServiceDateField` | `Date` | Accepts/serialises `YYYYMMDD` |
| `GTFSTimeField` | `Time` | `timedelta`; supports hours ≥ 24 |
| `GTFSLocalTimeField` | `Local time` | String `HH:MM:SS`; hours 0–23 |
| `GTFSIDField` | `ID` | CharField, optionally ASCII-only |
| `GTFSTextField` | `Text` | CharField with 255-char default |
| `GTFSTimezoneField` | `Timezone` | Validated IANA timezone name |
| `LanguageCodeField` | `Language code` | BCP 47 tag, e.g. `en-US` |
| `LatitudeField` | `Latitude` | Decimal, −90 to 90 |
| `LongitudeField` | `Longitude` | Decimal, −180 to 180 |
| `PhoneNumberField` | `Phone number` | Permissive international format |
| `EnumIntegerField` | `Enum` | Integer with required `choices` |
| `EnumCharField` | `Enum` | String with required `choices` |

## Development

```bash
git clone https://github.com/simovilab/gtfs-django.git
cd gtfs-django

# Install with development dependencies
uv sync

# Run tests
pytest
```

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
