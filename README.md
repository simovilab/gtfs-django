# GTFS Django

[![PyPI version](https://badge.fury.io/py/gtfs-django.svg)](https://badge.fury.io/py/gtfs-django)
[![Python versions](https://img.shields.io/pypi/pyversions/gtfs-django.svg)](https://pypi.org/project/gtfs-django/)
[![Django versions](https://img.shields.io/pypi/frameworkversions/django/gtfs-django.svg)](https://pypi.org/project/gtfs-django/)
[![License](https://img.shields.io/pypi/l/gtfs-django.svg)](https://github.com/simovilab/gtfs-django/blob/main/LICENSE)

A Django app for processing and managing GTFS (General Transit Feed Specification) data, including support for both static schedule data and real-time feeds.

## Features

- **GTFS Schedule Support**: Complete support for GTFS static data including agencies, routes, trips, stops, and schedules
- **GTFS Realtime Support**: Process GTFS-RT feeds for trip updates, vehicle positions, and service alerts
- **GeoDjango Integration**: Built-in geographic capabilities for spatial queries and mapping
- **Composite Primary Keys**: Uses Django 5.2+ composite primary key features for optimal GTFS data modeling
- **Provider Management**: Multi-provider support for managing multiple transit agencies
- **Admin Interface**: Django admin integration for easy data management

## Requirements

- **Python**: 3.12+
- **Django**: 5.2.0+ (required for composite primary key support)
- **PostGIS**: Recommended for production GeoDjango features

## Quick Start

### 1. Install the package

```bash
# Basic installation
pip install gtfs-django

# With PostgreSQL support
pip install gtfs-django[postgresql]
```

### 2. Add to Django settings

```python
INSTALLED_APPS = [
    # ... your other apps
    'django.contrib.gis',  # Required for GeoDjango features
    'gtfs',
]

# Database configuration with PostGIS (recommended)
DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': 'your_db_name',
        'USER': 'your_db_user',
        'PASSWORD': 'your_db_password',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}
```

### 3. Run migrations

```bash
python manage.py migrate
```

### 4. Create a GTFS provider

```python
from gtfs.models import GTFSProvider

provider = GTFSProvider.objects.create(
    code='metro',
    name='Metropolitan Transit Authority',
    timezone='America/New_York',
    schedule_url='https://example.com/gtfs.zip',
    is_active=True
)
```

## Models Overview

### Core GTFS Schedule Models

- **`Feed`**: Represents a GTFS feed with metadata
- **`Agency`**: Transit agencies providing services
- **`Route`**: Transit routes with service patterns
- **`Trip`**: Individual trips on routes
- **`Stop`**: Physical stops where vehicles pick up/drop off passengers
- **`StopTime`**: Scheduled times for stops on trips
- **`Calendar`** & **`CalendarDate`**: Service calendars and exceptions

### GeoDjango Models

- **`GeoShape`**: Route shapes with LineString geometries
- **`Stop`**: Includes PointField for precise geographic locations

### GTFS Realtime Models

- **`FeedMessage`**: GTFS-RT feed message headers
- **`TripUpdate`**: Real-time trip schedule updates
- **`VehiclePosition`**: Live vehicle location data
- **`Alert`**: Service alerts and disruptions

## Usage Examples

### Working with GTFS Schedule Data

```python
from gtfs.models import Agency, Route, Trip, Stop

# Get all agencies
agencies = Agency.objects.all()

# Find routes by type
bus_routes = Route.objects.filter(route_type=3)  # 3 = Bus

# Get stops within a geographic area (requires PostGIS)
from django.contrib.gis.geos import Point
from django.contrib.gis.measure import Distance

center = Point(-122.4194, 37.7749)  # San Francisco
nearby_stops = Stop.objects.filter(
    stop_point__distance_lte=(center, Distance(km=1))
)
```

### Processing GTFS Realtime Data

```python
from gtfs.models import VehiclePosition, TripUpdate

# Get recent vehicle positions
recent_positions = VehiclePosition.objects.filter(
    vehicle_timestamp__gte=timezone.now() - timedelta(minutes=5)
)

# Check for service alerts
from gtfs.models import Alert
active_alerts = Alert.objects.filter(
    published__lte=timezone.now(),
    # Add your alert filtering logic
)
```

## Advanced Features

### Composite Primary Keys

This package takes advantage of Django 5.2's composite primary key support for optimal GTFS data modeling:

```python
class StopTime(models.Model):
    # Uses composite primary key for (feed, trip_id, stop_sequence)
    class Meta:
        constraints = [
            UniqueConstraint(
                fields=["feed", "trip_id", "stop_sequence"],
                name="unique_stoptime_in_feed",
            )
        ]
```

### Geographic Queries

With PostGIS backend, you can perform sophisticated spatial queries:

```python
from django.contrib.gis.db.models import Q
from django.contrib.gis.geos import Polygon

# Find all stops within a polygon area
area = Polygon(...) # Define your polygon
stops_in_area = Stop.objects.filter(stop_point__within=area)

# Find nearest stops to a point
from django.contrib.gis.db.models.functions import Distance

nearest_stops = Stop.objects.annotate(
    distance=Distance('stop_point', center_point)
).order_by('distance')[:5]
```

## Development Setup

For development work on this package:

```bash
# Clone the repository
git clone https://github.com/simovilab/gtfs-django.git
cd gtfs-django

# Install development dependencies
pip install -e .[dev]

# Run tests
pytest

# Run with GeoDjango tests (requires PostGIS)
USE_GIS=1 pytest
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Guidelines

1. Follow Django coding standards
2. Add tests for new features
3. Update documentation as needed
4. Ensure compatibility with Django 5.2+

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Credits

Developed by [Simovi Lab](https://github.com/simovilab) for processing and managing GTFS transit data in Django applications.

## Related Projects

- [GTFS-to](https://github.com/BlinkTagInc/gtfs-to-geojson) - Convert GTFS to various formats
- [Transitland](https://www.transit.land/) - Open transit data platform
- [OpenTripPlanner](http://www.opentripplanner.org/) - Multimodal trip planning software

---

For more information about GTFS, visit the [General Transit Feed Specification](https://gtfs.org/) website.