---
icon: lucide/package
---

# GTFS Django Models

The GTFS Schedule models for Django are based on the official reference of files and fields inside a *feed*. In `gtfs-django` they are provided as **abstract models** that must be instantiated in your Django project to be used. This design allows for flexibility and customization while adhering to the GTFS specifications.

Models are defined, for example, as:

```python
from django.db import models


class BaseAgency(models.Model):
    """Transit agencies with service represented in this dataset."""

    agency_id = models.CharField(max_length=255, blank=True)
    agency_name = models.CharField(max_length=255)
    agency_url = models.URLField()
    agency_timezone = models.CharField(max_length=255)
    agency_lang = models.CharField(max_length=2, blank=True)
    agency_phone = models.CharField(max_length=127, blank=True, null=True)
    agency_fare_url = models.URLField(blank=True, null=True)
    agency_email = models.EmailField(max_length=254, blank=True, null=True)

    class Meta:
        abstract = True
```

Then, in a different Django app, you can create a concrete model that inherits from `BaseAgency`:

```python
from django.db import models
from gtfs_django.models import BaseAgency

class Agency(BaseAgency):
    pass
```

This provides flexibility for creating auxiliary tables, adding custom fields, or overriding methods without modifying the original GTFS models. For example, to link each agency to a specific GTFS feed, you could add a foreign key to a `Feed` model in your concrete `Agency` model:

```python
from django.db import models
from gtfs_django.models import BaseAgency


class Feed(models.Model):
    name = models.CharField(max_length=255)
    # Other fields related to the feed


class Agency(BaseAgency):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
```

This is important when different versions of GTFS feeds for the same transit system are being stored, because otherwise the unique IDs of the GTFS entities (like `agency_id`, `route_id`, etc.) could conflict across feeds.

Even more, the foreign key from one model to another should be added as well. For example, for `Route`:

```python
class Route(BaseRoute):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    linked_agency = models.ForeignKey(Agency, on_delete=models.CASCADE)
```

The suggestion of using `linked_agency` is made to avoid clashing in the database with `agency_id`.
