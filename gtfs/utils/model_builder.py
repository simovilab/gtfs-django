from django.conf import settings

validation_enabled = getattr(settings, "GTFS_VALIDATION", False)
reference_source = getattr(settings, "GTFS_SCHEDULE_REFERENCE_SOURCE", None)


def show_settings():
    return {
        "validation_enabled": validation_enabled,
        "reference_source": reference_source,
    }
