import os

SECRET_KEY = "test-secret-key"

USE_TZ = True
TIME_ZONE = "UTC"

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    # Enable GeoDjango if requested (default off to avoid system deps for unit tests)
    *(["django.contrib.gis"] if os.getenv("USE_GIS", "0") == "1" else []),
    "gtfs",
]

# All models are abstract — no tables are created, so an in-memory DB suffices.
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
