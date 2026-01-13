import os
import sys
from pathlib import Path

# =============================
# Path configuration
# =============================

# Go up one level to reach the project root (gtfs-django/)
BASE_DIR = Path(__file__).resolve().parent.parent

# Add the project root to Python's import path
sys.path.insert(0, str(BASE_DIR))

# =============================
# Basic Django settings
# =============================

SECRET_KEY = "test-secret-key"
DEBUG = True
ALLOWED_HOSTS = ["*"]

USE_TZ = True
TIME_ZONE = "UTC"

# =============================
# Installed apps
# =============================

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # Enable GeoDjango if requested (default off to avoid system deps for unit tests)
    *(["django.contrib.gis"] if os.getenv("USE_GIS", "0") == "1" else []),
    "gtfs",  # Main GTFS app
]

# =============================
# Middleware
# =============================

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

# =============================
# URL configuration
# =============================

ROOT_URLCONF = "tests.urls"

# =============================
# Templates
# =============================

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    }
]

# =============================
# Static files
# =============================

STATIC_URL = "/static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# =============================
# Database configuration
# =============================

# Default: SQLite for unit tests
# Optional: enable PostGIS via USE_GIS=1
if os.getenv("USE_GIS", "0") == "1":
    DATABASES = {
        "default": {
            "ENGINE": os.getenv(
                "DJANGO_DB_ENGINE",
                "django.contrib.gis.db.backends.postgis",
            ),
            "NAME": os.getenv("POSTGRES_DB", "gtfs_test"),
            "USER": os.getenv("POSTGRES_USER", "postgres"),
            "PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "HOST": os.getenv("POSTGRES_HOST", "localhost"),
            "PORT": int(os.getenv("POSTGRES_PORT", "5432")),
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": os.path.join(BASE_DIR, "db.sqlite3"),
        }
    }
