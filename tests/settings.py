import os
import sys
from pathlib import Path

# =========================================
# BASE SETTINGS
# =========================================
BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "test-secret-key"
DEBUG = True
ALLOWED_HOSTS = ["*"]

USE_TZ = True
TIME_ZONE = "UTC"

# =========================================
# APPLICATIONS
# =========================================
# Carga condicional: sin GeoDjango en modo test
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    # Solo activar GeoDjango si no estamos ejecutando tests
    *([] if "test" in sys.argv else ["django.contrib.gis"]),
    "gtfs",
]

# =========================================
# MIDDLEWARE
# =========================================
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

# =========================================
# URLS / TEMPLATES
# =========================================
ROOT_URLCONF = "tests.urls"

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
    },
]

STATIC_URL = "/static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# =========================================
# DATABASE CONFIGURATION
# =========================================
if "test" in sys.argv:
    # -------------------------
    # Use in-memory SQLite for tests
    # -------------------------
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    }
else:
    # -------------------------
    # Default: PostGIS (for dev/prod)
    # -------------------------
    if os.getenv("USE_GIS", "1") == "1":
        DATABASES = {
            "default": {
                "ENGINE": "django.contrib.gis.db.backends.postgis",
                "NAME": "gtfs_test",
                "USER": "gepacam",
                "PASSWORD": "gepacam",
                "HOST": "localhost",
                "PORT": "5432",
            }
        }
    else:
        DATABASES = {
            "default": {
                "ENGINE": "django.db.backends.sqlite3",
                "NAME": os.path.join(BASE_DIR, "db.sqlite3"),
            }
        }

# =========================================
# SPATIALITE (solo necesario si se usa SQLite + GIS)
# =========================================
SPATIALITE_LIBRARY_PATH = "mod_spatialite"
