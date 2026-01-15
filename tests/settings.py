# tests/settings.py

import os
from pathlib import Path

# --------------------------------------------
# BASE CONFIGURATION
# --------------------------------------------
BASE_DIR = Path(__file__).resolve().parent

SECRET_KEY = "test-secret-key"
DEBUG = True
ALLOWED_HOSTS = ["*"]

USE_TZ = True
TIME_ZONE = "UTC"

# --------------------------------------------
# INSTALLED APPS
# --------------------------------------------
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.gis",  # GeoDjango enabled
    "gtfs",                # GTFS Schedule app
]

# --------------------------------------------
# MIDDLEWARE
# --------------------------------------------
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

# --------------------------------------------
# URLS / TEMPLATES / DEFAULTS
# --------------------------------------------
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
    }
]

STATIC_URL = "/static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# --------------------------------------------
# DATABASE: GeoDjango 
# --------------------------------------------
DATABASES = {
    "default": {
        "ENGINE": "django.contrib.gis.db.backends.postgis",
        "NAME": "gtfs_test",          # nombre de la BD que se crea
        "USER": "geovanny",           # usuario de la BD
        "PASSWORD": "postgres",       # contraseña (se puede cambiar)
        "HOST": "localhost",
        "PORT": "5432",
    }
}


SPATIALITE_LIBRARY_PATH = "mod_spatialite"

# 💡 Evita triggers ISO con 'rowid'
SPATIAL_REF_SYS_TABLE = "spatial_ref_sys"
SPATIALITE_INIT_COMMANDS = [
    "SELECT InitSpatialMetaData(1);"  # Safe initialization
]


# --------------------------------------------
# ENVIRONMENT VARIABLES FOR GDAL
# --------------------------------------------
os.environ["GDAL_LIBRARY_PATH"] = "/usr/lib/libgdal.so.30"

