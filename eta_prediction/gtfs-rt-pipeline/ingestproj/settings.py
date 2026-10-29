import environ, os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
env = environ.Env()
environ.Env.read_env(BASE_DIR / ".env")

SECRET_KEY = env("DJANGO_SECRET_KEY", default="dev-key")
DEBUG = env.bool("DJANGO_DEBUG", default=True)
ALLOWED_HOSTS = [h.strip() for h in env("DJANGO_ALLOWED_HOSTS", default="*").split(",")]

INSTALLED_APPS = [
    "django.contrib.admin", "django.contrib.auth", "django.contrib.contenttypes",
    "django.contrib.sessions", "django.contrib.messages", "django.contrib.staticfiles", "django.contrib.gis",
    "rt_pipeline", "sch_pipeline"
]

# Admin/templates config (required for admin)
TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],               # you can add template dirs later if you need
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

# Silence the auto field warning (recommended)
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "ingestproj.urls"
WSGI_APPLICATION = "ingestproj.wsgi.application"


DATABASES = {"default": env.db("DATABASE_URL")}
DATABASES["default"]["ENGINE"] = "django.contrib.gis.db.backends.postgis"


STATIC_URL = "static/"

# Celery via env (set in celery.py as well)
REDIS_URL = env("REDIS_URL")
FEED_NAME = env("FEED_NAME")
GTFSRT_VEHICLE_POSITIONS_URL = env("GTFSRT_VEHICLE_POSITIONS_URL")
GTFSRT_TRIP_UPDATES_URL = env("GTFSRT_TRIP_UPDATES_URL")
POLL_SECONDS = env.int("POLL_SECONDS", default=15)
HTTP_CONNECT_TIMEOUT = env.float("HTTP_CONNECT_TIMEOUT", default=3.0)
HTTP_READ_TIMEOUT = env.float("HTTP_READ_TIMEOUT", default=5.0)

from celery.schedules import schedule

# CELERY_BEAT_SCHEDULE = {
#     # existing vehicle positions schedule
#     "poll-vehicle-positions": {
#         "task": "rt_pipeline.tasks.fetch_vehicle_positions",
#         "schedule": schedule(run_every=POLL_SECONDS),
#     },
#     # NEW — trip updates
#     "poll-trip-updates": {
#         "task": "rt_pipeline.tasks.fetch_trip_updates",
#         "schedule": schedule(run_every=POLL_SECONDS),
#     },
# }
