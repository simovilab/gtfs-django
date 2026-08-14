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
POLL_SECONDS = env.int("POLL_SECONDS", default=5)  # matches MBTA VP refresh
HTTP_CONNECT_TIMEOUT = env.float("HTTP_CONNECT_TIMEOUT", default=3.0)
HTTP_READ_TIMEOUT = env.float("HTTP_READ_TIMEOUT", default=5.0)

# --- S3 VehiclePosition sink (Hive-partitioned Parquet) ---
# When enabled, each parsed VP batch is also written to the S3 store
# (dual-write alongside Postgres). Credentials come from AWS_* env vars
# (loaded from .env into os.environ above). Empty base URI -> storage default
# (s3://transit/feeds/mbta/vehicle_positions).
S3_VP_SINK_ENABLED = env.bool("S3_VP_SINK_ENABLED", default=False)
S3_VP_BASE_URI = env("S3_VP_BASE_URI", default="")

# --- Local spool (poll -> spool -> hourly S3 flush) ---
# Polls append here (~200ms local DuckDB insert) instead of writing to S3
# directly. DuckDB is single-writer, so the poll queue must run at
# --concurrency=1. See rt_pipeline/storage/spool.py.
SPOOL_PATH = env("SPOOL_PATH", default="/data/spool/vp_spool.duckdb")

# --- Observability status files (atomic, cat/tail-able) ---
# See rt_pipeline/status.py.
STATUS_DIR = env("STATUS_DIR", default="/var/lib/simovi/status")

# --- Hourly spool -> S3 staging flush ---
# One Parquet object per (year, month, day) -- no route_id fan-out. Daily
# compaction re-partitions staging into the curated route_id-partitioned
# layout at S3_VP_BASE_URI.
S3_VP_STAGING_BASE_URI = env(
    "S3_VP_STAGING_BASE_URI",
    default="s3://transit/feeds/mbta/vehicle_positions_staging",
)
SPOOL_FLUSH_MINUTE = env.int("SPOOL_FLUSH_MINUTE", default=2)

# --- Daily staging -> curated compaction ---
COMPACT_HOUR_UTC = env.int("COMPACT_HOUR_UTC", default=3)
COMPACT_MINUTE = env.int("COMPACT_MINUTE", default=15)

# --- Weekly static GTFS snapshots (roadmap 0.2) ---
# Dated, unparsed zip per agency so realtime observations collected during
# the replication window can be matched back to the schedule in effect at
# the time. bUCR's feed is served by SIMOVI itself, not the agency, so its
# URL is expected to move -- always read from env, never hardcode.
MBTA_GTFS_STATIC_URL = env(
    "MBTA_GTFS_STATIC_URL", default="https://cdn.mbta.com/MBTA_GTFS.zip"
)
BUCR_GTFS_STATIC_URL = env(
    "BUCR_GTFS_STATIC_URL", default="https://feeds.simovi.org/bucr/schedule/gtfs.zip"
)
STATIC_GTFS_SNAPSHOT_DOW = env.int("STATIC_GTFS_SNAPSHOT_DOW", default=1)  # Monday
STATIC_GTFS_SNAPSHOT_HOUR_UTC = env.int("STATIC_GTFS_SNAPSHOT_HOUR_UTC", default=4)
STATIC_GTFS_SNAPSHOT_MINUTE = env.int("STATIC_GTFS_SNAPSHOT_MINUTE", default=0)
