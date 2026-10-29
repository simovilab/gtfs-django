import os
from celery import Celery
from celery.schedules import crontab
from django.conf import settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ingestproj.settings")
app = Celery("ingestproj")

# All Celery config via Django settings/env
app.conf.broker_url = settings.REDIS_URL
app.conf.result_backend = settings.REDIS_URL
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 4
app.conf.task_routes = {
    "rt_pipeline.tasks.fetch_vehicle_positions": {"queue": "fetch"},
    "rt_pipeline.tasks.parse_and_upsert_vehicle_positions": {"queue": "upsert"},
    "rt_pipeline.tasks.fetch_trip_updates": {"queue": "fetch"},
    "rt_pipeline.tasks.parse_and_upsert_trip_updates": {"queue": "upsert"},
    'fetch-gtfs-schedule': {
    'task': 'gtfs_static.tasks.fetch_and_import_gtfs_schedule',
    'schedule': crontab(hour=3, minute=0),
    'options': {'queue': 'static'}
    }
}
app.autodiscover_tasks()


