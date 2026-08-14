import os
from celery import Celery
from django.conf import settings

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ingestproj.settings")
app = Celery("ingestproj")

# All Celery config via Django settings/env
app.conf.broker_url = settings.REDIS_URL
app.conf.result_backend = settings.REDIS_URL
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 4
# Bound per-child memory growth: recycle a worker process after this many
# tasks rather than letting it run forever (defense in depth alongside the
# spool + expires fix for the OOM incident).
app.conf.worker_max_tasks_per_child = 200
app.conf.task_routes = {
    "rt_pipeline.tasks.poll_vehicle_positions_s3": {"queue": "fetch"},
    "rt_pipeline.tasks.fetch_vehicle_positions": {"queue": "fetch"},
    "rt_pipeline.tasks.parse_and_upsert_vehicle_positions": {"queue": "upsert"},
    "rt_pipeline.tasks.fetch_trip_updates": {"queue": "fetch"},
    "rt_pipeline.tasks.parse_and_upsert_trip_updates": {"queue": "upsert"},
    # The hourly flush MUST share the poll worker's queue. DuckDB allows one
    # read-write process per database file, and the flush deletes from the very
    # spool the poll task inserts into — running it on a second worker means
    # whichever opens the file second dies on the lock. `fetch` runs at
    # --concurrency=1, so poll and flush serialise inside one process. The
    # flush is a couple of S3 PUTs; the polls it delays are dropped by
    # `expires` rather than queued.
    "rt_pipeline.tasks.flush_vp_spool_s3": {"queue": "fetch"},
    # Compaction only ever touches S3, never the spool, so it is safe (and
    # preferable) on its own worker — it runs for minutes.
    "rt_pipeline.tasks.compact_vp_day": {"queue": "maint"},
}
app.autodiscover_tasks()


