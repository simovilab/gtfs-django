from django.contrib.gis import admin  # noqa: F401

# Concrete GTFS models now live in the consuming app (e.g. Databús `feed/`,
# the ETA suite `sch_pipeline/`), which subclass the abstract `Base*` models
# from `gtfs.models`. Register those models in that app's admin, not here.
