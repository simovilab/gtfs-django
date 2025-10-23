# gtfs-rt-pipeline/sch_pipeline/utils.py
from django.db.models import Count
from sch_pipeline.models import Trip

def top_routes_by_scheduled_trips(n: int = 5) -> list[str]:
    """
    Returns route_ids for the top-N busiest routes by number of scheduled trips.
    Stable, fast, and independent of realtime ingestion volume.
    """
    qs = (
        Trip.objects
        # .filter(feed__provider_id=provider_id)
        .values("route_id")
        .annotate(trips_count=Count("id"))
        .order_by("-trips_count")[:n]
    )
    return [row["route_id"] for row in qs]
