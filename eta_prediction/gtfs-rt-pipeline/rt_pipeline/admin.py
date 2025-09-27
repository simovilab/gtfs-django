from django.contrib import admin
from .models import RawMessage, VehiclePosition, TripUpdate

@admin.register(RawMessage)
class RawMessageAdmin(admin.ModelAdmin):
    list_display = ("feed_name", "fetched_at", "header_timestamp", "incrementality", "content_hash")
    search_fields = ("feed_name", "content_hash")
    list_filter = ("feed_name", "incrementality")
    date_hierarchy = "fetched_at"
    ordering = ("-fetched_at",)

    def __str__(self): return f"{self.feed_name} @ {self.fetched_at:%Y-%m-%d %H:%M:%S}"

@admin.register(VehiclePosition)
class VehiclePositionAdmin(admin.ModelAdmin):
    list_display = ("feed_name", "vehicle_id", "ts", "lat", "lon", "route_id", "trip_id", "speed")
    search_fields = ("vehicle_id", "route_id", "trip_id", "feed_name")
    list_filter = ("feed_name", "route_id")
    date_hierarchy = "ts"
    ordering = ("-ts",)

    def __str__(self): return f"{self.feed_name}:{self.vehicle_id} @ {self.ts:%Y-%m-%d %H:%M:%S}"

@admin.register(TripUpdate)
class TripUpdateAdmin(admin.ModelAdmin):
    list_display = (
        "feed_name",
        "trip_id",
        "route_id",
        "vehicle_id",
        "ts",
        "stop_id",
        "stop_sequence",
        "arrival_time",
        "departure_time",
        "arrival_delay",
        "departure_delay",
    )
    search_fields = ("trip_id", "route_id", "vehicle_id", "stop_id", "feed_name")
    list_filter = ("feed_name", "route_id")
    date_hierarchy = "ts"
    ordering = ("-ts",)

    def __str__(self):
        return f"{self.feed_name}:{self.trip_id} @ {self.ts:%Y-%m-%d %H:%M:%S}"