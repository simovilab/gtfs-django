"""
GTFS Schedule v2.0 — Authoritative Django Models
Generated from schedule.json schema.
Compatible with Django 5.2 / SQLite backend (non-GIS).
"""

from django.db import models


# ==============================================================
# AGENCY
# ==============================================================
class AgencySchedule(models.Model):
    agency_id = models.CharField(max_length=64, primary_key=True)
    agency_name = models.CharField(max_length=255)
    agency_url = models.URLField()
    agency_timezone = models.CharField(max_length=64)
    agency_phone = models.CharField(max_length=64, blank=True, null=True)
    agency_email = models.EmailField(blank=True, null=True)

    class Meta:
        db_table = "schedule_agency"
        verbose_name = "Agency (Schedule)"
        verbose_name_plural = "Agencies (Schedule)"

    def __str__(self):
        return self.agency_name


# ==============================================================
# ROUTES
# ==============================================================
class RouteSchedule(models.Model):
    route_id = models.CharField(max_length=64, primary_key=True)
    agency = models.ForeignKey(
        AgencySchedule,
        on_delete=models.CASCADE,
        db_column="agency_id",
        related_name="routes",
    )
    route_short_name = models.CharField(max_length=64)
    route_long_name = models.CharField(max_length=255)
    route_desc = models.TextField(blank=True, null=True)
    route_type = models.IntegerField()
    route_color = models.CharField(max_length=6, blank=True, null=True)
    route_text_color = models.CharField(max_length=6, blank=True, null=True)

    class Meta:
        db_table = "schedule_routes"
        verbose_name = "Route (Schedule)"
        verbose_name_plural = "Routes (Schedule)"

    def __str__(self):
        return f"{self.route_short_name} - {self.route_long_name}"


# ==============================================================
# CALENDAR
# ==============================================================
class CalendarSchedule(models.Model):
    service_id = models.CharField(max_length=64, primary_key=True)
    monday = models.IntegerField(default=0)
    tuesday = models.IntegerField(default=0)
    wednesday = models.IntegerField(default=0)
    thursday = models.IntegerField(default=0)
    friday = models.IntegerField(default=0)
    saturday = models.IntegerField(default=0)
    sunday = models.IntegerField(default=0)
    start_date = models.DateField()
    end_date = models.DateField()

    class Meta:
        db_table = "schedule_calendar"
        verbose_name = "Calendar (Schedule)"
        verbose_name_plural = "Calendars (Schedule)"

    def __str__(self):
        return f"Service {self.service_id}"


# ==============================================================
# CALENDAR DATES
# ==============================================================
class CalendarDateSchedule(models.Model):
    service = models.ForeignKey(
        CalendarSchedule,
        on_delete=models.CASCADE,
        db_column="service_id",
        related_name="calendar_dates",
    )
    date = models.DateField()
    exception_type = models.IntegerField()

    class Meta:
        db_table = "schedule_calendar_dates"
        verbose_name = "Calendar Date (Schedule)"
        verbose_name_plural = "Calendar Dates (Schedule)"
        unique_together = ("service", "date")

    def __str__(self):
        return f"{self.service_id} - {self.date}"


# ==============================================================
# SHAPES
# ==============================================================
class ShapeSchedule(models.Model):
    shape_id = models.CharField(max_length=64)
    shape_pt_lat = models.FloatField()
    shape_pt_lon = models.FloatField()
    shape_pt_sequence = models.IntegerField()
    shape_dist_traveled = models.FloatField(blank=True, null=True)

    class Meta:
        db_table = "schedule_shapes"
        verbose_name = "Shape (Schedule)"
        verbose_name_plural = "Shapes (Schedule)"
        unique_together = ("shape_id", "shape_pt_sequence")

    def __str__(self):
        return f"Shape {self.shape_id} (pt {self.shape_pt_sequence})"


# ==============================================================
# STOPS
# ==============================================================
class StopSchedule(models.Model):
    stop_id = models.CharField(max_length=64, primary_key=True)
    stop_code = models.CharField(max_length=64, blank=True, null=True)
    stop_name = models.CharField(max_length=255)
    stop_desc = models.TextField(blank=True, null=True)
    stop_lat = models.FloatField()
    stop_lon = models.FloatField()
    zone_id = models.CharField(max_length=64, blank=True, null=True)
    location_type = models.IntegerField(default=0)
    parent_station = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        db_column="parent_station",
        null=True,
        blank=True,
        related_name="child_stops",
    )
    stop_timezone = models.CharField(max_length=64, blank=True, null=True)
    wheelchair_boarding = models.IntegerField(default=0)

    class Meta:
        db_table = "schedule_stops"
        verbose_name = "Stop (Schedule)"
        verbose_name_plural = "Stops (Schedule)"

    def __str__(self):
        return self.stop_name


# ==============================================================
# TRIPS
# ==============================================================
class TripSchedule(models.Model):
    trip_id = models.CharField(max_length=64, primary_key=True)
    route = models.ForeignKey(
        RouteSchedule,
        on_delete=models.CASCADE,
        db_column="route_id",
        related_name="trips",
    )
    service = models.ForeignKey(
        CalendarSchedule,
        on_delete=models.CASCADE,
        db_column="service_id",
        related_name="trips",
    )
    trip_headsign = models.CharField(max_length=255, blank=True, null=True)
    trip_short_name = models.CharField(max_length=255, blank=True, null=True)
    direction_id = models.IntegerField(default=0)
    block_id = models.CharField(max_length=64, blank=True, null=True)
    shape = models.ForeignKey(
        ShapeSchedule,
        on_delete=models.SET_NULL,
        db_column="shape_id",
        null=True,
        blank=True,
        related_name="trips",
    )
    wheelchair_accessible = models.IntegerField(default=0)

    class Meta:
        db_table = "schedule_trips"
        verbose_name = "Trip (Schedule)"
        verbose_name_plural = "Trips (Schedule)"

    def __str__(self):
        return f"Trip {self.trip_id}"


# ==============================================================
# STOP TIMES
# ==============================================================
class StopTimeSchedule(models.Model):
    trip = models.ForeignKey(
        TripSchedule,
        on_delete=models.CASCADE,
        db_column="trip_id",
        related_name="stop_times",
    )
    stop = models.ForeignKey(
        StopSchedule,
        on_delete=models.CASCADE,
        db_column="stop_id",
        related_name="stop_times",
    )
    stop_sequence = models.IntegerField()
    arrival_time = models.CharField(max_length=16)
    departure_time = models.CharField(max_length=16)
    stop_headsign = models.CharField(max_length=255, blank=True, null=True)
    pickup_type = models.IntegerField(default=0)
    drop_off_type = models.IntegerField(default=0)
    shape_dist_traveled = models.FloatField(blank=True, null=True)
    timepoint = models.IntegerField(default=0)

    class Meta:
        db_table = "schedule_stop_times"
        verbose_name = "Stop Time (Schedule)"
        verbose_name_plural = "Stop Times (Schedule)"
        unique_together = ("trip", "stop_sequence")

    def __str__(self):
        return f"{self.trip_id} - seq {self.stop_sequence}"


# ==============================================================
# FEED INFO
# ==============================================================
class FeedInfoSchedule(models.Model):
    feed_publisher_name = models.CharField(max_length=255)
    feed_publisher_url = models.URLField()
    feed_lang = models.CharField(max_length=8)
    feed_version = models.CharField(max_length=64)
    feed_start_date = models.DateField(blank=True, null=True)
    feed_end_date = models.DateField(blank=True, null=True)
    feed_contact_email = models.EmailField(blank=True, null=True)
    feed_contact_url = models.URLField(blank=True, null=True)

    class Meta:
        db_table = "schedule_feed_info"
        verbose_name = "Feed Info (Schedule)"
        verbose_name_plural = "Feed Info (Schedule)"

    def __str__(self):
        return f"{self.feed_publisher_name} ({self.feed_lang})"
