from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator, RegexValidator

# ─────────────────────────────
# 1. FeedInfoSchedule
# ─────────────────────────────
class FeedInfoSchedule(models.Model):
    feed_publisher_name = models.CharField(max_length=200)
    feed_publisher_url = models.URLField()
    feed_lang = models.CharField(max_length=10)
    feed_version = models.CharField(max_length=50, null=True, blank=True)
    feed_start_date = models.DateField(null=True, blank=True)
    feed_end_date = models.DateField(null=True, blank=True)
    feed_contact_email = models.EmailField(null=True, blank=True)
    feed_contact_url = models.URLField(null=True, blank=True)

    def __str__(self):
        return f"FeedInfoSchedule {self.feed_publisher_name}"


# ─────────────────────────────
# 2. AgencySchedule
# ─────────────────────────────
class AgencySchedule(models.Model):
    feed = models.ForeignKey(FeedInfoSchedule, on_delete=models.CASCADE, related_name="agencies")
    agency_id = models.CharField(max_length=50)
    agency_name = models.CharField(max_length=200)
    agency_url = models.URLField()
    agency_timezone = models.CharField(max_length=50)
    agency_phone = models.CharField(max_length=20, null=True, blank=True)
    agency_email = models.EmailField(null=True, blank=True)

    class Meta:
        unique_together = ('feed', 'agency_id')

    def __str__(self):
        return self.agency_name


# ─────────────────────────────
# 3. RouteSchedule
# ─────────────────────────────
class RouteSchedule(models.Model):
    feed = models.ForeignKey(FeedInfoSchedule, on_delete=models.CASCADE, related_name="routes")
    route_id = models.CharField(max_length=50)
    agency = models.ForeignKey(AgencySchedule, on_delete=models.CASCADE, related_name="routes")
    route_short_name = models.CharField(max_length=50)
    route_long_name = models.CharField(max_length=255)
    route_desc = models.TextField(null=True, blank=True)
    route_type = models.IntegerField(validators=[MinValueValidator(0), MaxValueValidator(12)])
    route_color = models.CharField(
        max_length=6,
        validators=[RegexValidator(r"^[0-9A-Fa-f]{6}$", "Debe ser un color HEX válido.")],
        null=True,
        blank=True,
    )
    route_text_color = models.CharField(
        max_length=6,
        validators=[RegexValidator(r"^[0-9A-Fa-f]{6}$")],
        null=True,
        blank=True,
    )

    class Meta:
        unique_together = ('feed', 'route_id')

    def __str__(self):
        return f"{self.route_short_name} - {self.route_long_name}"


# ─────────────────────────────
# 4. CalendarSchedule
# ─────────────────────────────
class CalendarSchedule(models.Model):
    feed = models.ForeignKey(FeedInfoSchedule, on_delete=models.CASCADE, related_name="calendars")
    service_id = models.CharField(max_length=50)
    monday = models.IntegerField(choices=[(0, "No service"), (1, "Service runs")])
    tuesday = models.IntegerField(choices=[(0, "No service"), (1, "Service runs")])
    wednesday = models.IntegerField(choices=[(0, "No service"), (1, "Service runs")])
    thursday = models.IntegerField(choices=[(0, "No service"), (1, "Service runs")])
    friday = models.IntegerField(choices=[(0, "No service"), (1, "Service runs")])
    saturday = models.IntegerField(choices=[(0, "No service"), (1, "Service runs")])
    sunday = models.IntegerField(choices=[(0, "No service"), (1, "Service runs")])
    start_date = models.DateField()
    end_date = models.DateField()

    class Meta:
        unique_together = ('feed', 'service_id')

    def __str__(self):
        return self.service_id


# ─────────────────────────────
# 5. CalendarDateSchedule (sin CPK)
# ─────────────────────────────
class CalendarDateSchedule(models.Model):
    feed = models.ForeignKey('FeedInfoSchedule', on_delete=models.CASCADE, related_name="calendar_dates")
    service = models.ForeignKey('CalendarSchedule', on_delete=models.CASCADE, related_name="calendar_dates")
    date = models.DateField()
    exception_type = models.IntegerField(choices=[(1, "Service added"), (2, "Service removed")])

    class Meta:
        unique_together = ('service', 'date')

    def __str__(self):
        return f"{self.service.service_id} - {self.date}"


# ─────────────────────────────
# 6. StopSchedule
# ─────────────────────────────
class StopSchedule(models.Model):
    feed = models.ForeignKey(FeedInfoSchedule, on_delete=models.CASCADE, related_name="stops")
    stop_id = models.CharField(max_length=50)
    stop_code = models.CharField(max_length=50, null=True, blank=True)
    stop_name = models.CharField(max_length=200)
    stop_desc = models.TextField(null=True, blank=True)
    stop_lat = models.FloatField(validators=[MinValueValidator(-90), MaxValueValidator(90)])
    stop_lon = models.FloatField(validators=[MinValueValidator(-180), MaxValueValidator(180)])
    zone_id = models.CharField(max_length=50, null=True, blank=True)
    location_type = models.IntegerField(
        choices=[(0, "Stop"), (1, "Station"), (2, "Entrance/Exit"), (3, "Generic Node"), (4, "Boarding Area")],
        default=0
    )
    parent_station = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True)
    stop_timezone = models.CharField(max_length=50, null=True, blank=True)
    wheelchair_boarding = models.IntegerField(
        choices=[(0, "No info"), (1, "Accessible"), (2, "Not accessible")],
        default=0
    )

    class Meta:
        unique_together = ('feed', 'stop_id')

    def __str__(self):
        return self.stop_name


# ─────────────────────────────
# 7. ShapeSchedule (sin CPK)
# ─────────────────────────────
class ShapeSchedule(models.Model):
    feed = models.ForeignKey('FeedInfoSchedule', on_delete=models.CASCADE, related_name="shapes")
    shape_id = models.CharField(max_length=50)
    shape_pt_sequence = models.PositiveIntegerField()
    shape_pt_lat = models.FloatField(validators=[MinValueValidator(-90), MaxValueValidator(90)])
    shape_pt_lon = models.FloatField(validators=[MinValueValidator(-180), MaxValueValidator(180)])
    shape_dist_traveled = models.FloatField(validators=[MinValueValidator(0)], null=True, blank=True)

    class Meta:
        unique_together = ('shape_id', 'shape_pt_sequence')

    def __str__(self):
        return f"{self.shape_id} ({self.shape_pt_sequence})"


# ─────────────────────────────
# 8. TripSchedule
# ─────────────────────────────
class TripSchedule(models.Model):
    feed = models.ForeignKey(FeedInfoSchedule, on_delete=models.CASCADE, related_name="trips")
    trip_id = models.CharField(max_length=50)
    route = models.ForeignKey(RouteSchedule, on_delete=models.CASCADE, related_name="trips")
    service = models.ForeignKey(CalendarSchedule, on_delete=models.CASCADE, related_name="trips")
    trip_headsign = models.CharField(max_length=255, null=True, blank=True)
    trip_short_name = models.CharField(max_length=50, null=True, blank=True)
    direction_id = models.IntegerField(choices=[(0, "Outbound"), (1, "Inbound")], null=True, blank=True)
    block_id = models.CharField(max_length=50, null=True, blank=True)
    shape = models.ForeignKey(ShapeSchedule, on_delete=models.SET_NULL, null=True, blank=True)
    wheelchair_accessible = models.IntegerField(
        choices=[(0, "No info"), (1, "Accessible"), (2, "Not accessible")],
        default=0
    )

    class Meta:
        unique_together = ('feed', 'trip_id')

    def __str__(self):
        return f"{self.trip_id} - {self.trip_headsign or self.trip_short_name or ''}"


# ─────────────────────────────
# 9. StopTimeSchedule (sin CPK)
# ─────────────────────────────
class StopTimeSchedule(models.Model):
    feed = models.ForeignKey('FeedInfoSchedule', on_delete=models.CASCADE, related_name="stop_times")
    trip = models.ForeignKey('TripSchedule', on_delete=models.CASCADE, related_name="stop_times")
    stop = models.ForeignKey('StopSchedule', on_delete=models.CASCADE, related_name="stop_times")
    arrival_time = models.TimeField()
    departure_time = models.TimeField()
    stop_sequence = models.IntegerField()
    stop_headsign = models.CharField(max_length=255, null=True, blank=True)
    pickup_type = models.IntegerField(
        choices=[(0, "Regular"), (1, "No pickup"), (2, "Phone agency"), (3, "Coordinate with driver")],
        default=0
    )
    drop_off_type = models.IntegerField(
        choices=[(0, "Regular"), (1, "No drop off"), (2, "Phone agency"), (3, "Coordinate with driver")],
        default=0
    )
    shape_dist_traveled = models.FloatField(null=True, blank=True)
    timepoint = models.IntegerField(choices=[(0, "Approximate"), (1, "Exact")], default=1)

    class Meta:
        unique_together = ('trip', 'stop_sequence')

    def __str__(self):
        return f"{self.trip.trip_id} - seq {self.stop_sequence}"
