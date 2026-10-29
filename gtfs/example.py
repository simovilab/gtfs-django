from django.db import models
from gtfs.models import (
    BaseAgency,
    BaseRoute,
    BaseCalendar,
    BaseCalendarDate,
    BaseShape,
    BaseTrip,
    BaseStop,
    BaseStopTime,
)
from gtfs.fields import GTFSColorField


class Feed(models.Model):
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)


class Agency(BaseAgency):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)


class Route(BaseRoute):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    agency = models.ForeignKey(Agency, on_delete=models.CASCADE)


class Calendar(BaseCalendar):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)


class CalendarDate(BaseCalendarDate):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    service = models.ForeignKey(Calendar, on_delete=models.CASCADE)
    holiday_name = models.CharField(
        max_length=255,
        help_text="Name of the holiday (non-GTFS field)",
    )


class Shape(BaseShape):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    shape_color = GTFSColorField(
        null=True,
        blank=True,
        help_text="Color of the shape for display in maps (non-GTFS field)",
    )


class Trip(BaseTrip):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    route = models.ForeignKey(Route, on_delete=models.CASCADE)
    service = models.ForeignKey(Calendar, on_delete=models.CASCADE)


class Stop(BaseStop):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)


class StopTime(BaseStopTime):
    feed = models.ForeignKey(Feed, on_delete=models.CASCADE)
    trip = models.ForeignKey(Trip, on_delete=models.CASCADE)
    stop = models.ForeignKey(Stop, on_delete=models.CASCADE)
