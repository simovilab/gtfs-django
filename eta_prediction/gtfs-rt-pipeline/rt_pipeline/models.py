from django.db import models
import uuid

class RawMessage(models.Model):
    # Message type choices
    MESSAGE_TYPE_VEHICLE_POSITIONS = 'VP'
    MESSAGE_TYPE_TRIP_UPDATES = 'TU'
    MESSAGE_TYPE_CHOICES = [
        (MESSAGE_TYPE_VEHICLE_POSITIONS, 'Vehicle Positions'),
        (MESSAGE_TYPE_TRIP_UPDATES, 'Trip Updates'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    feed_name = models.TextField()
    message_type = models.CharField(max_length=3, choices=MESSAGE_TYPE_CHOICES)
    fetched_at = models.DateTimeField(auto_now_add=True)
    header_timestamp = models.DateTimeField(null=True, blank=True)
    incrementality = models.TextField(null=True, blank=True)
    content = models.BinaryField()
    content_hash = models.CharField(max_length=64)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["feed_name", "message_type", "content_hash"], 
                name="uq_feed_type_hash"
            )
        ]
        indexes = [
            models.Index(fields=["feed_name", "message_type", "-fetched_at"]),
        ]

class VehiclePosition(models.Model):
    feed_name = models.TextField()
    vehicle_id = models.TextField()
    ts = models.DateTimeField()
    lat = models.FloatField(null=True, blank=True)
    lon = models.FloatField(null=True, blank=True)
    bearing = models.FloatField(null=True, blank=True)
    speed = models.FloatField(null=True, blank=True)
    route_id = models.TextField(null=True, blank=True)
    trip_id = models.TextField(null=True, blank=True)
    current_stop_sequence = models.IntegerField(null=True, blank=True)
    raw_message = models.ForeignKey(RawMessage, null=True, on_delete=models.SET_NULL)
    ingested_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["feed_name", "vehicle_id", "ts"], name="uq_vp_natkey"
            )
        ]
        indexes = [
            models.Index(fields=["vehicle_id", "-ts"]),
            models.Index(fields=["route_id", "-ts"]),
        ]

class TripUpdate(models.Model):
    feed_name = models.TextField()

    # trip_update header-level
    ts = models.DateTimeField(null=True, blank=True)  # entity.trip_update.timestamp (UTC)
    trip_id = models.TextField(null=True, blank=True)
    route_id = models.TextField(null=True, blank=True)
    start_time = models.TextField(null=True, blank=True)
    start_date = models.TextField(null=True, blank=True)
    schedule_relationship = models.TextField(null=True, blank=True)  # header-level SR

    vehicle_id = models.TextField(null=True, blank=True)

    stop_sequence = models.IntegerField(null=True, blank=True)
    stop_id = models.TextField(null=True, blank=True)
    arrival_delay = models.IntegerField(null=True, blank=True)
    arrival_time = models.DateTimeField(null=True, blank=True)      # UTC
    departure_delay = models.IntegerField(null=True, blank=True)
    departure_time = models.DateTimeField(null=True, blank=True)    # UTC
    stu_schedule_relationship = models.TextField(null=True, blank=True)

    raw_message = models.ForeignKey('RawMessage', null=True, on_delete=models.SET_NULL)
    ingested_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["feed_name", "trip_id", "ts", "stop_sequence"],
                name="uq_tu_natkey",
            )
        ]
        indexes = [
            models.Index(fields=["trip_id", "-ts"]),
            models.Index(fields=["route_id", "-ts"]),
            models.Index(fields=["vehicle_id", "-ts"]),
            models.Index(fields=["stop_id", "-ts"]),
        ]