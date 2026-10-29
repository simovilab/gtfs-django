from django.contrib import admin
from django.contrib.gis.admin import GISModelAdmin
from .models import (
    GTFSProvider, Feed, Agency, Stop, Route, Calendar, CalendarDate,
    Shape, GeoShape, Trip, StopTime, FareAttribute, FareRule,
    FeedInfo, RouteStop, TripDuration, TripTime
)


@admin.register(GTFSProvider)
class GTFSProviderAdmin(admin.ModelAdmin):
    list_display = ('code', 'name', 'timezone', 'is_active')
    list_filter = ('is_active',)
    search_fields = ('code', 'name')
    fieldsets = (
        ('Basic Information', {
            'fields': ('code', 'name', 'description', 'website', 'timezone', 'is_active')
        }),
        ('Feed URLs', {
            'fields': (
                'schedule_url',
                'trip_updates_url',
                'vehicle_positions_url',
                'service_alerts_url'
            )
        }),
    )


@admin.register(Feed)
class FeedAdmin(admin.ModelAdmin):
    list_display = ('feed_id', 'gtfs_provider', 'is_current', 'retrieved_at')
    list_filter = ('is_current', 'gtfs_provider')
    search_fields = ('feed_id',)
    readonly_fields = ('retrieved_at',)
    date_hierarchy = 'retrieved_at'


@admin.register(Agency)
class AgencyAdmin(admin.ModelAdmin):
    list_display = ('agency_name', 'agency_id', 'feed', 'agency_timezone')
    list_filter = ('feed', 'agency_timezone')
    search_fields = ('agency_id', 'agency_name')
    fieldsets = (
        ('Identification', {
            'fields': ('feed', 'agency_id', 'agency_name')
        }),
        ('Contact Information', {
            'fields': ('agency_url', 'agency_phone', 'agency_email', 'agency_fare_url')
        }),
        ('Regional Settings', {
            'fields': ('agency_timezone', 'agency_lang')
        }),
    )


@admin.register(Stop)
class StopAdmin(GISModelAdmin):
    list_display = ('stop_id', 'stop_name', 'feed', 'stop_lat', 'stop_lon', 'location_type')
    list_filter = ('feed', 'location_type', 'wheelchair_boarding')
    search_fields = ('stop_id', 'stop_name', 'stop_code')
    readonly_fields = ('stop_point',)
    fieldsets = (
        ('Identification', {
            'fields': ('feed', 'stop_id', 'stop_code', 'stop_name')
        }),
        ('Location', {
            'fields': ('stop_lat', 'stop_lon', 'stop_point', 'zone_id')
        }),
        ('Type & Hierarchy', {
            'fields': ('location_type', 'parent_station')
        }),
        ('Accessibility', {
            'fields': ('wheelchair_boarding', 'platform_code')
        }),
        ('Amenities', {
            'fields': ('shelter', 'bench', 'lit', 'bay', 'device_charging_station'),
            'classes': ('collapse',)
        }),
        ('Additional Info', {
            'fields': ('stop_desc', 'stop_url', 'stop_heading', 'stop_timezone'),
            'classes': ('collapse',)
        }),
    )
    gis_widget_kwargs = {
        'attrs': {
            'default_zoom': 12,
        }
    }


@admin.register(Route)
class RouteAdmin(admin.ModelAdmin):
    list_display = ('route_short_name', 'route_long_name', 'route_type', 'feed', '_agency')
    list_filter = ('feed', 'route_type', '_agency')
    search_fields = ('route_id', 'route_short_name', 'route_long_name')
    readonly_fields = ('_agency',)
    fieldsets = (
        ('Identification', {
            'fields': ('feed', 'route_id', 'agency_id', '_agency')
        }),
        ('Names', {
            'fields': ('route_short_name', 'route_long_name', 'route_desc')
        }),
        ('Display', {
            'fields': ('route_type', 'route_color', 'route_text_color', 'route_sort_order')
        }),
        ('Additional', {
            'fields': ('route_url',),
            'classes': ('collapse',)
        }),
    )


@admin.register(Calendar)
class CalendarAdmin(admin.ModelAdmin):
    list_display = ('service_id', 'feed', 'start_date', 'end_date', 'weekday_summary')
    list_filter = ('feed', 'start_date', 'end_date')
    search_fields = ('service_id',)
    fieldsets = (
        ('Identification', {
            'fields': ('feed', 'service_id')
        }),
        ('Service Days', {
            'fields': (
                ('monday', 'tuesday', 'wednesday', 'thursday'),
                ('friday', 'saturday', 'sunday')
            )
        }),
        ('Date Range', {
            'fields': ('start_date', 'end_date')
        }),
    )
    
    def weekday_summary(self, obj):
        days = []
        if obj.monday: days.append('Mon')
        if obj.tuesday: days.append('Tue')
        if obj.wednesday: days.append('Wed')
        if obj.thursday: days.append('Thu')
        if obj.friday: days.append('Fri')
        if obj.saturday: days.append('Sat')
        if obj.sunday: days.append('Sun')
        return ', '.join(days) if days else 'No days'
    weekday_summary.short_description = 'Service Days'


@admin.register(CalendarDate)
class CalendarDateAdmin(admin.ModelAdmin):
    list_display = ('service_id', 'date', 'exception_type', 'holiday_name', 'feed')
    list_filter = ('feed', 'exception_type', 'date')
    search_fields = ('service_id', 'holiday_name')
    date_hierarchy = 'date'
    readonly_fields = ('_service',)


@admin.register(Shape)
class ShapeAdmin(admin.ModelAdmin):
    list_display = ('shape_id', 'shape_pt_sequence', 'feed', 'shape_pt_lat', 'shape_pt_lon')
    list_filter = ('feed',)
    search_fields = ('shape_id',)
    ordering = ('shape_id', 'shape_pt_sequence')


@admin.register(GeoShape)
class GeoShapeAdmin(GISModelAdmin):
    list_display = ('shape_id', 'feed', 'shape_name', 'has_altitude')
    list_filter = ('feed', 'has_altitude')
    search_fields = ('shape_id', 'shape_name')
    readonly_fields = ('geometry',)
    fieldsets = (
        ('Identification', {
            'fields': ('feed', 'shape_id')
        }),
        ('Metadata', {
            'fields': ('shape_name', 'shape_desc', 'shape_from', 'shape_to', 'has_altitude')
        }),
        ('Geometry', {
            'fields': ('geometry',)
        }),
    )


@admin.register(Trip)
class TripAdmin(admin.ModelAdmin):
    list_display = ('trip_id', 'route_id', 'service_id', 'trip_headsign', 'direction_id', 'feed')
    list_filter = ('feed', 'direction_id', 'wheelchair_accessible', 'bikes_allowed')
    search_fields = ('trip_id', 'trip_headsign', 'route_id')
    readonly_fields = ('_route', '_service', 'geoshape')
    fieldsets = (
        ('Identification', {
            'fields': ('feed', 'trip_id', 'route_id', '_route')
        }),
        ('Service', {
            'fields': ('service_id', '_service', 'direction_id')
        }),
        ('Display', {
            'fields': ('trip_headsign', 'trip_short_name')
        }),
        ('Shape & Block', {
            'fields': ('shape_id', 'geoshape', 'block_id'),
            'classes': ('collapse',)
        }),
        ('Accessibility', {
            'fields': ('wheelchair_accessible', 'bikes_allowed')
        }),
    )


@admin.register(StopTime)
class StopTimeAdmin(admin.ModelAdmin):
    list_display = ('trip_id', 'stop_sequence', 'stop_id', 'arrival_time', 'departure_time', 'feed')
    list_filter = ('feed', 'pickup_type', 'drop_off_type', 'timepoint')
    search_fields = ('trip_id', 'stop_id')
    readonly_fields = ('_trip', '_stop')
    ordering = ('trip_id', 'stop_sequence')
    fieldsets = (
        ('References', {
            'fields': ('feed', 'trip_id', '_trip', 'stop_id', '_stop')
        }),
        ('Sequence', {
            'fields': ('stop_sequence',)
        }),
        ('Times', {
            'fields': ('arrival_time', 'departure_time', 'timepoint')
        }),
        ('Pickup/Drop-off', {
            'fields': ('pickup_type', 'drop_off_type', 'stop_headsign')
        }),
        ('Distance', {
            'fields': ('shape_dist_traveled',),
            'classes': ('collapse',)
        }),
    )


@admin.register(FareAttribute)
class FareAttributeAdmin(admin.ModelAdmin):
    list_display = ('fare_id', 'price', 'currency_type', 'payment_method', 'feed')
    list_filter = ('feed', 'currency_type', 'payment_method', 'transfers')
    search_fields = ('fare_id',)
    readonly_fields = ('_agency',)
    fieldsets = (
        ('Identification', {
            'fields': ('feed', 'fare_id', 'agency_id', '_agency')
        }),
        ('Price', {
            'fields': ('price', 'currency_type', 'payment_method')
        }),
        ('Transfers', {
            'fields': ('transfers', 'transfer_duration')
        }),
    )


@admin.register(FareRule)
class FareRuleAdmin(admin.ModelAdmin):
    list_display = ('fare_id', 'route_id', 'origin_id', 'destination_id', 'feed')
    list_filter = ('feed',)
    search_fields = ('fare_id', 'route_id')
    readonly_fields = ('_fare', '_route')


@admin.register(FeedInfo)
class FeedInfoAdmin(admin.ModelAdmin):
    list_display = ('feed_publisher_name', 'feed_version', 'feed', 'feed_start_date', 'feed_end_date')
    list_filter = ('feed',)
    search_fields = ('feed_publisher_name', 'feed_version')
    fieldsets = (
        ('Identification', {
            'fields': ('feed', 'feed_publisher_name', 'feed_publisher_url')
        }),
        ('Version & Dates', {
            'fields': ('feed_version', 'feed_start_date', 'feed_end_date')
        }),
        ('Language & Contact', {
            'fields': ('feed_lang', 'feed_contact_email', 'feed_contact_url')
        }),
    )


@admin.register(RouteStop)
class RouteStopAdmin(admin.ModelAdmin):
    list_display = ('route_id', 'stop_id', 'stop_sequence', 'direction_id', 'timepoint', 'feed')
    list_filter = ('feed', 'direction_id', 'timepoint')
    search_fields = ('route_id', 'stop_id')
    readonly_fields = ('_route', '_shape', '_stop')
    ordering = ('route_id', 'shape_id', 'stop_sequence')


@admin.register(TripDuration)
class TripDurationAdmin(admin.ModelAdmin):
    list_display = ('route_id', 'service_id', 'start_time', 'end_time', 'stretch', 'stretch_duration', 'feed')
    list_filter = ('feed',)
    search_fields = ('route_id', 'service_id')
    readonly_fields = ('_route', '_shape', '_service')
    ordering = ('route_id', 'start_time', 'stretch')


@admin.register(TripTime)
class TripTimeAdmin(admin.ModelAdmin):
    list_display = ('trip_id', 'stop_id', 'stop_sequence', 'departure_time', 'feed')
    list_filter = ('feed',)
    search_fields = ('trip_id', 'stop_id')
    readonly_fields = ('_trip', '_stop')
    ordering = ('trip_id', 'stop_sequence')