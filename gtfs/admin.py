from django.contrib.gis import admin

from .models import (
    GTFSProvider,
    Feed,
    Agency,
    Stop,
    Route,
    Calendar,
    CalendarDate,
    Shape,
    GeoShape,
    Trip,
    StopTime,
    FareAttribute,
    FareRule,
    FeedInfo,
    RouteStop,
    TripDuration,
    TripTime,
    FeedMessage,
    TripUpdate,
    StopTimeUpdate,
    VehiclePosition,
)

# Register your models here.


class StopAdmin(admin.GISModelAdmin):
    exclude = ["stop_lat", "stop_lon"]


admin.site.register(GTFSProvider)
admin.site.register(Feed)
admin.site.register(Agency)
admin.site.register(Stop, StopAdmin)
admin.site.register(Route)
admin.site.register(Calendar)
admin.site.register(CalendarDate)
admin.site.register(Shape)
admin.site.register(GeoShape, admin.GISModelAdmin)
admin.site.register(Trip)
admin.site.register(StopTime)
admin.site.register(FareAttribute)
admin.site.register(FareRule)
admin.site.register(FeedInfo)
admin.site.register(RouteStop)
admin.site.register(TripDuration)
admin.site.register(TripTime)
admin.site.register(FeedMessage)
admin.site.register(TripUpdate)
admin.site.register(StopTimeUpdate)
admin.site.register(VehiclePosition, admin.GISModelAdmin)



#GTFS Schedule implementation
from .models_schedule import (
    FeedInfoSchedule,
    AgencySchedule,
    RouteSchedule,
    CalendarSchedule,
    TripSchedule,
    StopSchedule,
    StopTimeSchedule,
    ShapeSchedule,
    CalendarDateSchedule,
)

admin.site.register(FeedInfoSchedule)
admin.site.register(AgencySchedule)
admin.site.register(RouteSchedule)
admin.site.register(CalendarSchedule)
admin.site.register(TripSchedule)
admin.site.register(StopSchedule)
admin.site.register(StopTimeSchedule)
admin.site.register(ShapeSchedule)
admin.site.register(CalendarDateSchedule)
