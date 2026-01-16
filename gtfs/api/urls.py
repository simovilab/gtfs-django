from django.urls import include, path
from rest_framework import routers
from rest_framework.authtoken.views import obtain_auth_token
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView

from . import views


router = routers.DefaultRouter()
# GTFS Schedule
router.register(r"agency", views.AgencyViewSet)
router.register(r"stops", views.StopViewSet)
#router.register(r"geo-stops", views.GeoStopViewSet, basename="geo-stop")
router.register(r"shapes", views.ShapeViewSet)
#router.register(r"geo-shapes", views.GeoShapeViewSet)
router.register(r"routes", views.RouteViewSet)
router.register(r"calendars", views.CalendarViewSet)
router.register(r"calendar-dates", views.CalendarDateViewSet)
router.register(r"trips", views.TripViewSet)
router.register(r"stop-times", views.StopTimeViewSet)
router.register(r"fare-attributes", views.FareAttributeViewSet)
router.register(r"fare-rules", views.FareRuleViewSet)
router.register(r"feed-info", views.FeedInfoViewSet)

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    path("", include(router.urls)),
]

# router.register(r"geo-stops", views.GeoStopViewSet, basename="geo-stop")
# router.register(r"geo-shapes", views.GeoShapeViewSet)
