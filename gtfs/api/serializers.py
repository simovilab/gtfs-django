
from gtfs.models import *
from rest_framework import serializers
from rest_framework_gis.serializers import GeoFeatureModelSerializer, GeometryField


class FeedSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Feed
        fields = "__all__"


# -------------
# GTFS Schedule
# -------------


class AgencySerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = Agency
        fields = "__all__"


class StopSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = Stop
        fields = "__all__"

""""
class GeoStopSerializer(GeoFeatureModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)
    stop_point = GeometryField()

    class Meta:
        model = Stop
        geo_field = "stop_point"
        fields = "__all__"

"""
class RouteSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = Route
        fields = "__all__"


class CalendarSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = Calendar
        fields = "__all__"


class CalendarDateSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = CalendarDate
        fields = "__all__"


class ShapeSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = Shape
        fields = "__all__"

"""
class GeoShapeSerializer(GeoFeatureModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)
    geometry = GeometryField()

    class Meta:
        model = GeoShape
        geo_field = "geometry"
        fields = "__all__"
"""

class TripSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = Trip
        fields = "__all__"


class StopTimeSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = StopTime
        fields = "__all__"


class FareAttributeSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = FareAttribute
        fields = "__all__"


class FareRuleSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = FareRule
        fields = "__all__"


class FeedInfoSerializer(serializers.HyperlinkedModelSerializer):
    feed = serializers.PrimaryKeyRelatedField(read_only=True)

    class Meta:
        model = FeedInfo
        fields = "__all__"


