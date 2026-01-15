# gtfs/tests/test_schedule_crud.py
from django.test import TestCase
from gtfs.models import (
    FeedInfoSchedule,
    AgencySchedule,
    RouteSchedule,
    CalendarSchedule,
    TripSchedule,
    StopSchedule,
    StopTimeSchedule,
    ShapeSchedule,
    CalendarDateSchedule
)

class ScheduleCRUDTests(TestCase):
    """Integration test for GTFS Schedule models (no composite PKs)."""

    def setUp(self):
        # Crear feed base
        self.feed = FeedInfoSchedule.objects.create(
            feed_publisher_name="UCR Feed",
            feed_publisher_url="https://ucr.ac.cr",
            feed_lang="es",
            feed_version="v1.0"
        )

        # Crear agencia
        self.agency = AgencySchedule.objects.create(
            feed=self.feed,
            agency_id="UCR",
            agency_name="Universidad de Costa Rica",
            agency_url="https://ucr.ac.cr",
            agency_timezone="America/Costa_Rica"
        )

        # Crear ruta
        self.route = RouteSchedule.objects.create(
            feed=self.feed,
            route_id="R1",
            agency=self.agency,
            route_short_name="1",
            route_long_name="Campus a San Pedro",
            route_type=3
        )

        # Crear calendario
        self.calendar = CalendarSchedule.objects.create(
            feed=self.feed,
            service_id="S2025",
            monday=1, tuesday=1, wednesday=1, thursday=1, friday=1,
            saturday=0, sunday=0,
            start_date="2025-03-01", end_date="2025-12-31"
        )

        # Crear shape
        self.shape = ShapeSchedule.objects.create(
            feed=self.feed,
            shape_id="Shape1",
            shape_pt_lat=9.936,
            shape_pt_lon=-84.054,
            shape_pt_sequence=1
        )

        # Crear parada
        self.stop = StopSchedule.objects.create(
            feed=self.feed,
            stop_id="SP01",
            stop_name="Parada San Pedro",
            stop_lat=9.936,
            stop_lon=-84.054
        )

        # Crear viaje
        self.trip = TripSchedule.objects.create(
            feed=self.feed,
            trip_id="T100",
            route=self.route,
            service=self.calendar,
            trip_headsign="San Pedro",
            shape=self.shape
        )

        # Crear stop_time
        self.stoptime = StopTimeSchedule.objects.create(
            feed=self.feed,
            trip=self.trip,
            stop=self.stop,
            arrival_time="07:30:00",
            departure_time="07:31:00",
            stop_sequence=1
        )

        # Crear calendar_date
        self.cal_date = CalendarDateSchedule.objects.create(
            feed=self.feed,
            service=self.calendar,
            date="2025-04-01",
            exception_type=1
        )

    def test_crud_integrity(self):
        """Verifica que las relaciones básicas funcionen correctamente."""
        # Feed -> Agency
        self.assertEqual(self.feed.agencies.count(), 1)
        self.assertEqual(self.feed.agencies.first().agency_id, "UCR")

        # Agency -> Route
        self.assertEqual(self.agency.routes.count(), 1)
        self.assertEqual(self.agency.routes.first().route_id, "R1")

        # Route -> Trip
        self.assertEqual(self.route.trips.count(), 1)
        self.assertEqual(self.route.trips.first().trip_id, "T100")

        # Trip -> StopTimes
        self.assertEqual(self.trip.stop_times.count(), 1)
        st = self.trip.stop_times.first()
        self.assertEqual(st.stop_sequence, 1)
        self.assertEqual(st.stop.stop_name, "Parada San Pedro")

        # Calendar -> CalendarDates
        self.assertEqual(self.calendar.calendar_dates.count(), 1)
        self.assertEqual(self.calendar.calendar_dates.first().date.strftime("%Y-%m-%d"), "2025-04-01")

        # Query reversas
        self.assertEqual(self.stop.stop_times.count(), 1)
        self.assertEqual(self.feed.routes.count(), 1)
        self.assertEqual(self.feed.trips.count(), 1)

    def test_update_delete(self):
        """Prueba actualizaciones y eliminaciones básicas."""
        # Update
        self.route.route_long_name = "Campus a Montes de Oca"
        self.route.save()
        self.assertEqual(RouteSchedule.objects.get(route_id="R1").route_long_name, "Campus a Montes de Oca")

        # Delete
        self.trip.delete()
        self.assertEqual(self.route.trips.count(), 0)
