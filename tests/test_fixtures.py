import json
import tempfile
from pathlib import Path

from django.core.management import call_command
from django.test import TestCase

from gtfs.models_schedule import (
    AgencySchedule, RouteSchedule, CalendarSchedule,
    StopSchedule, TripSchedule, StopTimeSchedule
)


class ManagementFixtureTests(TestCase):
    def test_create_and_load_fixture(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "schedule_min.json"
            call_command("create_schedule_fixtures", "--seed", "123", "--output", str(out))
            assert out.exists()

            # Cargar el fixture a la DB de test
            call_command("loaddata", str(out))

            self.assertEqual(AgencySchedule.objects.count(), 1)
            self.assertEqual(RouteSchedule.objects.count(), 1)
            self.assertEqual(CalendarSchedule.objects.count(), 1)
            self.assertEqual(StopSchedule.objects.count(), 2)
            self.assertEqual(TripSchedule.objects.count(), 1)
            self.assertEqual(StopTimeSchedule.objects.count(), 2)

            # Chequeo simple de relaciones
            trip = TripSchedule.objects.get(pk="T100")
            self.assertEqual(trip.route_id, "R10")
            self.assertEqual(trip.service_id, "WKDY")
            self.assertEqual(trip.stop_times.count(), 2)
