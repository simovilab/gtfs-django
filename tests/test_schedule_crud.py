from django.test import SimpleTestCase
from gtfs.models_schedule import (
    AgencySchedule,
    CalendarSchedule,
    ShapeSchedule,
)


class ScheduleCrudTests(SimpleTestCase):
    def test_agency_schedule_crud(self):
        a = AgencySchedule(agency_id="A1", agency_name="Demo Agency")
        self.assertEqual(a.agency_id, "A1")
        self.assertEqual(a.agency_name, "Demo Agency")

        # Update
        a.agency_name = "Updated Agency"
        self.assertEqual(a.agency_name, "Updated Agency")

    def test_calendar_schedule_crud(self):
        c = CalendarSchedule(service_id="S1", monday=True, tuesday=False)
        self.assertTrue(c.monday)
        self.assertFalse(c.tuesday)

    def test_shape_schedule_crud(self):
        s = ShapeSchedule(shape_id="SH1", shape_pt_lat=9.9, shape_pt_lon=-84.0)
        self.assertEqual(s.shape_id, "SH1")
