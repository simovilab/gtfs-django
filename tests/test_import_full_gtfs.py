import os
import zipfile
import csv
from pathlib import Path
from django.core.management import call_command
from django.test import TestCase
from gtfs.models_schedule import (
    AgencySchedule,
    RouteSchedule,
    StopSchedule,
    TripSchedule,
    CalendarSchedule,
    CalendarDateSchedule,
    StopTimeSchedule,
    ShapeSchedule,
    FeedInfoSchedule,
)


class FullGTFSImportTests(TestCase):
    """Integration test: import_gtfs should handle all 9 GTFS Schedule tables."""

    def setUp(self):
        # Crear carpeta persistente dentro del proyecto
        self.tmp_path = Path("tmp_gtfs")
        self.tmp_path.mkdir(exist_ok=True)
        self.zip_path = self.tmp_path / "full_gtfs.zip"

        print(f"\n  Generating GTFS files in: {self.tmp_path.resolve()}")

        # Diccionario con los nombres de archivo y sus filas de datos
        gtfs_data = {
            "agency.txt": [
                ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_phone", "agency_email"],
                ["A1", "Demo Transit", "https://demo.example", "UTC", "000-000", "info@demo.example"],
            ],
            "routes.txt": [
                ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
                ["R1", "A1", "1", "Central Line", "3"],
            ],
            "stops.txt": [
                ["stop_id", "stop_name", "stop_lat", "stop_lon"],
                ["S1", "Central Station", "9.93", "-84.08"],
                ["S2", "North Park", "9.95", "-84.06"],
            ],
            "calendar.txt": [
                ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
                ["WKDY", "1", "1", "1", "1", "1", "0", "0", "20250101", "20251231"],
            ],
            "calendar_dates.txt": [
                ["service_id", "date", "exception_type"],
                ["WKDY", "20250501", "1"],
            ],
            "trips.txt": [
                ["trip_id", "route_id", "service_id", "trip_headsign"],
                ["T1", "R1", "WKDY", "Northbound"],
            ],
            "stop_times.txt": [
                ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
                ["T1", "08:00:00", "08:00:00", "S1", "1"],
                ["T1", "08:10:00", "08:10:00", "S2", "2"],
            ],
            "shapes.txt": [
                ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"],
                ["SH1", "9.93", "-84.08", "1", "0.0"],
                ["SH1", "9.95", "-84.06", "2", "1.5"],
            ],
            "feed_info.txt": [
                ["feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_version", "feed_start_date", "feed_end_date", "feed_contact_email", "feed_contact_url"],
                ["SIMOVILab", "https://simovilab.org", "en", "0.1.0", "20250101", "20251231", "info@simovilab.org", "https://simovilab.org/contact"],
            ],
        }

        # Crear los archivos CSV dentro de tmp_gtfs/
        for fname, rows in gtfs_data.items():
            with open(self.tmp_path / fname, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(rows)

        # Crear ZIP
        with zipfile.ZipFile(self.zip_path, "w") as zf:
            for fname in gtfs_data.keys():
                zf.write(self.tmp_path / fname, fname)

        print(f" ZIP file created at: {self.zip_path.resolve()}")

    def test_full_gtfs_import(self):
        """Ensure importgtfs command loads all 9 GTFS files successfully."""
        call_command("importgtfs", str(self.zip_path))

        # --- Verificar existencia de datos ---
        assert AgencySchedule.objects.count() == 1
        assert RouteSchedule.objects.count() == 1
        assert StopSchedule.objects.count() == 2
        assert TripSchedule.objects.count() == 1
        assert CalendarSchedule.objects.count() >= 1
        assert CalendarDateSchedule.objects.count() == 1
        assert StopTimeSchedule.objects.count() == 2
        assert ShapeSchedule.objects.count() == 2
        assert FeedInfoSchedule.objects.count() == 1

        trip = TripSchedule.objects.first()
        assert trip.route.route_id == "R1"
        assert trip.service.service_id == "WKDY"

        print("\n Full GTFS import verified successfully (data persisted in tmp_gtfs/).")
