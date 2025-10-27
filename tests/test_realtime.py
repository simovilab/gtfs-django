import unittest
import os
import json
from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format
from gtfs.utils import realtime
from google.transit import gtfs_realtime_pb2 as gtfs_rt


class GTFSRealtimeTests(unittest.TestCase):
    def setUp(self):
        # Initialize test data
        self.test_data = {
            'trip_id': '123',
            'vehicle_id': '456',
            'alert_id': '789',
            'route_id': 'R001',
            'stop_id': 'S001'
        }

    def _add_header(self, feed):
        """Adds a valid header to FeedMessage before serialization."""
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
        feed.header.timestamp = 1730000000  # sample UNIX timestamp

    # ---------------- Trip Update Test ----------------
    def test_trip_update_serialization(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        self._add_header(feed)

        entity = feed.entity.add(id="test_entity_1")
        trip_update = entity.trip_update
        trip_update.trip.trip_id = self.test_data['trip_id']
        trip_update.trip.route_id = self.test_data['route_id']

        stop_time = trip_update.stop_time_update.add()
        stop_time.stop_sequence = 1
        stop_time.arrival.delay = 60

        serialized = feed.SerializeToString()
        parsed = gtfs_realtime_pb2.FeedMessage()
        parsed.ParseFromString(serialized)

        self.assertEqual(parsed.entity[0].trip_update.trip.trip_id, self.test_data['trip_id'])
        self.assertEqual(parsed.entity[0].trip_update.stop_time_update[0].arrival.delay, 60)

    # ---------------- Vehicle Position Test ----------------
    def test_vehicle_position_serialization(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        self._add_header(feed)

        entity = feed.entity.add(id="vehicle_entity_1")
        vehicle = entity.vehicle
        vehicle.vehicle.id = self.test_data['vehicle_id']
        vehicle.position.latitude = 37.7749
        vehicle.position.longitude = -122.4194

        serialized = feed.SerializeToString()
        parsed = gtfs_realtime_pb2.FeedMessage()
        parsed.ParseFromString(serialized)

        self.assertEqual(parsed.entity[0].vehicle.vehicle.id, self.test_data['vehicle_id'])
        self.assertAlmostEqual(parsed.entity[0].vehicle.position.latitude, 37.7749, places=4)
        self.assertAlmostEqual(parsed.entity[0].vehicle.position.longitude, -122.4194, places=4)

    # ---------------- Alert Test ----------------
    def test_alert_serialization(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        self._add_header(feed)

        entity = feed.entity.add(id="alert_entity_1")
        alert = entity.alert
        alert.header_text.translation.add(text='Test Alert')
        alert.description_text.translation.add(text='Alert description')
        informed_entity = alert.informed_entity.add()
        informed_entity.route_id = self.test_data['route_id']

        serialized = feed.SerializeToString()
        parsed = gtfs_realtime_pb2.FeedMessage()
        parsed.ParseFromString(serialized)

        self.assertEqual(parsed.entity[0].alert.header_text.translation[0].text, 'Test Alert')
        self.assertEqual(parsed.entity[0].alert.informed_entity[0].route_id, self.test_data['route_id'])

    # ---------------- Feed Validation Test ----------------
    def test_feed_validation(self):
        valid_feed = gtfs_realtime_pb2.FeedMessage()
        self._add_header(valid_feed)
        valid_feed.entity.add(id="ok_entity")

        self.assertTrue(self._validate_feed(valid_feed))

        invalid_feed = gtfs_realtime_pb2.FeedMessage()
        self.assertFalse(self._validate_feed(invalid_feed))

    def _validate_feed(self, feed):
        """Checks required GTFS-Realtime header and entity fields."""
        if not feed.header.gtfs_realtime_version:
            return False
        if not feed.entity:
            return False
        return True

    # ---------------- Bytewax TripUpdates Builder Test ----------------
    def test_build_trip_updates_bytewax(self):
        """
        Validates that the Bytewax TripUpdates builder runs and produces valid GTFS-RT output.
        """
        result = realtime.build_trip_updates_bytewax()
        self.assertIn("TripUpdates feed built successfully", result)

        # Check files created
        json_path = "feed/files/trip_updates_bytewax.json"
        pb_path = "feed/files/trip_updates_bytewax.pb"
        self.assertTrue(os.path.exists(json_path))
        self.assertTrue(os.path.exists(pb_path))

        # Validate JSON structure
        with open(json_path, "r") as f:
            data = json.load(f)
        self.assertIn("header", data)
        self.assertIn("entity", data)
        self.assertEqual(data["header"]["gtfs_realtime_version"], "2.0")
        self.assertIsInstance(data["entity"], list)
        self.assertGreater(len(data["entity"]), 0)

        # Validate protobuf
        with open(pb_path, "rb") as f:
            content = f.read()
        feed = gtfs_rt.FeedMessage()
        feed.ParseFromString(content)
        self.assertEqual(feed.header.gtfs_realtime_version, "2.0")
        self.assertGreater(len(feed.entity), 0)

    # ---------------- Generate Sample Binaries ----------------
    @staticmethod
    def save_sample_binaries():
        """Creates sample GTFS-RT binary files for manual validation."""
        os.makedirs("tests/data", exist_ok=True)
        sample_types = ["trip_update", "vehicle_position", "alert"]

        for sample in sample_types:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.header.gtfs_realtime_version = "2.0"
            feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
            feed.header.timestamp = 1730000000

            entity = feed.entity.add(id=f"entity_{sample}")
            if sample == "trip_update":
                entity.trip_update.trip.trip_id = "123"
                entity.trip_update.stop_time_update.add().arrival.delay = 45
            elif sample == "vehicle_position":
                entity.vehicle.vehicle.id = "456"
                entity.vehicle.position.latitude = 37.7749
                entity.vehicle.position.longitude = -122.4194
            elif sample == "alert":
                entity.alert.header_text.translation.add(text="Sample Alert")
                entity.alert.informed_entity.add().route_id = "R001"

            with open(f"tests/data/{sample}.bin", "wb") as f:
                f.write(feed.SerializeToString())

        print("Sample GTFS-RT binaries written to tests/data/")


if __name__ == "__main__":
    # Generate sample binaries for manual validation
    GTFSRealtimeTests.save_sample_binaries()

    print("\n==========================")
    print("EJECUTANDO UNIT TESTS")
    print("==========================\n")
    unittest.main()

