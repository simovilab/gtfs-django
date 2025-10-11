from .stop_times import estimate_stop_times
from .fake_stop_times import fake_stop_times
from gtfs.models import Journey, Progression, Position, Occupancy
from celery import shared_task
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import logging
import pytz
import zipfile
import io
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2 as gtfs_rt
from google.protobuf import json_format
from gtfs.models import GTFSProvider, FeedMessage, VehiclePosition, TripUpdate, StopTimeUpdate, Alert
from django.utils import timezone
from django.utils.timezone import make_aware
import unittest
import json
from google.transit import gtfs_realtime_pb2
import pytz
from django.core.cache import cache
import requests
from google.protobuf.message import DecodeError


class GTFSRealtimeTests(unittest.TestCase):
    def setUp(self):
        # Initialize test data with sample identifiers
        self.test_data = {
            'trip_id': '123',
            'vehicle_id': '456',
            'alert_id': '789',
            'route_id': 'R001',
            'stop_id': 'S001'
        }
        
    def test_trip_update_serialization(self):
        # Create test message with trip update data
        feed = gtfs_realtime_pb2.FeedMessage()
        entity = feed.entity.add()
        trip_update = entity.trip_update
        
        # Configure basic trip information
        trip_update.trip.trip_id = self.test_data['trip_id']
        trip_update.trip.route_id = self.test_data['route_id']
        
        # Configure stop time update with delay
        stop_time = trip_update.stop_time_update.add()
        stop_time.stop_sequence = 1
        stop_time.arrival.delay = 60  # 1 minute delay
        
        # Serialize message to binary format
        serialized_data = feed.SerializeToString()
        
        # Deserialize and validate the message
        parsed_feed = gtfs_realtime_pb2.FeedMessage()
        parsed_feed.ParseFromString(serialized_data)
        
        # Verify trip update fields
        self.assertEqual(parsed_feed.entity[0].trip_update.trip.trip_id,
                        self.test_data['trip_id'])
        self.assertEqual(parsed_feed.entity[0].trip_update.stop_time_update[0].stop_sequence,
                        1)
        self.assertEqual(parsed_feed.entity[0].trip_update.stop_time_update[0].arrival.delay,
                        60)

    def test_vehicle_position_serialization(self):
        # Create test message with vehicle position data
        feed = gtfs_realtime_pb2.FeedMessage()
        entity = feed.entity.add()
        vehicle_position = entity.vehicle
        
        # Configure vehicle identification and location
        vehicle_position.vehicle.id = self.test_data['vehicle_id']
        vehicle_position.position.latitude = 37.7749
        vehicle_position.position.longitude = -122.4194
        
        # Serialize message to binary format
        serialized_data = feed.SerializeToString()
        
        # Deserialize and validate the message
        parsed_feed = gtfs_realtime_pb2.FeedMessage()
        parsed_feed.ParseFromString(serialized_data)
        
        # Verify vehicle position fields
        self.assertEqual(parsed_feed.entity[0].vehicle.vehicle.id,
                        self.test_data['vehicle_id'])
        self.assertAlmostEqual(parsed_feed.entity[0].vehicle.position.latitude,
                             37.7749, places=4)
        self.assertAlmostEqual(parsed_feed.entity[0].vehicle.position.longitude,
                             -122.4194, places=4)

    def test_alert_serialization(self):
        # Create test message with alert data
        feed = gtfs_realtime_pb2.FeedMessage()
        entity = feed.entity.add()
        alert = entity.alert
        
        # Configure alert translations
        alert.header_text.translations.add(text='Test Alert')
        alert.description_text.translations.add(text='Alert description')
        
        # Add affected entity information
        informed_entity = alert.informed_entity.add()
        informed_entity.route_id = self.test_data['route_id']
        
        # Serialize message to binary format
        serialized_data = feed.SerializeToString()
        
        # Deserialize and validate the message
        parsed_feed = gtfs_realtime_pb2.FeedMessage()
        parsed_feed.ParseFromString(serialized_data)
        
        # Verify alert fields
        self.assertEqual(parsed_feed.entity[0].alert.header_text.translations[0].text,
                        'Test Alert')
        self.assertEqual(parsed_feed.entity[0].alert.informed_entity[0].route_id,
                        self.test_data['route_id'])

    def test_feed_validation(self):
        # Create valid message for testing
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        entity = feed.entity.add()
        
        # Validate valid message
        self.assertTrue(self._validate_feed(feed))
        
        # Create invalid message (missing version)
        invalid_feed = gtfs_realtime_pb2.FeedMessage()
        
        # Validate invalid message
        self.assertFalse(self._validate_feed(invalid_feed))

    def _validate_feed(self, feed):
        # Check required GTFS Realtime fields
        if not feed.header.gtfs_realtime_version:
            return False
        if not feed.entity:
            return False
        return True

if __name__ == '__main__':
    unittest.main()




def _format_time(dt):
    """
    Formats a datetime.time or datetime.datetime object as "HH:MM:SS" string.
    """
    if hasattr(dt, "time"):
        dt = dt.time()
    return dt.strftime("%H:%M:%S")


def get_vehicle_positions():
    providers = GTFSProvider.objects.filter(is_active=True)
    saved_data = False
    for provider in providers:
        vehicle_positions = gtfs_rt.FeedMessage()
        try:
            vehicle_positions_response = requests.get(provider.vehicle_positions_url)
            print(f"Fetching vehicle positions from {provider.vehicle_positions_url}")
        except requests.exceptions.RequestException as e:
                # timeouts, DNS, HTTP errors
                print(f"Network error fetching from {provider.vehicle_positions_url}: {e}")
                continue

        except DecodeError as e:
            # decoding Protobuf error
            print(f"Failed to parse Protobuf data from {provider.vehicle_positions_url}: {e}")
            continue

        except Exception as e:
            # unexpected errors
            print(f"Unexpected error processing {provider.code}: {e}")
            continue
        vehicle_positions.ParseFromString(vehicle_positions_response.content)

        # Save feed message to database
        feed_message = FeedMessage(
            feed_message_id=f"{provider.code}-vehicle-{vehicle_positions.header.timestamp}",
            provider=provider,
            entity_type="vehicle",
            timestamp=datetime.fromtimestamp(
                int(vehicle_positions.header.timestamp),
                tz=pytz.timezone(provider.timezone),
            ),
            incrementality=vehicle_positions.header.incrementality,
            gtfs_realtime_version=vehicle_positions.header.gtfs_realtime_version,
        )

        feed_message.save()

        vehicle_positions_json = json_format.MessageToJson(
            vehicle_positions, preserving_proto_field_name=True
        )
        vehicle_positions_json = json.loads(vehicle_positions_json)
        if "entity" not in vehicle_positions_json:
            print("No vehicle positions found")
            continue
        vehicle_positions_df = pd.json_normalize(
            vehicle_positions_json["entity"], sep="_"
        )
        vehicle_positions_df.rename(columns={"id": "entity_id"}, inplace=True)
        vehicle_positions_df["feed_message"] = feed_message
        # Drop unnecessary columns
        try:
            vehicle_positions_df.drop(
                columns=["vehicle_multi_carriage_details"],
                inplace=True,
            )
        except Exception:
            pass
        # Fix entity timestamp
        vehicle_positions_df["vehicle_timestamp"] = pd.to_datetime(
            vehicle_positions_df["vehicle_timestamp"].astype(int), unit="s", utc=True
        )
        # Fix trip start date
        vehicle_positions_df["vehicle_trip_start_date"] = pd.to_datetime(
            vehicle_positions_df["vehicle_trip_start_date"], format="%Y%m%d"
        )
        vehicle_positions_df["vehicle_trip_start_date"].fillna(
            datetime.now().date(), inplace=True
        )
        # Fix trip start time
        vehicle_positions_df["vehicle_trip_start_time"] = pd.to_timedelta(
            vehicle_positions_df["vehicle_trip_start_time"]
        )
        vehicle_positions_df["vehicle_trip_start_time"].fillna(
            timedelta(hours=0, minutes=0, seconds=0), inplace=True
        )
        # Fix trip direction
        vehicle_positions_df["vehicle_trip_direction_id"].fillna(-1, inplace=True)
        # Fix current stop sequence
        vehicle_positions_df["vehicle_current_stop_sequence"].fillna(-1, inplace=True)
        # Create vehicle position point
        vehicle_positions_df["vehicle_position_point"] = vehicle_positions_df.apply(
            lambda x: f"POINT ({x.vehicle_position_longitude} {x.vehicle_position_latitude})",
            axis=1,
        )
        # Save to database
        objects = [
            VehiclePosition(**row)
            for row in vehicle_positions_df.to_dict(orient="records")
        ]
        VehiclePosition.objects.bulk_create(objects)
        saved_data = saved_data or True

    # Send status update to WebSocket
    message = {}
    message["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message["number_providers"] = len(providers)
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "status",
        {
            "type": "status_message",
            "message": message,
        },
    )

    if saved_data:
        return "VehiclePositions saved to database"
    else:
        return "No VehiclePositions found"

def build_vehicle_positions():
    """
    Build the VehiclePosition feed message."""

    # Feed message dictionary
    feed_message = {}

    # Feed message header
    feed_message["header"] = {}
    feed_message["header"]["gtfs_realtime_version"] = "2.0"
    feed_message["header"]["incrementality"] = "FULL_DATASET"
    feed_message["header"]["timestamp"] = int(datetime.now().timestamp())

    # Feed message entity
    feed_message["entity"] = []

    # TODO: Instrument this process with Prometheus
    journeys = Journey.objects.filter(journey_status="IN_PROGRESS")

    for journey in journeys:
        vehicle = journey.vehicle

        # Get position object
        positions = Position.objects.filter(vehicle=vehicle, is_new=True)
        if positions.exists():
            position = positions.latest("timestamp")
            for position in positions:
                position.is_new = False
                position.save()
        else:
            position = None
        # Get progression object
        progressions = Progression.objects.filter(vehicle=vehicle, is_new=True)
        if progressions.exists():
            progression = progressions.latest("timestamp")
            for progression in progressions:
                progression.is_new = False
                progression.save()
        else:
            progression = None
        # Get occupancy object
        occupancies = Occupancy.objects.filter(vehicle=vehicle, is_new=True)
        if occupancies.exists():
            occupancy = occupancies.latest("timestamp")
            for occupancy in occupancies:
                occupancy.is_new = False
                occupancy.save()
        else:
            occupancy = None

        if not position and not progression and not occupancy:
            # TODO: Log this event, create strategy to clean up stale journeys
            continue

        # Build entity
        entity = {}
        entity["id"] = f"{vehicle.id}"
        entity["vehicle"] = {}
        # Timestamp
        entity["vehicle"]["timestamp"] = int(position.timestamp.timestamp())
        # Trip
        entity["vehicle"]["trip"] = {}
        entity["vehicle"]["trip"]["trip_id"] = journey.trip_id
        entity["vehicle"]["trip"]["route_id"] = journey.route_id
        entity["vehicle"]["trip"]["direction_id"] = journey.direction_id
        entity["vehicle"]["trip"]["start_time"] = _format_time(journey.start_time)
        entity["vehicle"]["trip"]["start_date"] = journey.start_date.strftime("%Y%m%d")
        entity["vehicle"]["trip"]["schedule_relationship"] = (
            journey.schedule_relationship
        )
        # Vehicle
        entity["vehicle"]["vehicle"] = {}
        entity["vehicle"]["vehicle"]["id"] = vehicle.id
        entity["vehicle"]["vehicle"]["label"] = vehicle.label
        entity["vehicle"]["vehicle"]["license_plate"] = vehicle.license_plate
        # Position
        if position:
            entity["vehicle"]["position"] = {}
            entity["vehicle"]["position"]["latitude"] = position.point.y
            entity["vehicle"]["position"]["longitude"] = position.point.x
            entity["vehicle"]["position"]["bearing"] = position.bearing
            entity["vehicle"]["position"]["odometer"] = position.odometer
            entity["vehicle"]["position"]["speed"] = position.speed
        # Progression
        if progression:
            entity["vehicle"]["current_stop_sequence"] = (
                progression.current_stop_sequence
            )
            entity["vehicle"]["stop_id"] = progression.stop_id
            entity["vehicle"]["current_status"] = progression.current_status
            entity["vehicle"]["congestion_level"] = progression.congestion_level
        # Occupancy
        if occupancy:
            entity["vehicle"]["occupancy_status"] = occupancy.occupancy_status
            entity["vehicle"]["occupancy_percentage"] = occupancy.occupancy_percentage
        # Append entity to feed message
        feed_message["entity"].append(entity)

    # Create and save JSON
    feed_message_json = json.dumps(feed_message)
    with open("feed/files/vehicle_positions.json", "w") as f:
        f.write(feed_message_json)

    # Create and save Protobuf
    feed_message_json = json.loads(feed_message_json)
    feed_message_pb = json_format.ParseDict(feed_message_json, gtfs_rt.FeedMessage())
    with open("feed/files/vehicle_positions.pb", "wb") as f:
        f.write(feed_message_pb.SerializeToString())

    return "FeedMessage VehiclePosition built successfully"


def build_trip_updates():
    # Feed message dictionary
    feed_message = {}

    # Feed message header
    feed_message["header"] = {}
    feed_message["header"]["gtfs_realtime_version"] = "2.0"
    feed_message["header"]["incrementality"] = "FULL_DATASET"
    feed_message["header"]["timestamp"] = int(datetime.now().timestamp())

    # Feed message entity
    feed_message["entity"] = []

    journeys = Journey.objects.filter(journey_status="IN_PROGRESS")

    for journey in journeys:
        vehicle = journey.equipment.vehicle
        position = Position.objects.filter(journey=journey).latest("timestamp")
        progression = Progression.objects.filter(journey=journey).latest("timestamp")
        # Entity
        entity = {}
        entity["id"] = f"bus-{vehicle.id}"
        entity["trip_update"] = {}
        # Timestamp
        entity["trip_update"]["timestamp"] = int(position.timestamp.timestamp())
        # Trip
        entity["trip_update"]["trip"] = {}
        entity["trip_update"]["trip"]["trip_id"] = journey.trip_id
        entity["trip_update"]["trip"]["route_id"] = journey.route_id
        entity["trip_update"]["trip"]["direction_id"] = journey.direction_id
        entity["trip_update"]["trip"]["start_time"] = _format_time(journey.start_time)
        entity["trip_update"]["trip"]["start_date"] = journey.start_date.strftime(
            "%Y%m%d"
        )
        entity["trip_update"]["trip"]["schedule_relationship"] = (
            journey.schedule_relationship
        )
        # Vehicle
        entity["trip_update"]["vehicle"] = {}
        entity["trip_update"]["vehicle"]["id"] = vehicle.id
        entity["trip_update"]["vehicle"]["label"] = vehicle.label
        entity["trip_update"]["vehicle"]["license_plate"] = vehicle.license_plate
        # Stop time update
        entity["trip_update"]["stop_time_update"] = fake_stop_times(
            journey=journey, progression=progression
        )
        # Append entity to feed message
        feed_message["entity"].append(entity)

    # Create and save JSON
    feed_message_json = json.dumps(feed_message)
    with open("feed/files/trip_updates.json", "w") as f:
        f.write(feed_message_json)

    # Create and save Protobuf
    feed_message_json = json.loads(feed_message_json)
    feed_message_pb = json_format.ParseDict(feed_message_json, gtfs_rt.FeedMessage())
    with open("feed/files/trip_updates.pb", "wb") as f:
        f.write(feed_message_pb.SerializeToString())

    # Send status update to WebSocket
    message = {}
    message["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message["journeys"] = len(journeys)
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "status",
        {
            "type": "status_message",
            "message": message,
        },
    )

    return "Feed TripUpdate built."

def build_alerts():
    """
    Construye el feed de alertas del servicio.
    """
    print("Building feed: ServiceAlerts")
    
    # Feed message structure
    feed_dict = {
        "header": {
            "gtfs_realtime_version": "2.0",
            "incrementality": "FULL_DATASET",
            "timestamp": int(timezone.now().timestamp()),
        },
        "entity": [],
    }
    
    alerts = Alert.objects.all()
    if not alerts.exists():
        print("No alerts found in database.")
        return "No alerts to build"
    
    for alert in alerts:
        # Compose one entity per alert
        entity = {
            "id": str(alert.alert_id),
            "alert": {
                # Required fields
                "header_text": {
                    "translation": [{"text": alert.alert_header}]
                },
                "description_text": {
                    "translation": [{"text": alert.alert_description}]
                },
                # Required: at least one informed_entity
                "informed_entity": alert.informed_entity,
            },
        }
        
        # Optional fields based on GTFS spec
        active_period = {}
        if alert.service_start_time:
            start_dt = datetime.combine(alert.service_date, alert.service_start_time)
            active_period["start"] = int(make_aware(start_dt).timestamp())
        if alert.service_end_time:
            end_dt = datetime.combine(alert.service_date, alert.service_end_time)
            active_period["end"] = int(make_aware(end_dt).timestamp())
        if active_period:
            entity["alert"]["active_period"] = [active_period]
        
        if alert.cause:
            entity["alert"]["cause"] = int(alert.cause)
        if alert.effect:
            entity["alert"]["effect"] = int(alert.effect)
        if alert.alert_url:
            entity["alert"]["url"] = {"translation": [{"text": alert.alert_url}]}
        if alert.severity:
            entity["alert"]["severity_level"] = int(alert.severity)
        
        feed_dict["entity"].append(entity)
    
    # --- Serialize to JSON file ---
    feed_json = json.dumps(feed_dict, indent=2)
    with open("feed/files/alerts.json", "w", encoding="utf-8") as f:
        f.write(feed_json)
    print("Saved feed/files/alerts.json")
    
    # --- Serialize to Protobuf binary ---
    feed_pb = json_format.ParseDict(feed_dict, gtfs_rt.FeedMessage())
    with open("feed/files/alerts.pb", "wb") as f:
        f.write(feed_pb.SerializeToString())
    print("Saved feed/files/alerts.pb")
    
    # Optionally register in DB as FeedMessage
    provider = GTFSProvider.objects.filter(is_active=True).first()
    if provider:
        FeedMessage.objects.create(
            feed_message_id=f"{provider.code}-alerts-{feed_dict['header']['timestamp']}",
            provider=provider,
            entity_type="alert",
            timestamp=timezone.now(),
            incrementality="FULL_DATASET",
            gtfs_realtime_version="2.0",
    )
    
    return "ServiceAlerts saved to database"


def get_vehicle_positions():
    providers = GTFSProvider.objects.filter(is_active=True)
    saved_data = False
    for provider in providers:
        vehicle_positions = gtfs_rt.FeedMessage()
        try:
            vehicle_positions_response = requests.get(provider.vehicle_positions_url)
            print(f"Fetching vehicle positions from {provider.vehicle_positions_url}")
        except Exception as e:
            print(
                f"Error fetching vehicle positions from {provider.vehicle_positions_url}: {e}"
            )
            continue
        vehicle_positions.ParseFromString(vehicle_positions_response.content)

        # Save feed message to database
        feed_message = FeedMessage(
            feed_message_id=f"{provider.code}-vehicle-{vehicle_positions.header.timestamp}",
            provider=provider,
            entity_type="vehicle",
            timestamp=datetime.fromtimestamp(
                int(vehicle_positions.header.timestamp),
                tz=pytz.timezone(provider.timezone),
            ),
            incrementality=vehicle_positions.header.incrementality,
            gtfs_realtime_version=vehicle_positions.header.gtfs_realtime_version,
        )

        feed_message.save()

        vehicle_positions_json = json_format.MessageToJson(
            vehicle_positions, preserving_proto_field_name=True
        )
        vehicle_positions_json = json.loads(vehicle_positions_json)
        if "entity" not in vehicle_positions_json:
            print("No vehicle positions found")
            continue
        vehicle_positions_df = pd.json_normalize(
            vehicle_positions_json["entity"], sep="_"
        )
        vehicle_positions_df.rename(columns={"id": "entity_id"}, inplace=True)
        vehicle_positions_df["feed_message"] = feed_message
        # Drop unnecessary columns
        try:
            vehicle_positions_df.drop(
                columns=["vehicle_multi_carriage_details"],
                inplace=True,
            )
        except Exception:
            pass
        # Fix entity timestamp
        vehicle_positions_df["vehicle_timestamp"] = pd.to_datetime(
            vehicle_positions_df["vehicle_timestamp"].astype(int), unit="s", utc=True
        )
        # Fix trip start date
        vehicle_positions_df["vehicle_trip_start_date"] = pd.to_datetime(
            vehicle_positions_df["vehicle_trip_start_date"], format="%Y%m%d"
        )
        vehicle_positions_df["vehicle_trip_start_date"].fillna(
            datetime.now().date(), inplace=True
        )
        # Fix trip start time
        vehicle_positions_df["vehicle_trip_start_time"] = pd.to_timedelta(
            vehicle_positions_df["vehicle_trip_start_time"]
        )
        vehicle_positions_df["vehicle_trip_start_time"].fillna(
            timedelta(hours=0, minutes=0, seconds=0), inplace=True
        )
        # Fix trip direction
        vehicle_positions_df["vehicle_trip_direction_id"].fillna(-1, inplace=True)
        # Fix current stop sequence
        vehicle_positions_df["vehicle_current_stop_sequence"].fillna(-1, inplace=True)
        # Create vehicle position point
        vehicle_positions_df["vehicle_position_point"] = vehicle_positions_df.apply(
            lambda x: f"POINT ({x.vehicle_position_longitude} {x.vehicle_position_latitude})",
            axis=1,
        )
        # Save to database
        objects = [
            VehiclePosition(**row)
            for row in vehicle_positions_df.to_dict(orient="records")
        ]
        VehiclePosition.objects.bulk_create(objects)
        saved_data = saved_data or True

    # Send status update to WebSocket
    message = {}
    message["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message["number_providers"] = len(providers)
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "status",
        {
            "type": "status_message",
            "message": message,
        },
    )

    if saved_data:
        return "VehiclePositions saved to database"
    else:
        return "No VehiclePositions found"



def get_trip_updates():
    providers = GTFSProvider.objects.filter(is_active=True)
    for provider in providers:
        try:
            trip_updates_response = requests.get(provider.trip_updates_url, timeout=10)
            trip_updates_response.raise_for_status()
        except requests.RequestException as e:
            print(
                f"Error fetching trip updates from {provider.trip_updates_url}: {str(e)}"
            )
            continue

        # Parse FeedMessage object from Protobuf
        trip_updates = gtfs_rt.FeedMessage()
        trip_updates.ParseFromString(trip_updates_response.content)

        # Build FeedMessage object
        feed_message = FeedMessage(
            feed_message_id=f"{provider.code}-trip_updates-{trip_updates.header.timestamp}",
            provider=provider,
            entity_type="trip_update",
            timestamp=datetime.fromtimestamp(
                int(trip_updates.header.timestamp),
                tz=pytz.timezone(provider.timezone),
            ),
            incrementality=trip_updates.header.incrementality,
            gtfs_realtime_version=trip_updates.header.gtfs_realtime_version,
        )
        # Save FeedMessage object
        feed_message.save()

        # Build TripUpdate DataFrame
        trip_updates_json = json_format.MessageToJson(
            trip_updates, preserving_proto_field_name=True
        )
        trip_updates_json = json.loads(trip_updates_json)
        trip_updates_df = pd.json_normalize(trip_updates_json["entity"], sep="_")
        trip_updates_df.rename(columns={"id": "entity_id"}, inplace=True)
        trip_updates_df["feed_message"] = feed_message

        # Fix entity timestamp
        trip_updates_df["trip_update_timestamp"].fillna(
            datetime.now().timestamp(), inplace=True
        )
        trip_updates_df["trip_update_timestamp"] = pd.to_datetime(
            trip_updates_df["trip_update_timestamp"].astype(int), unit="s", utc=True
        )
        # Fix trip start date
        trip_updates_df["trip_update_trip_start_date"] = pd.to_datetime(
            trip_updates_df["trip_update_trip_start_date"], format="%Y%m%d"
        )
        trip_updates_df["trip_update_trip_start_date"].fillna(
            datetime.now().date(), inplace=True
        )
        # Fix trip start time
        trip_updates_df["trip_update_trip_start_time"] = pd.to_timedelta(
            trip_updates_df["trip_update_trip_start_time"]
        )
        trip_updates_df["trip_update_trip_start_time"].fillna(
            timedelta(hours=0, minutes=0, seconds=0), inplace=True
        )
        # Fix trip direction
        trip_updates_df["trip_update_trip_direction_id"].fillna(-1, inplace=True)

        for i, trip_update in trip_updates_df.iterrows():
            this_trip_update = TripUpdate(
                entity_id=trip_update["entity_id"],
                feed_message=trip_update["feed_message"],
                trip_trip_id=trip_update["trip_update_trip_trip_id"],
                trip_route_id=trip_update["trip_update_trip_route_id"],
                trip_direction_id=trip_update["trip_update_trip_direction_id"],
                trip_start_time=trip_update["trip_update_trip_start_time"],
                trip_start_date=trip_update["trip_update_trip_start_date"],
                trip_schedule_relationship=trip_update[
                    "trip_update_trip_schedule_relationship"
                ],
                vehicle_id=trip_update["trip_update_vehicle_id"],
                vehicle_label=trip_update["trip_update_vehicle_label"],
                # trip_update_vehicle_license_plate=trip_update["trip_update_vehicle_license_plate"],
                # trip_update_vehicle_wheelchair_accessible=trip_update["trip_update_vehicle_wheelchair_accessible"],
                timestamp=trip_update["trip_update_timestamp"],
                # trip_update_delay=trip_update["trip_update_delay"],
            )
            # Save this TripUpdate object
            this_trip_update.save()

            # Build StopTimeUpdate DataFrame
            stop_time_updates_json = str(trip_update["trip_update_stop_time_update"])
            stop_time_updates_json = stop_time_updates_json.replace("'", '"')
            stop_time_updates_json = json.loads(stop_time_updates_json)
            stop_time_updates_df = pd.json_normalize(stop_time_updates_json, sep="_")
            stop_time_updates_df["feed_message"] = feed_message
            stop_time_updates_df["trip_update"] = this_trip_update

            # Fix arrival time timestamp
            if "arrival_time" in stop_time_updates_df.columns:
                stop_time_updates_df["arrival_time"].fillna(
                    datetime.now().timestamp(), inplace=True
                )
                stop_time_updates_df["arrival_time"] = pd.to_datetime(
                    stop_time_updates_df["arrival_time"].astype(int), unit="s", utc=True
                )
            # Fix departure time timestamp
            if "departure_time" in stop_time_updates_df.columns:
                stop_time_updates_df["departure_time"].fillna(
                    datetime.now().timestamp(), inplace=True
                )
                stop_time_updates_df["departure_time"] = pd.to_datetime(
                    stop_time_updates_df["departure_time"].astype(int),
                    unit="s",
                    utc=True,
                )
            # Fix arrival uncertainty
            if "arrival_uncertainty" in stop_time_updates_df.columns:
                stop_time_updates_df["arrival_uncertainty"].fillna(0, inplace=True)
            # Fix departure uncertainty
            if "departure_uncertainty" in stop_time_updates_df.columns:
                stop_time_updates_df["departure_uncertainty"].fillna(0, inplace=True)
            # Fix arrival delay
            if "arrival_delay" in stop_time_updates_df.columns:
                stop_time_updates_df["arrival_delay"].fillna(0, inplace=True)
            # Fix departure delay
            if "departure_delay" in stop_time_updates_df.columns:
                stop_time_updates_df["departure_delay"].fillna(0, inplace=True)

            # Save to database
            objects = [
                StopTimeUpdate(**row)
                for row in stop_time_updates_df.to_dict(orient="records")
            ]
            StopTimeUpdate.objects.bulk_create(objects)

    return "TripUpdates saved to database"


def get_service_alerts():
    """
    Obtiene y procesa las alertas del servicio GTFS Realtime.
    """
    providers = GTFSProvider.objects.filter(is_active=True)
    if not providers.exists():
        print("No active providers found.")
        return "No active providers"
    
    saved_any = False
    for provider in providers:
        try:
            response = requests.get(provider.service_alerts_url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching alerts from {provider.service_alerts_url}: {str(e)}")
            continue
        
        # Decode Protobuf binary
        feed_message = gtfs_rt.FeedMessage()
        feed_message.ParseFromString(response.content)
        
        # Validation (as per GTFS spec)
        if not feed_message.header.gtfs_realtime_version:
            print("Invalid feed: missing header.gtfs_realtime_version")
            continue
        if not feed_message.entity:
            print("Empty feed (no alerts).")
            continue
        
        print(f"Decoding {len(feed_message.entity)} alerts from provider {provider.code}...")
        
        for entity in feed_message.entity:
            if not entity.HasField("alert"):
                continue
            
            alert_obj = entity.alert
            # Extract mandatory fields
            header_text = (
                alert_obj.header_text.translation[0].text
                if alert_obj.header_text.translation
                else "Sin título"
            )
            description_text = (
                alert_obj.description_text.translation[0].text
                if alert_obj.description_text.translation
                else ""
            )
            
            informed_entity = []
            for info in alert_obj.informed_entity:
                # EntitySelector may contain route_id, stop_id, trip, etc.
                entity_info = {}
                if info.route_id:
                    entity_info["route_id"] = info.route_id
                if info.stop_id:
                    entity_info["stop_id"] = info.stop_id
                if info.trip.trip_id:
                    entity_info["trip_id"] = info.trip.trip_id
                informed_entity.append(entity_info)
            
            # Extract time range
            service_date = datetime.now().date()
            start_time = None
            end_time = None
            if alert_obj.active_period:
                ap = alert_obj.active_period[0]
                if ap.HasField("start"):
                    start_time = datetime.fromtimestamp(
                        ap.start, 
                        tz=pytz.timezone(provider.timezone)
                    ).time()
                if ap.HasField("end"):
                    end_time = datetime.fromtimestamp(
                        ap.end, 
                        tz=pytz.timezone(provider.timezone)
                    ).time()
            
            # Save to database
            Alert.objects.create(
                feed=provider.feed_set.first(),
                alert_id=entity.id,
                route_id=informed_entity[0].get("route_id", ""),
                trip_id=informed_entity[0].get("trip_id", ""),
                service_date=service_date,
                service_start_time=start_time or datetime.now().time(),
                service_end_time=end_time or datetime.now().time(),
                alert_header=header_text,
                alert_description=description_text,
                cause=int(alert_obj.cause) if alert_obj.HasField("cause") else 1,
                effect=int(alert_obj.effect) if alert_obj.HasField("effect") else 1,
                severity=int(alert_obj.severity_level) if alert_obj.HasField("severity_level") else 1,
                published=datetime.now(),
                updated=datetime.now(),
                informed_entity=informed_entity,
            )
            saved_any = True
        
        if saved_any:
            print("ServiceAlerts saved to database.")
            return "ServiceAlerts saved to database"
        else:
            print("No ServiceAlerts found or decoded.")
            return "No ServiceAlerts found"
