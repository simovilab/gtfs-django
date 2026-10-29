import os
import io
import csv
import zipfile
from datetime import datetime
from decimal import Decimal

import requests
from django.core.management.base import BaseCommand
from django.db import transaction
from django.contrib.gis.geos import Point, LineString

from sch_pipeline.models import (
    Feed, Agency, Stop, Route, Calendar, CalendarDate,
    Shape, GeoShape, Trip, StopTime, GTFSProvider
)


class Command(BaseCommand):
    help = 'Download and import GTFS static schedule data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--url',
            type=str,
            help='GTFS zip URL (overrides .env)',
        )
        parser.add_argument(
            '--provider-id',
            type=int,
            default=1,
            help='GTFSProvider ID to associate with this feed',
        )

    def handle(self, *args, **options):
        url = options['url'] or os.getenv('GTFS_SCHEDULE_ZIP_URL')
        provider_id = options['provider_id']
        
        if not url:
            self.stdout.write(self.style.ERROR('No GTFS URL provided'))
            return

        self.stdout.write(f'Downloading {url}...')
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Download failed: {e}'))
            return

        # Create feed ID
        feed_id = f"{os.getenv('FEED_NAME', 'gtfs')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.stdout.write(f'Importing as feed: {feed_id}')

        try:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                importer = GTFSImporter(feed_id, provider_id, zf, self.stdout)
                importer.import_all()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Import failed: {e}'))
            raise

        self.stdout.write(self.style.SUCCESS(f'Successfully imported {feed_id}'))


class GTFSImporter:
    def __init__(self, feed_id, provider_id, zipfile, stdout):
        self.feed_id = feed_id
        self.provider_id = provider_id
        self.zipfile = zipfile
        self.stdout = stdout
        self.feed = None

    def import_all(self):
        """Import in dependency order"""
        with transaction.atomic():
            self.import_feed()
            self.import_agencies()
            self.import_stops()
            self.import_routes()
            self.import_calendar()
            self.import_calendar_dates()
            self.import_shapes()
            self.import_trips()
            self.import_stop_times()

    def import_feed(self):
        self.stdout.write('Creating Feed...')
        provider = GTFSProvider.objects.get(provider_id=self.provider_id)
        self.feed = Feed.objects.create(
            feed_id=self.feed_id,
            gtfs_provider=provider,
            is_current=True
        )

    def import_agencies(self):
        self.stdout.write('Importing agencies...')
        with self.zipfile.open('agency.txt') as f:
            reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8-sig'))
            agencies = []
            for row in reader:
                agencies.append(Agency(
                    feed=self.feed,
                    agency_id=row.get('agency_id', ''),
                    agency_name=row['agency_name'],
                    agency_url=row['agency_url'],
                    agency_timezone=row['agency_timezone'],
                    agency_lang=row.get('agency_lang', ''),
                    agency_phone=row.get('agency_phone', ''),
                    agency_fare_url=row.get('agency_fare_url', ''),
                    agency_email=row.get('agency_email', ''),
                ))
            Agency.objects.bulk_create(agencies, batch_size=1000, ignore_conflicts=True)
        self.stdout.write(f'  Imported {len(agencies)} agencies')

    def import_stops(self):
        self.stdout.write('Importing stops...')
        with self.zipfile.open('stops.txt') as f:
            reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8-sig'))
            stops = []
            for row in reader:
                lat = Decimal(row['stop_lat']) if row.get('stop_lat') else None
                lon = Decimal(row['stop_lon']) if row.get('stop_lon') else None
                
                stops.append(Stop(
                    feed=self.feed,
                    stop_id=row['stop_id'],
                    stop_code=row.get('stop_code', ''),
                    stop_name=row['stop_name'],
                    stop_lat=lat,
                    stop_lon=lon,
                    stop_point=Point(float(lon), float(lat)) if lat and lon else None,
                    location_type=int(row.get('location_type', 0)),
                    parent_station=row.get('parent_station', ''),
                    wheelchair_boarding=int(row.get('wheelchair_boarding', 0)),
                ))
            Stop.objects.bulk_create(stops, batch_size=1000, ignore_conflicts=True)
        self.stdout.write(f'  Imported {len(stops)} stops')

    def import_routes(self):
        self.stdout.write('Importing routes...')
        with self.zipfile.open('routes.txt') as f:
            reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8-sig'))
            routes = []
            for row in reader:
                routes.append(Route(
                    feed=self.feed,
                    route_id=row['route_id'],
                    agency_id=row.get('agency_id', ''),
                    route_short_name=row.get('route_short_name', ''),
                    route_long_name=row.get('route_long_name', ''),
                    route_type=int(row.get('route_type', 3)),
                    route_color=row.get('route_color', ''),
                ))
            Route.objects.bulk_create(routes, batch_size=1000, ignore_conflicts=True)
        self.stdout.write(f'  Imported {len(routes)} routes')

    def import_calendar(self):
        self.stdout.write('Importing calendar...')
        try:
            with self.zipfile.open('calendar.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8-sig'))
                calendars = []
                for row in reader:
                    calendars.append(Calendar(
                        feed=self.feed,
                        service_id=row['service_id'],
                        monday=row['monday'] == '1',
                        tuesday=row['tuesday'] == '1',
                        wednesday=row['wednesday'] == '1',
                        thursday=row['thursday'] == '1',
                        friday=row['friday'] == '1',
                        saturday=row['saturday'] == '1',
                        sunday=row['sunday'] == '1',
                        start_date=datetime.strptime(row['start_date'], '%Y%m%d').date(),
                        end_date=datetime.strptime(row['end_date'], '%Y%m%d').date(),
                    ))
                Calendar.objects.bulk_create(calendars, batch_size=1000, ignore_conflicts=True)
            self.stdout.write(f'  Imported {len(calendars)} calendar entries')
        except KeyError:
            self.stdout.write('  No calendar.txt found (optional)')

    def import_calendar_dates(self):
        self.stdout.write('Importing calendar dates...')
        try:
            with self.zipfile.open('calendar_dates.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8-sig'))
                dates = []
                for row in reader:
                    dates.append(CalendarDate(
                        feed=self.feed,
                        service_id=row['service_id'],
                        date=datetime.strptime(row['date'], '%Y%m%d').date(),
                        exception_type=int(row['exception_type']),
                    ))
                CalendarDate.objects.bulk_create(dates, batch_size=1000, ignore_conflicts=True)
            self.stdout.write(f'  Imported {len(dates)} calendar date exceptions')
        except KeyError:
            self.stdout.write('  No calendar_dates.txt found (optional)')

    def import_shapes(self):
        self.stdout.write('Importing shapes...')
        try:
            with self.zipfile.open('shapes.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8-sig'))
                shapes = []
                for row in reader:
                    shapes.append(Shape(
                        feed=self.feed,
                        shape_id=row['shape_id'],
                        shape_pt_lat=Decimal(row['shape_pt_lat']),
                        shape_pt_lon=Decimal(row['shape_pt_lon']),
                        shape_pt_sequence=int(row['shape_pt_sequence']),
                    ))
                Shape.objects.bulk_create(shapes, batch_size=1000, ignore_conflicts=True)
            self.stdout.write(f'  Imported {len(shapes)} shape points')
        except KeyError:
            self.stdout.write('  No shapes.txt found (optional)')

    def import_trips(self):
        self.stdout.write('Importing trips...')
        with self.zipfile.open('trips.txt') as f:
            reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8-sig'))
            trips = []
            for row in reader:
                trips.append(Trip(
                    feed=self.feed,
                    route_id=row['route_id'],
                    service_id=row['service_id'],
                    trip_id=row['trip_id'],
                    trip_headsign=row.get('trip_headsign', ''),
                    direction_id=int(row.get('direction_id', 0)),
                    shape_id=row.get('shape_id', ''),
                    wheelchair_accessible=int(row.get('wheelchair_accessible', 0)),
                    bikes_allowed=int(row.get('bikes_allowed', 0)),
                ))
            Trip.objects.bulk_create(trips, batch_size=1000, ignore_conflicts=True)
        self.stdout.write(f'  Imported {len(trips)} trips')

    def import_stop_times(self):
        self.stdout.write('Importing stop times (this may take a while)...')
        with self.zipfile.open('stop_times.txt') as f:
            reader = csv.DictReader(io.TextIOWrapper(f, 'utf-8-sig'))
            stop_times = []
            count = 0
            for row in reader:
                stop_times.append(StopTime(
                    feed=self.feed,
                    trip_id=row['trip_id'],
                    stop_id=row['stop_id'],
                    stop_sequence=int(row['stop_sequence']),
                    arrival_time=self._parse_time(row.get('arrival_time')),
                    departure_time=self._parse_time(row.get('departure_time')),
                    pickup_type=int(row.get('pickup_type', 0)),
                    drop_off_type=int(row.get('drop_off_type', 0)),
                ))
                
                if len(stop_times) >= 5000:
                    StopTime.objects.bulk_create(stop_times, batch_size=5000, ignore_conflicts=True)
                    count += len(stop_times)
                    self.stdout.write(f'    {count} stop times...', ending='\r')
                    stop_times = []
            
            if stop_times:
                StopTime.objects.bulk_create(stop_times, batch_size=5000, ignore_conflicts=True)
                count += len(stop_times)
        
        self.stdout.write(f'  Imported {count} stop times')

    def _parse_time(self, time_str):
        """Parse GTFS time format (HH:MM:SS, may be >24 hours)"""
        if not time_str:
            return None
        try:
            h, m, s = time_str.split(':')
            # GTFS allows times > 24 hours (e.g., 25:30:00 for 1:30 AM next day)
            # Django TimeField can't handle this, so we cap at 23:59:59
            hours = min(int(h), 23)
            return f"{hours:02d}:{m}:{s}"
        except:
            return None