from celery import shared_task
import requests
import zipfile
import io
from django.db import transaction
from .models import Feed, Agency, Stop, Route, Trip, StopTime, Calendar
from .importers import GTFSImporter  # You'll need to write this

@shared_task(queue='static')
def fetch_and_import_gtfs_schedule():
    """
    Download GTFS .zip, extract, parse CSVs, bulk insert.
    Run this less frequently (e.g., daily or when feed updates).
    """
    url = os.getenv('GTFS_SCHEDULE_ZIP_URL')
    
    # Download
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    # Check if feed changed (via ETag or Last-Modified)
    etag = response.headers.get('ETag')
    last_modified = response.headers.get('Last-Modified')
    
    # Create Feed record
    feed_id = f"{os.getenv('FEED_NAME')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        importer = GTFSImporter(feed_id, z)
        importer.import_all()
    
    return f"Imported {feed_id}"