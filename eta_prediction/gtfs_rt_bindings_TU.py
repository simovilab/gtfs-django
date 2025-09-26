from google.transit import gtfs_realtime_pb2
import requests
from datetime import datetime, timezone

"""
This script takes a GTFS-RT Trip Updates feed and prints out the first N trip updates.
"""
N = 10
# URL = "https://cdn.mbta.com/realtime/TripUpdates.pb"
URL = "https://databus.bucr.digital/feed/realtime/trip_updates.pb" # bUCR Realtime Trip Updates feed

feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get(URL)
feed.ParseFromString(response.content)

for entity in feed.entity[:N]:  # show first N trip updates
    if entity.HasField('trip_update'):
        print(entity.trip_update)
