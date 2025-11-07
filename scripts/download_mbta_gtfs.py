#!/usr/bin/env python3
"""
download_mbta_gtfs.py
--------------------------------------------------------
Utility script to fetch a real GTFS Schedule feed (MBTA)
and test end-to-end import/export functionality.

Usage:
    python scripts/download_mbta_gtfs.py
"""

import os
import requests
import subprocess
from pathlib import Path

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

MBTA_GTFS_URL = "https://cdn.mbta.com/MBTA_GTFS.zip"
BASE_DIR = Path(__file__).resolve().parent.parent
TMP_DIR = BASE_DIR / "tmp_gtfs"
TMP_DIR.mkdir(exist_ok=True)

FEED_ZIP = TMP_DIR / "MBTA_GTFS.zip"

# ---------------------------------------------------------------------
# Step 1. Download the feed
# ---------------------------------------------------------------------
print(f"Downloading GTFS feed from {MBTA_GTFS_URL}...")
resp = requests.get(MBTA_GTFS_URL, timeout=60)
resp.raise_for_status()

with open(FEED_ZIP, "wb") as f:
    f.write(resp.content)

print(f"Saved feed to: {FEED_ZIP}")

# ---------------------------------------------------------------------
# Step 2. Import into database
# ---------------------------------------------------------------------
print("Importing GTFS feed into database...")
subprocess.run(
    ["python", "manage.py", "importgtfs", str(FEED_ZIP)],
    cwd=BASE_DIR,
    check=True,
)

# ---------------------------------------------------------------------
# Step 3. Export back to GTFS ZIP
# ---------------------------------------------------------------------
print("Exporting GTFS feed from database...")
subprocess.run(
    ["python", "manage.py", "exportgtfs"],
    cwd=BASE_DIR,
    check=True,
)

exported_zip = TMP_DIR / "exported_feed.zip"
if exported_zip.exists():
    print(f"Successfully exported GTFS feed to {exported_zip}")
else:
    print("Exported ZIP not found — check logs for details.")
