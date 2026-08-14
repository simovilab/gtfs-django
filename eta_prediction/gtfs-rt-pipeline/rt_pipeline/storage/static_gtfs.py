"""Weekly dated snapshots of each agency's static GTFS feed.

Static GTFS (stops/routes/trips/shapes) changes only occasionally, but every
realtime observation collected during the 90-day replication window needs to
be matchable back to "what schedule was in effect" at the time it was
recorded. Each snapshot is the upstream zip stored verbatim (no parsing) at
``feeds/<agency>/gtfs_static/<ISO date>.zip`` -- see docs/S3_LAYOUT.md and
roadmap 0.2.

Uploads go through `mc pipe` rather than DuckDB's httpfs (used elsewhere in
this package for Parquet) because this is one opaque binary blob, not a
columnar dataset. Credential resolution is delegated to
``rt_pipeline.compaction.credentials`` rather than reimplemented here.
"""

from __future__ import annotations

import datetime as dt
import subprocess
import zipfile
from io import BytesIO

import requests

# Present in every real GTFS feed regardless of agency/protocol; their
# absence means the URL returned something other than a GTFS zip (an error
# page, an empty response, a redirect to a login wall) that should not be
# uploaded under a dated key that later steps would trust.
REQUIRED_MEMBERS = ("stops.txt", "routes.txt")


class StaticGtfsError(RuntimeError):
    """The upstream feed did not return a usable GTFS zip, or upload failed."""


def fetch(url: str, *, timeout: int = 60) -> bytes:
    """Download and sanity-check a static GTFS zip.

    Raises `StaticGtfsError` on anything that isn't a real GTFS feed rather
    than silently uploading garbage under a dated key.
    """
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    content = resp.content
    try:
        zf = zipfile.ZipFile(BytesIO(content))
        bad_member = zf.testzip()
    except zipfile.BadZipFile as exc:
        raise StaticGtfsError(f"{url} did not return a valid zip") from exc
    if bad_member is not None:
        raise StaticGtfsError(f"{url}: corrupt member {bad_member!r} in zip")
    missing = [m for m in REQUIRED_MEMBERS if m not in zf.namelist()]
    if missing:
        raise StaticGtfsError(f"{url}: zip is missing required GTFS files: {missing}")
    return content


def put_snapshot(
    content: bytes,
    agency: str,
    snapshot_date: dt.date,
    *,
    alias: str = "simovilab",
    bucket: str = "transit",
) -> str:
    """Upload `content` to feeds/<agency>/gtfs_static/<ISO date>.zip.

    Returns the bucket-relative key. Resolves `mc` credentials the same way
    `rt_pipeline.compaction.run_compaction` does (an `mc` alias, else
    environment variables).
    """
    from ..compaction.credentials import load_credentials

    load_credentials(alias)
    key = f"feeds/{agency}/gtfs_static/{snapshot_date.isoformat()}.zip"
    target = f"{alias}/{bucket}/{key}"
    proc = subprocess.run(["mc", "pipe", target], input=content, capture_output=True)
    if proc.returncode != 0:
        raise StaticGtfsError(
            f"mc pipe failed for {target}: {proc.stderr.decode(errors='replace').strip()}"
        )
    return key
