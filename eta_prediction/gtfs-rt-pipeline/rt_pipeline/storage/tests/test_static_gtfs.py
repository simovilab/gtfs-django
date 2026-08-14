"""Tests for weekly static GTFS snapshots (rt_pipeline.storage.static_gtfs).

`requests.get`, `subprocess.run`, and credential resolution are all
monkeypatched -- no network or `mc` dependency, same spirit as the other
storage tests running against a local tmpdir instead of real S3.
"""

from __future__ import annotations

import datetime as dt
import io
import zipfile

import pytest
import requests

from rt_pipeline.storage import static_gtfs as sg


def _zip_bytes(members: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return buf.getvalue()


_VALID_GTFS = {
    "stops.txt": "stop_id,stop_name\n1,Main St\n",
    "routes.txt": "route_id,route_short_name\nR1,1\n",
}


class _FakeResponse:
    def __init__(self, content: bytes, *, status: int = 200):
        self.content = content
        self._status = status

    def raise_for_status(self) -> None:
        if self._status >= 400:
            raise requests.HTTPError(f"HTTP {self._status}")


def test_fetch_returns_bytes_for_valid_gtfs(monkeypatch):
    body = _zip_bytes(_VALID_GTFS)
    monkeypatch.setattr(sg.requests, "get", lambda url, timeout: _FakeResponse(body))

    result = sg.fetch("https://example.org/gtfs.zip")

    assert result == body


def test_fetch_raises_on_non_zip(monkeypatch):
    monkeypatch.setattr(
        sg.requests, "get", lambda url, timeout: _FakeResponse(b"<html>not a zip</html>")
    )

    with pytest.raises(sg.StaticGtfsError, match="valid zip"):
        sg.fetch("https://example.org/gtfs.zip")


def test_fetch_raises_on_missing_required_members(monkeypatch):
    body = _zip_bytes({"stops.txt": "stop_id\n1\n"})  # routes.txt missing
    monkeypatch.setattr(sg.requests, "get", lambda url, timeout: _FakeResponse(body))

    with pytest.raises(sg.StaticGtfsError, match="missing required GTFS files"):
        sg.fetch("https://example.org/gtfs.zip")


def test_fetch_raises_on_http_error(monkeypatch):
    monkeypatch.setattr(
        sg.requests, "get", lambda url, timeout: _FakeResponse(b"", status=404)
    )

    with pytest.raises(requests.HTTPError):
        sg.fetch("https://example.org/gtfs.zip")


class _FakeCompletedProcess:
    def __init__(self, returncode: int, stderr: bytes = b""):
        self.returncode = returncode
        self.stderr = stderr


def test_put_snapshot_builds_dated_key_and_pipes_content(monkeypatch):
    calls = {}

    def fake_run(cmd, input, capture_output):
        calls["cmd"] = cmd
        calls["input"] = input
        return _FakeCompletedProcess(0)

    monkeypatch.setattr(sg.subprocess, "run", fake_run)
    monkeypatch.setattr(
        "rt_pipeline.compaction.credentials.load_credentials", lambda alias: None
    )

    key = sg.put_snapshot(b"zip-bytes", "mbta", dt.date(2026, 8, 17))

    assert key == "feeds/mbta/gtfs_static/2026-08-17.zip"
    assert calls["cmd"] == ["mc", "pipe", "simovilab/transit/feeds/mbta/gtfs_static/2026-08-17.zip"]
    assert calls["input"] == b"zip-bytes"


def test_put_snapshot_raises_on_mc_failure(monkeypatch):
    monkeypatch.setattr(
        sg.subprocess,
        "run",
        lambda cmd, input, capture_output: _FakeCompletedProcess(1, b"access denied"),
    )
    monkeypatch.setattr(
        "rt_pipeline.compaction.credentials.load_credentials", lambda alias: None
    )

    with pytest.raises(sg.StaticGtfsError, match="access denied"):
        sg.put_snapshot(b"zip-bytes", "bucr", dt.date(2026, 8, 17))
