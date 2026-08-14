"""Atomic, human- and machine-readable status snapshots for the RT collectors.

Why: when a collector goes quiet at 2am, the fastest diagnostic is
``ssh ... cat /var/lib/simovi/status/<name>.txt`` or ``tail -f
.../<name>.events.log`` -- no Django shell, no log-grepping. This module is
the write side of that contract; a later ops script (``simovi-status``) is
the read side.

Per name (e.g. ``mbta``), three files:

- ``<name>.json``  -- machine-readable, merged state
- ``<name>.txt``   -- human snapshot, safe to ``cat``/``watch``
- ``<name>.events.log`` -- append-only, one line per update, safe to
  ``tail -f`` (a rewritten snapshot file breaks ``tail -f``, hence the split)

Snapshot writes (json + txt) are ATOMIC: written to a temp file in the same
directory, then ``os.replace``d into place. A reader can never observe a
partially-written file -- a half-written status file that gets ``cat``'d
during an incident is worse than a missing one.

Deliberately free of Django imports so it's usable standalone from an ops
script and unit-testable without a settings module; callers (Celery tasks)
read ``settings.STATUS_DIR`` themselves and pass it in.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

DEFAULT_STATUS_DIR = "/var/lib/simovi/status"

# Keys that are computed fresh on every render rather than carried forward by
# the merge -- stripped from "existing" before merging so a stale computed
# value (e.g. last poll's age, frozen at write time) never lingers in the
# persisted json and gets treated as a real field.
_COMPUTED_KEYS = {"last_poll_age_s", "_updated_at_utc"}

# Events-log size cap. Only notable events are logged (see `update`), so this
# is a backstop against a pathological error loop, not routine rotation.
_EVENTS_MAX_BYTES = 5 * 1024 * 1024
_EVENTS_KEEP_LINES = 2000


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _status_dir(status_dir: Optional[str]) -> Path:
    return Path(status_dir or DEFAULT_STATUS_DIR)


def _atomic_write(path: Path, content: str) -> None:
    """Write ``content`` to ``path`` via temp-file-then-rename in the same dir."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        # mkstemp creates 0600 and os.replace preserves the mode. The container
        # runs as root while these files are read from the host as an ordinary
        # user, so 0600 makes them unreadable and defeats the point.
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w") as f:
            f.write(content)
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _last_poll_age_s(merged: dict[str, Any]) -> Optional[float]:
    """Seconds since ``last_poll_utc``, computed fresh on every render (never
    persisted) so the age reflects wall-clock time even if nothing has
    updated the status file since."""
    raw = merged.get("last_poll_utc")
    if not raw:
        return None
    try:
        last_poll = dt.datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    if last_poll.tzinfo is None:
        last_poll = last_poll.replace(tzinfo=dt.timezone.utc)
    return (_now_utc() - last_poll).total_seconds()


def _render_txt(name: str, merged: dict[str, Any], age_s: Optional[float]) -> str:
    lines = [f"# {name} status", f"rendered_at_utc: {_now_utc().isoformat()}"]
    if age_s is not None:
        lines.append(f"last_poll_age_s: {age_s:.1f}")
    for key in sorted(merged):
        lines.append(f"{key}: {merged[key]}")
    return "\n".join(lines) + "\n"


def update(
    name: str,
    status_dir: Optional[str] = None,
    *,
    event: bool = False,
    **fields: Any,
) -> None:
    """Merge ``fields`` into the persisted status for ``name``.

    Reads the existing ``<name>.json`` (if any), merges in ``fields``
    (new keys win), and rewrites both ``<name>.json`` and ``<name>.txt``
    atomically.

    ``event=True`` additionally appends a line to ``<name>.events.log``.
    Pass it only for *notable* transitions — a flush, an error, a
    compaction — never for routine polls. At a 5s poll interval an
    unconditional event line is ~3.7 MB/day of append-only file that
    nothing rotates, and it buries the interesting lines so deeply that
    `tail -f` is useless. Current state belongs in the snapshot, which
    `cat` reads; the log is for things that happened.

    Never raises: this is observability, not the job. A status-write
    failure (e.g. read-only filesystem, missing directory permissions) is
    logged and swallowed so it can never fail the calling task.
    """
    try:
        _update(name, status_dir, fields, event=event)
    except Exception:  # never let status reporting break the caller
        logger.exception("rt_pipeline.status.update failed for %r", name)


def _update(
    name: str,
    status_dir: Optional[str],
    fields: dict[str, Any],
    *,
    event: bool = False,
) -> None:
    d = _status_dir(status_dir)
    json_path = d / f"{name}.json"
    txt_path = d / f"{name}.txt"
    log_path = d / f"{name}.events.log"

    existing: dict[str, Any] = {}
    if json_path.exists():
        try:
            existing = json.loads(json_path.read_text())
        except (OSError, json.JSONDecodeError):
            existing = {}
    existing = {k: v for k, v in existing.items() if k not in _COMPUTED_KEYS}

    merged = {**existing, **fields}
    merged["_updated_at_utc"] = _now_utc().isoformat()
    age_s = _last_poll_age_s(merged)

    json_out = dict(merged)
    if age_s is not None:
        json_out["last_poll_age_s"] = age_s
    _atomic_write(json_path, json.dumps(json_out, indent=2, sort_keys=True, default=str))
    _atomic_write(txt_path, _render_txt(name, merged, age_s))

    if not event:
        return

    d.mkdir(parents=True, exist_ok=True)
    # Cheap size cap: the events log is append-only and nothing else rotates
    # it, so keep the tail rather than letting it grow for 90 days.
    try:
        if log_path.exists() and log_path.stat().st_size > _EVENTS_MAX_BYTES:
            kept = log_path.read_text().splitlines()[-_EVENTS_KEEP_LINES:]
            _atomic_write(log_path, "\n".join(kept) + "\n")
    except OSError:
        pass

    record = {"ts_utc": _now_utc().isoformat(), **fields}
    with open(log_path, "a") as f:
        f.write(json.dumps(record, sort_keys=True, default=str) + "\n")


def read(name: str, status_dir: Optional[str] = None) -> dict[str, Any]:
    """Read back the merged json status for ``name``. Returns ``{}`` if absent
    or unreadable -- used by tests and, later, the ``simovi-status`` script."""
    json_path = _status_dir(status_dir) / f"{name}.json"
    try:
        return json.loads(json_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
