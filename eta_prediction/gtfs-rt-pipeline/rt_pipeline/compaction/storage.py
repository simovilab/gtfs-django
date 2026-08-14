"""Storage backends behind one interface.

Production talks to MinIO through the `mc` CLI for listing/deleting/renaming
(cheap, delimited operations -- a single MBTA day can hold hundreds of
thousands of objects, so nothing here ever lists them one by one). Tests use
a plain `pathlib` directory instead, with no `mc` or network dependency.

DuckDB never talks to either backend directly: it gets a DuckDB-readable URI
from `Storage.uri()` (an `s3://...` string or a filesystem path). That is
what makes the compaction logic in `duckdb_ops.py` and `execution.py` the
same code under test and in production -- only `uri()`/`child_dirs()`/etc.
differ.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class CommandError(RuntimeError):
    pass


def run(cmd: list[str], env: dict | None = None) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise CommandError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr.strip()}"
        )
    return proc.stdout


class Storage(Protocol):
    """Bucket-relative key operations. `key` never includes the alias/bucket
    (McStorage) or the root directory (LocalStorage) -- callers only ever see
    keys like ``feeds/mbta/vehicle_positions/year=2026/...``."""

    def uri(self, key: str) -> str:
        """DuckDB-readable location for `key` (an ``s3://...`` URI or a path)."""
        ...

    def child_dirs(self, key: str) -> list[str]:
        """Immediate child directory names directly under `key`."""
        ...

    def list_recursive(self, key: str) -> list[str]:
        """All file keys under `key`, relative to it. Only use on small prefixes."""
        ...

    def has_any_object(self, key: str) -> bool:
        """True if at least one file or directory exists directly under `key`."""
        ...

    def exists(self, key: str) -> bool:
        """True if `key` is exactly an existing file."""
        ...

    def remove_prefix(self, key: str) -> None:
        """Recursively delete everything under `key`."""
        ...

    def move(self, src_key: str, dst_key: str) -> None:
        ...

    def remove(self, key: str) -> None:
        ...


@dataclass
class McStorage:
    """Production backend: `mc` for list/delete/rename, `s3://` URIs for DuckDB."""

    alias: str
    bucket: str
    env: dict | None = None

    def _p(self, key: str) -> str:
        return f"{self.alias}/{self.bucket}/{key}"

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    @staticmethod
    def _parse(out: str) -> list[dict]:
        return [json.loads(line) for line in out.splitlines() if line.strip()]

    def child_dirs(self, key: str) -> list[str]:
        out = run(["mc", "ls", "--json", self._p(key.rstrip("/") + "/")], self.env)
        return sorted(
            e["key"].rstrip("/") for e in self._parse(out) if e.get("type") == "folder"
        )

    def list_recursive(self, key: str) -> list[str]:
        out = run(
            ["mc", "ls", "--recursive", "--json", self._p(key.rstrip("/") + "/")], self.env
        )
        return [e["key"] for e in self._parse(out) if e.get("type") == "file"]

    def has_any_object(self, key: str) -> bool:
        out = run(["mc", "ls", "--json", self._p(key.rstrip("/") + "/")], self.env)
        return any(e.get("type") in ("file", "folder") for e in self._parse(out))

    def exists(self, key: str) -> bool:
        try:
            run(["mc", "stat", "--json", self._p(key)], self.env)
            return True
        except CommandError:
            return False

    def remove_prefix(self, key: str) -> None:
        run(["mc", "rm", "--recursive", "--force", self._p(key.rstrip("/") + "/")], self.env)

    def move(self, src_key: str, dst_key: str) -> None:
        run(["mc", "mv", self._p(src_key), self._p(dst_key)], self.env)

    def remove(self, key: str) -> None:
        run(["mc", "rm", "--force", self._p(key)], self.env)


@dataclass
class LocalStorage:
    """Test/dev backend: a plain directory tree, no S3 or `mc` involved."""

    root: Path

    def _p(self, key: str) -> Path:
        return self.root / key

    def uri(self, key: str) -> str:
        return str(self._p(key))

    def child_dirs(self, key: str) -> list[str]:
        p = self._p(key)
        if not p.is_dir():
            return []
        return sorted(e.name for e in p.iterdir() if e.is_dir())

    def list_recursive(self, key: str) -> list[str]:
        p = self._p(key)
        if not p.is_dir():
            return []
        return sorted(str(f.relative_to(p)) for f in p.rglob("*") if f.is_file())

    def has_any_object(self, key: str) -> bool:
        p = self._p(key)
        return p.is_dir() and any(p.iterdir())

    def exists(self, key: str) -> bool:
        return self._p(key).is_file()

    def remove_prefix(self, key: str) -> None:
        p = self._p(key)
        if p.exists():
            shutil.rmtree(p)

    def move(self, src_key: str, dst_key: str) -> None:
        src, dst = self._p(src_key), self._p(dst_key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))

    def remove(self, key: str) -> None:
        p = self._p(key)
        if p.exists():
            p.unlink()
