"""DuckDB-backed compaction primitives: dedup, merge, and repartition.

Uses the `duckdb` Python module directly rather than shelling out to the
`duckdb` CLI binary -- one less external binary the tests (or a minimal
container) need installed, and it lets `Storage.uri()` hand back either an
``s3://`` URI or a plain filesystem path with no other code change.

Both compaction shapes share the same dedup mechanics: read every source
file (resolved via DuckDB's ``glob()``, so a leaf with zero matching files
degrades to an empty result instead of raising), keep one row per natural
key -- the most recently ingested copy -- via
``QUALIFY row_number() OVER (PARTITION BY <key> ORDER BY <recency> DESC) = 1``,
then write the result:

  * `compact_leaf`        -- merge into a single file (legacy in-place leaves,
                              and bUCR's staging -> curated, which has no
                              route-level split).
  * `compact_partitioned` -- merge and re-partition by `partition_cols`
                              (MBTA's staging -> curated: hourly staging
                              objects, partitioned only by day, are rewritten
                              into `route_id=<r>/<date>.parquet` cells).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Sequence
from urllib.parse import quote

import duckdb

if TYPE_CHECKING:
    from .credentials import S3Credentials


def _lit(value: str) -> str:
    """SQL-quote a string literal."""
    return "'" + value.replace("'", "''") + "'"


def _prepare_local_dst(uri: str, *, is_dir: bool) -> None:
    """Ensure the destination directory exists for a local-path write.

    Unlike ``s3://`` (a virtual, directory-less namespace), DuckDB's local
    Parquet writer does not create missing parent directories -- it only
    creates the leaf directory it names, and errors if that leaf's own
    parent is missing. A no-op for ``s3://`` URIs.
    """
    if uri.startswith("s3://"):
        return
    target = Path(uri) if is_dir else Path(uri).parent
    target.mkdir(parents=True, exist_ok=True)


def _sql_list(values: Sequence[str]) -> str:
    return "[" + ", ".join(_lit(v) for v in values) + "]"


@dataclass(frozen=True)
class CompactionCounts:
    src: int  # rows read across every source file, before dedup
    dst: int  # rows written, after dedup

    @property
    def dupes_removed(self) -> int:
        return self.src - self.dst


class DuckDB:
    """Thin wrapper around a configured DuckDB connection."""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con

    @classmethod
    def connect_local(cls) -> "DuckDB":
        return cls(duckdb.connect())

    @classmethod
    def connect_s3(cls, creds: "S3Credentials") -> "DuckDB":
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(
            f"""
            CREATE OR REPLACE SECRET s (
                TYPE s3, PROVIDER config,
                KEY_ID {_lit(creds.access_key)},
                SECRET {_lit(creds.secret_key)},
                ENDPOINT {_lit(creds.endpoint)},
                USE_SSL {str(creds.use_ssl).lower()},
                URL_STYLE 'path'
            );
            """
        )
        return cls(con)

    def clone(self) -> "DuckDB":
        """A thread-local cursor sharing this connection's catalog (secrets
        included) -- the supported way to run DuckDB concurrently from
        multiple threads against one database."""
        return DuckDB(self.con.cursor())

    # -- internals -----------------------------------------------------

    def _glob(self, pattern: str) -> list[str]:
        try:
            rows = self.con.execute(f"SELECT file FROM glob({_lit(pattern)})").fetchall()
        except duckdb.IOException:
            return []
        return [r[0] for r in rows]

    @staticmethod
    def _read_sql(files: Sequence[str], hive_partitioning: bool) -> str:
        return (
            f"SELECT * FROM read_parquet({_sql_list(files)}, "
            f"hive_partitioning={str(hive_partitioning).lower()}, union_by_name=true)"
        )

    def _source_sql(self, sources: Sequence[tuple[str, bool]]) -> tuple[str | None, bool]:
        """Build the unioned source query for one or more (glob, hive_partitioning)
        pairs. Each pair gets its own `read_parquet()` call, combined with
        `UNION ALL BY NAME` -- required because DuckDB refuses to mix files
        with different Hive partition schemes (e.g. staging's `year=/month=/day=`
        vs. curated's `year=/month=/day=/route_id=`) inside a single call.

        Returns (sql, any_missing): `sql` is None if every pattern matched
        nothing.
        """
        parts: list[str] = []
        any_missing = False
        for pattern, hive_partitioning in sources:
            files = self._glob(pattern)
            if not files:
                any_missing = True
                continue
            parts.append(f"({self._read_sql(files, hive_partitioning)})")
        if not parts:
            return None, any_missing
        return " UNION ALL BY NAME ".join(parts), any_missing

    @staticmethod
    def _dedup_sql(source_sql: str, dedup_keys: Sequence[str], recency_col: str) -> str:
        keys = ", ".join(dedup_keys)
        return (
            f"SELECT * FROM ({source_sql}) "
            f"QUALIFY row_number() OVER (PARTITION BY {keys} ORDER BY {recency_col} DESC) = 1"
        )

    # -- public API ------------------------------------------------------

    def compact_leaf(
        self,
        sources: Sequence[tuple[str, bool]],
        dst_uri: str,
        *,
        sort_col: str,
        dedup_keys: Sequence[str],
        recency_col: str,
    ) -> CompactionCounts:
        """Union every file matching `sources`, dedup, write one file to `dst_uri`.

        `sources` is a list of ``(glob_pattern, hive_partitioning)`` pairs --
        one entry per origin with potentially different Hive partition
        schemes (e.g. a flat staging glob plus an already-partitioned
        curated glob).
        """
        read_sql, _ = self._source_sql(sources)
        if read_sql is None:
            return CompactionCounts(0, 0)

        dedup_sql = self._dedup_sql(read_sql, dedup_keys, recency_col)
        src = self.con.execute(f"SELECT count(*) FROM ({read_sql})").fetchone()[0]
        dst = self.con.execute(f"SELECT count(*) FROM ({dedup_sql})").fetchone()[0]
        if dst == 0:
            return CompactionCounts(src, 0)

        _prepare_local_dst(dst_uri, is_dir=False)
        self.con.execute(
            f"COPY (SELECT * FROM ({dedup_sql}) ORDER BY {sort_col}) TO {_lit(dst_uri)} "
            "(FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 122880)"
        )
        return CompactionCounts(src, dst)

    def compact_partitioned(
        self,
        sources: Sequence[tuple[str, bool]],
        dst_dir_uri: str,
        *,
        sort_col: str,
        dedup_keys: Sequence[str],
        recency_col: str,
        partition_cols: Sequence[str],
    ) -> CompactionCounts:
        """Union every file matching `sources`, dedup, write partitioned by
        `partition_cols` under `dst_dir_uri`, one file per partition cell.

        `sources` is a list of ``(glob_pattern, hive_partitioning)`` pairs,
        see `compact_leaf`. The curated side of a staging->curated union
        typically needs ``hive_partitioning=True`` -- it stores its
        partition columns (e.g. `route_id`) only in the path, so they must
        be reconstructed to appear as real columns again before they can be
        re-partitioned.

        Writes each partition cell with its own single-file `COPY` -- the
        same technique `compact_leaf` uses -- rather than DuckDB's own
        `PARTITION_BY` writer. `PARTITION_BY` was tried first, including
        `PRAGMA threads=1` to force one output file per cell, but production
        data still produced 2-4 files for dozens of MBTA route partitions on
        the first automatic run (2026-08-14): the partitioned Parquet
        writer's own internal sink parallelism isn't fully governed by that
        pragma. Filtering into per-cell `COPY` calls sidesteps that writer
        path entirely, at the cost of one extra table scan per distinct
        partition value -- dedup'd rows are materialized into a temp table
        first so that cost is a cheap in-memory scan, not a re-run of the
        union + dedup per cell.
        """
        read_sql, _ = self._source_sql(sources)
        if read_sql is None:
            return CompactionCounts(0, 0)

        dedup_sql = self._dedup_sql(read_sql, dedup_keys, recency_col)
        src = self.con.execute(f"SELECT count(*) FROM ({read_sql})").fetchone()[0]

        part_col_list = ", ".join(partition_cols)
        self.con.execute("DROP TABLE IF EXISTS _compact_partitioned")
        self.con.execute(
            f"CREATE TEMP TABLE _compact_partitioned AS "
            f"SELECT * FROM ({dedup_sql}) ORDER BY {part_col_list}, {sort_col}"
        )
        try:
            dst = self.con.execute("SELECT count(*) FROM _compact_partitioned").fetchone()[0]
            if dst == 0:
                return CompactionCounts(src, 0)

            cells = self.con.execute(
                f"SELECT DISTINCT {part_col_list} FROM _compact_partitioned"
            ).fetchall()
            for cell in cells:
                where = " AND ".join(
                    f"{col} = {_lit(str(val))}" for col, val in zip(partition_cols, cell)
                )
                part_dir = "/".join(
                    f"{col}={quote(str(val), safe='')}"
                    for col, val in zip(partition_cols, cell)
                )
                file_uri = f"{dst_dir_uri.rstrip('/')}/{part_dir}/data_0.parquet"
                _prepare_local_dst(file_uri, is_dir=False)
                self.con.execute(
                    f"COPY (SELECT * EXCLUDE ({part_col_list}) FROM _compact_partitioned "
                    f"WHERE {where} ORDER BY {sort_col}) TO {_lit(file_uri)} "
                    "(FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 122880)"
                )
        finally:
            self.con.execute("DROP TABLE IF EXISTS _compact_partitioned")
        return CompactionCounts(src, dst)
