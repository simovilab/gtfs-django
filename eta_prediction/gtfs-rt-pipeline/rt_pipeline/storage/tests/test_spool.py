"""Tests for the local DuckDB spool (rt_pipeline.storage.spool).

Run against tmp_path so there's no dependency on Django settings or a real
S3/MinIO endpoint -- flush_to_staging is exercised against a local directory
the same way s3_writer's own tests exercise write_vehicle_positions (DuckDB
reads/writes a local Hive-partitioned directory identically to ``s3://``).
"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from rt_pipeline.storage.spool import NATURAL_KEY, SPOOL_COLUMNS, Spool, flush_to_staging


def _sample(
    n: int,
    *,
    day: int = 2,
    hour: int = 8,
    vehicle_prefix: str = "v",
    route_id: str = "Green-D",
    ingested_at: dt.datetime | None = None,
) -> pd.DataFrame:
    """Build a valid spool-input DataFrame with ``n`` distinct-key rows."""
    base = dt.datetime(2025, 1, day, hour, 0, 0, tzinfo=dt.timezone.utc)
    ingested = ingested_at or base
    return pd.DataFrame(
        {
            "feed_name": ["mbta"] * n,
            "vehicle_id": [f"{vehicle_prefix}{i}" for i in range(n)],
            "ts": [base + dt.timedelta(seconds=15 * i) for i in range(n)],
            "lat": [42.0 + 0.001 * i for i in range(n)],
            "lon": [-71.0 - 0.001 * i for i in range(n)],
            "bearing": [90.0] * n,
            "speed": [10.0] * n,
            "trip_id": [f"trip-{i}" for i in range(n)],
            "current_stop_sequence": list(range(n)),
            "current_status": ["IN_TRANSIT_TO"] * n,
            "stop_id": [f"stop-{i}" for i in range(n)],
            "ingested_at": [ingested] * n,
            "route_id": [route_id] * n,
        }
    )


def _spool(tmp_path) -> Spool:
    return Spool(str(tmp_path / "vp_spool.duckdb"))


# --- append / dedup -------------------------------------------------------


def test_append_columns_match_natural_key_contract():
    # Sanity: the natural key documented in S3_LAYOUT.md is exactly this.
    assert NATURAL_KEY == ("feed_name", "vehicle_id", "ts")
    assert set(NATURAL_KEY) <= set(SPOOL_COLUMNS)


def test_append_dedups_on_reinsert(tmp_path):
    sp = _spool(tmp_path)
    df = _sample(1)

    first = sp.append(df)
    second = sp.append(df)  # identical (feed_name, vehicle_id, ts)

    assert first == 1
    assert second == 0
    assert sp.stats()["rows"] == 1


def test_append_dedups_even_with_different_ingested_at(tmp_path):
    sp = _spool(tmp_path)
    df1 = _sample(1, ingested_at=dt.datetime(2025, 1, 2, 8, 0, 0, tzinfo=dt.timezone.utc))
    df2 = _sample(1, ingested_at=dt.datetime(2025, 1, 2, 8, 5, 0, tzinfo=dt.timezone.utc))

    first = sp.append(df1)
    second = sp.append(df2)  # same natural key, later ingested_at

    assert first == 1
    assert second == 0
    assert sp.stats()["rows"] == 1


def test_append_partial_overlap_only_counts_new_rows(tmp_path):
    sp = _spool(tmp_path)
    sp.append(_sample(3, vehicle_prefix="v"))
    # v0..v2 collide; w0..w2 are new.
    inserted = sp.append(
        pd.concat([_sample(3, vehicle_prefix="v"), _sample(3, vehicle_prefix="w")], ignore_index=True)
    )
    assert inserted == 3
    assert sp.stats()["rows"] == 6


def test_append_empty_df_is_noop(tmp_path):
    sp = _spool(tmp_path)
    assert sp.append(pd.DataFrame()) == 0
    assert sp.stats()["rows"] == 0


def test_append_missing_columns_raises(tmp_path):
    sp = _spool(tmp_path)
    df = _sample(1).drop(columns=["lat"])
    with pytest.raises(ValueError):
        sp.append(df)


# --- select_before / delete_before -----------------------------------------


def test_select_and_delete_before_are_half_open(tmp_path):
    sp = _spool(tmp_path)
    # Rows at exactly 08:00:00, 08:00:15, 08:00:30 (n=3, 15s apart).
    sp.append(_sample(3, hour=8))
    cutoff = dt.datetime(2025, 1, 2, 8, 0, 15, tzinfo=dt.timezone.utc)

    before = sp.select_before(cutoff)
    # Half-open [.., cutoff): the row AT the cutoff (08:00:15) is excluded.
    assert sorted(before["ts"].dt.to_pydatetime()) == [
        dt.datetime(2025, 1, 2, 8, 0, 0)
    ]

    deleted = sp.delete_before(cutoff)
    assert deleted == 1
    assert sp.stats()["rows"] == 2

    remaining = sp.select_before(dt.datetime(2025, 1, 3, tzinfo=dt.timezone.utc))
    assert sorted(remaining["ts"].dt.to_pydatetime()) == [
        dt.datetime(2025, 1, 2, 8, 0, 15),
        dt.datetime(2025, 1, 2, 8, 0, 30),
    ]


def test_delete_before_is_a_separate_call_from_select(tmp_path):
    """select_before must never delete -- flush needs write-then-verify-then-delete."""
    sp = _spool(tmp_path)
    sp.append(_sample(2))
    cutoff = dt.datetime(2025, 1, 3, tzinfo=dt.timezone.utc)

    sp.select_before(cutoff)
    assert sp.stats()["rows"] == 2  # untouched by select

    sp.delete_before(cutoff)
    assert sp.stats()["rows"] == 0


# --- stats -------------------------------------------------------------


def test_stats_on_empty_spool_does_not_crash(tmp_path):
    sp = _spool(tmp_path)
    stats = sp.stats()
    assert stats["rows"] == 0
    assert stats["oldest_ts"] is None
    assert stats["newest_ts"] is None
    assert stats["bytes"] > 0  # the DuckDB file itself exists on disk


def test_stats_reports_row_count_and_bounds(tmp_path):
    sp = _spool(tmp_path)
    sp.append(_sample(3, day=2, hour=8))
    stats = sp.stats()
    assert stats["rows"] == 3
    assert stats["oldest_ts"] == dt.datetime(2025, 1, 2, 8, 0, 0)
    assert stats["newest_ts"] == dt.datetime(2025, 1, 2, 8, 0, 30)


# --- flush_to_staging ----------------------------------------------------


def test_flush_writes_one_object_per_day_not_per_route(tmp_path):
    sp = _spool(tmp_path)
    sp.append(_sample(3, day=2, route_id="Green-D", vehicle_prefix="v"))
    sp.append(_sample(3, day=2, route_id="Green-E", vehicle_prefix="w"))
    base_uri = str(tmp_path / "staging")
    cutoff = dt.datetime(2025, 1, 3, tzinfo=dt.timezone.utc)

    result = flush_to_staging(sp, base_uri, cutoff)

    assert result["flushed"] == 6
    assert result["days"] == ["2025-01-02"]

    day_dir = tmp_path / "staging" / "year=2025" / "month=1" / "day=2"
    assert day_dir.is_dir()
    parquet_files = list(day_dir.glob("*.parquet"))
    assert len(parquet_files) >= 1
    # The whole point of the flush: no route_id= segment anywhere under the
    # staging base -- that per-route fan-out is the bug this spool fixes.
    all_paths = [str(p) for p in (tmp_path / "staging").rglob("*")]
    assert not any("route_id=" in p for p in all_paths)

    # Spool drained.
    assert sp.stats()["rows"] == 0


def test_flush_straddling_midnight_utc_produces_two_day_partitions(tmp_path):
    sp = _spool(tmp_path)
    sp.append(_sample(1, day=2, hour=23, vehicle_prefix="late"))  # 2025-01-02 23:00 UTC
    sp.append(_sample(1, day=3, hour=0, vehicle_prefix="early"))  # 2025-01-03 00:00 UTC
    base_uri = str(tmp_path / "staging")
    cutoff = dt.datetime(2025, 1, 4, tzinfo=dt.timezone.utc)

    result = flush_to_staging(sp, base_uri, cutoff)

    assert result["flushed"] == 2
    assert sorted(result["days"]) == ["2025-01-02", "2025-01-03"]
    assert (tmp_path / "staging" / "year=2025" / "month=1" / "day=2").is_dir()
    assert (tmp_path / "staging" / "year=2025" / "month=1" / "day=3").is_dir()


def test_flush_empty_spool_is_a_noop(tmp_path):
    sp = _spool(tmp_path)
    base_uri = str(tmp_path / "staging")
    result = flush_to_staging(sp, base_uri, dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc))
    assert result == {"flushed": 0, "days": []}
    assert not (tmp_path / "staging").exists()


def test_flush_leaves_spool_intact_when_write_raises(tmp_path, monkeypatch):
    sp = _spool(tmp_path)
    sp.append(_sample(2, day=2))
    base_uri = str(tmp_path / "staging")
    cutoff = dt.datetime(2025, 1, 3, tzinfo=dt.timezone.utc)

    class _FailingConnection:
        def register(self, name, df):
            pass

        def unregister(self, name):
            pass

        def execute(self, sql, *args, **kwargs):
            if "COPY" in sql:
                raise RuntimeError("simulated S3 PUT failure")
            return self

        def fetchone(self):
            return (0,)

        def close(self):
            pass

    monkeypatch.setattr(
        "rt_pipeline.storage.spool.connect", lambda base_uri, config=None: _FailingConnection()
    )

    with pytest.raises(RuntimeError, match="simulated S3 PUT failure"):
        flush_to_staging(sp, base_uri, cutoff)

    # The write failed -> nothing should have been deleted from the spool.
    assert sp.stats()["rows"] == 2


def test_flush_verify_mismatch_raises_and_preserves_spool(tmp_path, monkeypatch):
    """If the written row count doesn't match what we meant to write, treat
    it like a failed write: raise, and never delete."""
    sp = _spool(tmp_path)
    sp.append(_sample(2, day=2))
    base_uri = str(tmp_path / "staging")
    cutoff = dt.datetime(2025, 1, 3, tzinfo=dt.timezone.utc)

    import rt_pipeline.storage.spool as spool_mod

    real_connect = spool_mod.connect

    class _UnderReportingConnection:
        def __init__(self, real_con):
            self._real = real_con

        def register(self, name, df):
            self._real.register(name, df)

        def unregister(self, name):
            self._real.unregister(name)

        def execute(self, sql, *args, **kwargs):
            self._real.execute(sql, *args, **kwargs)
            if "parquet_file_metadata" in sql:
                # Lie about the verified row count.
                class _Row:
                    def fetchone(self_inner):
                        return (0,)

                return _Row()
            return self

        def fetchone(self):
            return (0,)

        def close(self):
            self._real.close()

    def fake_connect(uri, config=None):
        return _UnderReportingConnection(real_connect(uri, config))

    monkeypatch.setattr(spool_mod, "connect", fake_connect)

    with pytest.raises(RuntimeError, match="flush verify mismatch"):
        flush_to_staging(sp, base_uri, cutoff)

    assert sp.stats()["rows"] == 2
