from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

import psycopg
from psycopg.rows import dict_row


def _ensure_aware(ts: datetime) -> datetime:
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        # Assume UTC if naive
        return ts.replace(tzinfo=timezone.utc)
    return ts


def calculate_headway(conn: psycopg.Connection, route_id: str, timestamp: datetime, vehicle_id: str) -> float:
    """
    Time (seconds) since the last VehiclePosition on the same route (from any *other* vehicle)
    before `timestamp`.

    If none found, returns float("inf").
    """
    ts = _ensure_aware(timestamp)

    sql = """
        SELECT EXTRACT(EPOCH FROM (%(ts)s - MAX(vp.ts))) AS headway_s
        FROM rt_pipeline_vehicleposition AS vp
        WHERE vp.route_id = %(route_id)s
          AND vp.vehicle_id <> %(vehicle_id)s
          AND vp.ts < %(ts)s
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"ts": ts, "route_id": route_id, "vehicle_id": vehicle_id})
        row = cur.fetchone()
        headway_s = row and row.get("headway_s")
        return float(headway_s) if headway_s is not None else float("inf")


def detect_congestion_proxy(
    conn: psycopg.Connection,
    route_id: str,
    stop_sequence: int,
    timestamp: datetime,
    *,
    speed_window_minutes: int = 10,
    vehicles_window_minutes: int = 10,
    delay_trend_window_minutes: int = 30,
    slope_threshold_sec_per_min: float = 5.0,
) -> Dict[str, Any]:
    """
    Heuristic congestion proxy using recent speed, vehicle concurrency, and delay trend.

    Returns:
        {
          "avg_speed_last_10min": float | None,   # km/h
          "vehicles_on_route": int,
          "delay_trend": "increasing" | "stable" | "decreasing"
        }
    """
    ts = _ensure_aware(timestamp)
    t0_speed = ts - timedelta(minutes=speed_window_minutes)
    t0_veh   = ts - timedelta(minutes=vehicles_window_minutes)
    t0_trend = ts - timedelta(minutes=delay_trend_window_minutes)

    # 1) Avg speed on route over the last N minutes (km/h)
    sql_speed = """
        SELECT AVG(vp.speed) AS avg_speed_mps
        FROM rt_pipeline_vehicleposition AS vp
        WHERE vp.route_id = %(route_id)s
          AND vp.ts >= %(t0)s AND vp.ts < %(t1)s
          AND vp.speed IS NOT NULL
    """
    # 2) Concurrent vehicles on route over the last M minutes
    sql_veh = """
        SELECT COUNT(DISTINCT vp.vehicle_id) AS n
        FROM rt_pipeline_vehicleposition AS vp
        WHERE vp.route_id = %(route_id)s
          AND vp.ts >= %(t0)s AND vp.ts < %(t1)s
    """
    # 3) Delay trend at a given stop_sequence over the last K minutes
    #    We take whichever of arrival_delay / departure_delay is present.
    sql_delay = """
        SELECT
          EXTRACT(EPOCH FROM tu.ts) AS t_epoch,
          COALESCE(tu.arrival_delay, tu.departure_delay) AS delay_s
        FROM rt_pipeline_tripupdate AS tu
        WHERE tu.route_id = %(route_id)s
          AND tu.stop_sequence = %(stop_sequence)s
          AND tu.ts >= %(t0)s AND tu.ts < %(t1)s
          AND (tu.arrival_delay IS NOT NULL OR tu.departure_delay IS NOT NULL)
        ORDER BY tu.ts
    """

    with conn.cursor(row_factory=dict_row) as cur:
        # Speed
        cur.execute(sql_speed, {"route_id": route_id, "t0": t0_speed, "t1": ts})
        row = cur.fetchone()
        avg_speed_mps = row and row.get("avg_speed_mps")
        avg_speed_kmh = float(avg_speed_mps) * 3.6 if avg_speed_mps is not None else None

        # Vehicles
        cur.execute(sql_veh, {"route_id": route_id, "t0": t0_veh, "t1": ts})
        row = cur.fetchone()
        vehicles_on_route = int(row["n"]) if row and row.get("n") is not None else 0

        # Delay trend
        cur.execute(sql_delay, {
            "route_id": route_id,
            "stop_sequence": int(stop_sequence),
            "t0": t0_trend,
            "t1": ts
        })
        samples = cur.fetchall()

    # Compute slope (simple least squares) of delay vs time (minutes).
    def _delay_trend_label(samples_rows) -> str:
        # Need at least 3 points for a meaningful slope
        if not samples_rows or len(samples_rows) < 3:
            return "stable"
        xs = [(r["t_epoch"] - samples_rows[0]["t_epoch"]) / 60.0 for r in samples_rows]  # minutes since first
        ys = [float(r["delay_s"]) for r in samples_rows]

        n = float(len(xs))
        sum_x = sum(xs)
        sum_y = sum(ys)
        sum_xx = sum(x*x for x in xs)
        sum_xy = sum(x*y for x, y in zip(xs, ys))
        denom = (n * sum_xx - sum_x * sum_x)
        if denom == 0.0:
            return "stable"
        slope = (n * sum_xy - sum_x * sum_y) / denom  # units: seconds of delay per minute

        if slope > slope_threshold_sec_per_min:
            return "increasing"
        if slope < -slope_threshold_sec_per_min:
            return "decreasing"
        return "stable"

    delay_trend = _delay_trend_label(samples)

    return {
        "avg_speed_last_10min": avg_speed_kmh,
        "vehicles_on_route": vehicles_on_route,
        "delay_trend": delay_trend,
    }
