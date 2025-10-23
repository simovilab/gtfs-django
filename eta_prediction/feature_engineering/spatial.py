# feature_engineering/spatial.py
from __future__ import annotations
import math
import hashlib
from typing import Dict, Iterable, Optional, Tuple

EARTH_RADIUS_M = 6_371_000.0

# --------- small helpers ---------
def _deg2rad(x: float) -> float:
    return x * math.pi / 180.0

def _rad2deg(x: float) -> float:
    return x * 180.0 / math.pi

def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters."""
    φ1, φ2 = _deg2rad(lat1), _deg2rad(lat2)
    dφ = φ2 - φ1
    dλ = _deg2rad(lon2 - lon1)
    a = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_M * c

def _initial_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Forward azimuth from point1 to point2 (0..360)."""
    φ1, φ2 = _deg2rad(lat1), _deg2rad(lat2)
    dλ = _deg2rad(lon2 - lon1)
    y = math.sin(dλ) * math.cos(φ2)
    x = math.cos(φ1) * math.sin(φ2) - math.sin(φ1) * math.cos(φ2) * math.cos(dλ)
    θ = math.atan2(y, x)
    brg = (_rad2deg(θ) + 360.0) % 360.0
    return brg

def _angle_diff_deg(a: float, b: float) -> float:
    """Smallest absolute difference between two headings (0..180)."""
    d = abs((a - b + 180.0) % 360.0 - 180.0)
    return d

def _clamp(x: float, a: float, b: float) -> float:
    return max(a, min(b, x))

def _segment_id(stop_id: str, next_stop_id: str) -> str:
    """Stable compact id for (stop_id, next_stop_id)."""
    return hashlib.sha1(f"{stop_id}|{next_stop_id}".encode("utf-8")).hexdigest()[:12]

# --------- public API ---------
def calculate_distance_features(
    vehicle_position: Dict, stop: Dict, next_stop: Optional[Dict]
) -> Dict:
    """
    Args (minimal expected keys):
        vehicle_position: {'lat': float, 'lon': float, 'bearing': Optional[float]}
        stop:             {'stop_id': str, 'lat': float, 'lon': float}
        next_stop:        {'stop_id': str, 'lat': float, 'lon': float} or None

    Returns:
        {
          'distance_to_stop': meters,
          'distance_to_next_stop': meters or None,
          'bearing_to_stop': degrees 0..360,
          'is_approaching': bool,
          'segment_id': str or None,
          'progress_on_segment': float in [0,1] or None
        }
    """
    vlat, vlon = float(vehicle_position["lat"]), float(vehicle_position["lon"])
    vbearing = vehicle_position.get("bearing")  # degrees or None

    slat, slon = float(stop["lat"]), float(stop["lon"])
    d_to_stop = _haversine_m(vlat, vlon, slat, slon)
    brg_to_stop = _initial_bearing_deg(vlat, vlon, slat, slon)

    # Approaching: if vehicle heading (if present) is roughly aligned with the direction to the stop
    # Threshold = 35° is a good default; tweak per your data.
    is_approaching = False
    if vbearing is not None:
        is_approaching = _angle_diff_deg(vbearing, brg_to_stop) <= 35.0

    # Defaults for next stop / segment progress
    dist_to_next = None
    seg_id = None
    progress = None

    if next_stop is not None:
        nlat, nlon = float(next_stop["lat"]), float(next_stop["lon"])
        dist_to_next = _haversine_m(vlat, vlon, nlat, nlon)
        seg_id = _segment_id(str(stop["stop_id"]), str(next_stop["stop_id"]))

        # Segment length (stop -> next_stop)
        seg_len = _haversine_m(slat, slon, nlat, nlon)

        # A robust, simple proxy for along-segment progress when you don't have shape polylines:
        # assume vehicle is moving from stop -> next_stop; progress ~ 1 - (dist to next_stop / segment_len)
        # This behaves well except for large detours; clamp to [0,1].
        if seg_len > 0:
            proxy = 1.0 - (dist_to_next / seg_len)
            # If the bus hasn’t departed the current stop yet, we might get negative values; clamp.
            progress = _clamp(proxy, 0.0, 1.0)
        else:
            progress = 0.0

        # Optional refinement: if vehicle is clearly closer to the next_stop than to stop, bias toward >0.5
        # (Keeps progress monotone as it passes the midpoint)
        # Uncomment if you find it helpful:
        # if dist_to_next < d_to_stop:
        #     progress = max(progress, 0.5)

    return {
        "distance_to_stop": d_to_stop,
        "distance_to_next_stop": dist_to_next,
        "bearing_to_stop": brg_to_stop,
        "is_approaching": is_approaching,
        "segment_id": seg_id,
        "progress_on_segment": progress,
    }


def get_route_features(
    route_id: str,
    *,
    conn=None,
    stops_in_order: Optional[Iterable[Dict[str, float]]] = None,
) -> Dict:
    """
    Compute per-route geometry features.

    Two ways to use:
      1) Pass a DB connection (psycopg2 / asyncpg) via `conn` to fetch ordered stops from GTFS tables.
      2) Or pass `stops_in_order` as an iterable of dicts:
         [{'stop_id': 'A', 'lat': ..., 'lon': ...}, ..., {'stop_id':'Z', ...}]

    Returns:
        - total_stops: int
        - route_length_km: float
        - avg_stop_spacing: meters (mean of consecutive inter-stop distances; 0 if <2 stops)
    """
    if stops_in_order is None and conn is None:
        raise ValueError("Provide either `conn` or `stops_in_order`.")

    if stops_in_order is None:
        # --- Postgres path (expects your schema naming) ---
        # Adjust table/column names if needed:
        # sch_pipeline_routestop(route_id, stop_id, stop_sequence)
        # sch_pipeline_stop(stop_id, stop_lat, stop_lon)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rs.stop_id, s.stop_lat, s.stop_lon
                FROM sch_pipeline_routestop rs
                JOIN sch_pipeline_stop s ON s.stop_id = rs.stop_id
                WHERE rs.route_id = %s
                ORDER BY rs.stop_sequence
                """,
                (route_id,),
            )
            rows = cur.fetchall()
            stops = [{"stop_id": r[0], "lat": float(r[1]), "lon": float(r[2])} for r in rows]
    else:
        stops = list(stops_in_order)

    total_stops = len(stops)
    if total_stops < 2:
        return {"total_stops": total_stops, "route_length_km": 0.0, "avg_stop_spacing": 0.0}

    # Pairwise distances along the ordered stop list
    dists = []
    for a, b in zip(stops, stops[1:]):
        d = _haversine_m(a["lat"], a["lon"], b["lat"], b["lon"])
        dists.append(d)

    route_length_m = sum(dists)
    avg_spacing_m = sum(dists) / len(dists) if dists else 0.0

    return {
        "total_stops": total_stops,
        "route_length_km": route_length_m / 1000.0,
        "avg_stop_spacing": avg_spacing_m,
    }

# --------- optional: PostGIS exact distances (if you want to use it) ---------
def distance_postgis_m(conn, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Uses ST_DistanceSphere for high-precision spherical distance, if a DB call is acceptable.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ST_DistanceSphere(
                ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            )
            """,
            (lon1, lat1, lon2, lat2),
        )
        return float(cur.fetchone()[0])
