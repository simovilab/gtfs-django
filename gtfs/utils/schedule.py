# gtfs/utils/schedule.py
import csv
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime
from django.db import transaction, IntegrityError
from django.core.management.base import CommandError

from gtfs.models_schedule import (
    AgencySchedule,
    RouteSchedule,
    StopSchedule,
    TripSchedule,
    CalendarSchedule,
    CalendarDateSchedule,
    ShapeSchedule,
    StopTimeSchedule,
    FeedInfoSchedule,
)


# ---------------------------
# Helpers de parseo y casting
# ---------------------------
def _read_csv_rows(root: Path, filename: str):
    path = root / filename
    if not path.exists():
        return None
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _as_int(v, default=None):
    if v in (None, "", "NULL", "null"):
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _as_float(v, default=None):
    if v in (None, "", "NULL", "null"):
        return default
    try:
        return float(v)
    except ValueError:
        return default


def _as_date(v, default=None):
    if v in (None, "", "NULL", "null"):
        return default
    # GTFS usa YYYYMMDD en muchos archivos; en otros puedes tener YYYY-MM-DD
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            pass
    return default


def _as_str(v, default=""):
    return default if v in (None, "NULL", "null") else str(v)


def _bool01(v):
    """Devuelve 0/1 desde '0'/'1' o vacíos (default 0)."""
    return 1 if str(v).strip() == "1" else 0


# ---------------------------
# Importador principal
# ---------------------------
def import_gtfs(feed_path: str | Path):
    """
    Importa un GTFS Schedule completo (ZIP o carpeta) a la BD.

    Soporta: agency, routes, stops, trips, calendar, calendar_dates,
             shapes, stop_times, feed_info.

    - Valida FKs y tipos básicos.
    - Reporta errores con contexto (archivo + línea).
    - Ejecuta todo en una transacción atómica.
    """
    feed_path = Path(feed_path)
    if not feed_path.exists():
        raise FileNotFoundError(f"Feed not found: {feed_path}")

    # Detectar ZIP o carpeta
    if feed_path.suffix.lower() == ".zip":
        tmpdir = tempfile.TemporaryDirectory()
        with zipfile.ZipFile(feed_path, "r") as zf:
            zf.extractall(tmpdir.name)
        root = Path(tmpdir.name)
    else:
        root = feed_path

    # Cargar todos los CSV (si existen)
    files = {
        "agency": "agency.txt",
        "routes": "routes.txt",
        "stops": "stops.txt",
        "trips": "trips.txt",
        "calendar": "calendar.txt",
        "calendar_dates": "calendar_dates.txt",
        "stop_times": "stop_times.txt",
        "shapes": "shapes.txt",
        "feed_info": "feed_info.txt",
    }
    rows = {key: _read_csv_rows(root, fname) for key, fname in files.items()}

    # Requeridos mínimos por spec (calendar o calendar_dates, al menos uno):
    for req in ("agency", "routes", "stops", "trips"):
        if rows[req] is None or len(rows[req]) == 0:
            raise CommandError(f"Missing required GTFS file or empty: {files[req]}")
    if (rows["calendar"] is None or len(rows["calendar"]) == 0) and (
        rows["calendar_dates"] is None or len(rows["calendar_dates"]) == 0
    ):
        raise CommandError(
            "Missing service definition: provide calendar.txt and/or calendar_dates.txt"
        )

    print("Extracted feed contents:")
    for k, fname in files.items():
        if rows[k] is not None:
            print(f"  - {fname}")

    # Import atómico
    try:
        with transaction.atomic():

            # ------------------
            # 1) Agency
            # ------------------
            for i, r in enumerate(rows["agency"], start=2):
                try:
                    AgencySchedule.objects.update_or_create(
                        agency_id=_as_str(r.get("agency_id")) or "A1",
                        defaults={
                            "agency_name": _as_str(r.get("agency_name")),
                            "agency_url": _as_str(r.get("agency_url")),
                            "agency_timezone": _as_str(r.get("agency_timezone")),
                            "agency_phone": _as_str(r.get("agency_phone"), None),
                            "agency_email": _as_str(r.get("agency_email"), None),
                        },
                    )
                except Exception as e:
                    raise CommandError(f"In agency.txt line {i}: {e}")

            # ------------------
            # 2) Calendar
            # ------------------
            if rows["calendar"]:
                for i, r in enumerate(rows["calendar"], start=2):
                    try:
                        CalendarSchedule.objects.update_or_create(
                            service_id=_as_str(r.get("service_id")),
                            defaults={
                                "monday": _bool01(r.get("monday")),
                                "tuesday": _bool01(r.get("tuesday")),
                                "wednesday": _bool01(r.get("wednesday")),
                                "thursday": _bool01(r.get("thursday")),
                                "friday": _bool01(r.get("friday")),
                                "saturday": _bool01(r.get("saturday")),
                                "sunday": _bool01(r.get("sunday")),
                                "start_date": _as_date(r.get("start_date")),
                                "end_date": _as_date(r.get("end_date")),
                            },
                        )
                    except Exception as e:
                        raise CommandError(f"In calendar.txt line {i}: {e}")

            # ------------------
            # 3) Routes
            # ------------------
            for i, r in enumerate(rows["routes"], start=2):
                try:
                    agency_id = _as_str(r.get("agency_id")) or "A1"
                    if not AgencySchedule.objects.filter(pk=agency_id).exists():
                        raise CommandError(
                            f"In routes.txt line {i}: agency_id '{agency_id}' not found in agency.txt"
                        )
                    RouteSchedule.objects.update_or_create(
                        route_id=_as_str(r.get("route_id")),
                        defaults={
                            "agency_id": agency_id,
                            "route_short_name": _as_str(r.get("route_short_name")),
                            "route_long_name": _as_str(r.get("route_long_name")),
                            "route_desc": _as_str(r.get("route_desc"), None),
                            "route_type": _as_int(r.get("route_type"), 3),
                            "route_color": _as_str(r.get("route_color"), None),
                            "route_text_color": _as_str(r.get("route_text_color"), None),
                        },
                    )
                except CommandError:
                    raise
                except Exception as e:
                    raise CommandError(f"In routes.txt line {i}: {e}")

            # ------------------
            # 4) Stops
            # ------------------
            for i, r in enumerate(rows["stops"], start=2):
                try:
                    parent_station = _as_str(r.get("parent_station"), None)
                    if parent_station and not StopSchedule.objects.filter(
                        pk=parent_station
                    ).exists():
                        # No creamos parent implícito; forzamos error contextual
                        raise CommandError(
                            f"In stops.txt line {i}: parent_station '{parent_station}' not found"
                        )
                    StopSchedule.objects.update_or_create(
                        stop_id=_as_str(r.get("stop_id")),
                        defaults={
                            "stop_code": _as_str(r.get("stop_code"), None),
                            "stop_name": _as_str(r.get("stop_name")),
                            "stop_desc": _as_str(r.get("stop_desc"), None),
                            "stop_lat": _as_float(r.get("stop_lat"), 0.0),
                            "stop_lon": _as_float(r.get("stop_lon"), 0.0),
                            "zone_id": _as_str(r.get("zone_id"), None),
                            "location_type": _as_int(r.get("location_type"), 0),
                            "parent_station": StopSchedule.objects.get(pk=parent_station)
                            if parent_station
                            else None,
                            "stop_timezone": _as_str(r.get("stop_timezone"), None),
                            "wheelchair_boarding": _as_int(
                                r.get("wheelchair_boarding"), 0
                            ),
                        },
                    )
                except CommandError:
                    raise
                except Exception as e:
                    raise CommandError(f"In stops.txt line {i}: {e}")

            # ------------------
            # 5) Trips  (auto-crea servicio si no vino calendar)
            # ------------------
            for i, r in enumerate(rows["trips"], start=2):
                try:
                    route_id = _as_str(r.get("route_id"))
                    if not RouteSchedule.objects.filter(pk=route_id).exists():
                        raise CommandError(
                            f"In trips.txt line {i}: route_id '{route_id}' not found in routes.txt"
                        )
                    service_id = _as_str(r.get("service_id"))
                    if not CalendarSchedule.objects.filter(
                        pk=service_id
                    ).exists():
                        # Si no hay calendar pero hay calendar_dates, podemos crear un servicio dummy
                        # para no romper FK (aceptado por varias implementaciones GTFS).
                        CalendarSchedule.objects.update_or_create(
                            service_id=service_id,
                            defaults={
                                "monday": 0,
                                "tuesday": 0,
                                "wednesday": 0,
                                "thursday": 0,
                                "friday": 0,
                                "saturday": 0,
                                "sunday": 0,
                                "start_date": _as_date("2000-01-01"),
                                "end_date": _as_date("2099-12-31"),
                            },
                        )
                    TripSchedule.objects.update_or_create(
                        trip_id=_as_str(r.get("trip_id")),
                        defaults={
                            "route_id": route_id,
                            "service_id": service_id,
                            "trip_headsign": _as_str(r.get("trip_headsign"), None),
                            "trip_short_name": _as_str(r.get("trip_short_name"), None),
                            "direction_id": _as_int(r.get("direction_id"), 0),
                            "block_id": _as_str(r.get("block_id"), None),
                            # Nota: tu modelo TripSchedule.shape es FK a ShapeSchedule,
                            # pero ShapeSchedule no tiene PK=shape_id; por lo tanto lo dejamos en None.
                            "shape": None,
                            "wheelchair_accessible": _as_int(
                                r.get("wheelchair_accessible"), 0
                            ),
                        },
                    )
                except CommandError:
                    raise
                except Exception as e:
                    raise CommandError(f"In trips.txt line {i}: {e}")

            # ------------------
            # 6) Stop Times (opcional pero común)
            # ------------------
            if rows["stop_times"]:
                for i, r in enumerate(rows["stop_times"], start=2):
                    try:
                        trip_id = _as_str(r.get("trip_id"))
                        stop_id = _as_str(r.get("stop_id"))
                        if not TripSchedule.objects.filter(pk=trip_id).exists():
                            raise CommandError(
                                f"In stop_times.txt line {i}: trip_id '{trip_id}' not found in trips.txt"
                            )
                        if not StopSchedule.objects.filter(pk=stop_id).exists():
                            raise CommandError(
                                f"In stop_times.txt line {i}: stop_id '{stop_id}' not found in stops.txt"
                            )
                        StopTimeSchedule.objects.update_or_create(
                            trip_id=trip_id,
                            stop_sequence=_as_int(r.get("stop_sequence"), 0),
                            defaults={
                                "stop_id": stop_id,
                                "arrival_time": _as_str(r.get("arrival_time")),
                                "departure_time": _as_str(r.get("departure_time")),
                                "stop_headsign": _as_str(r.get("stop_headsign"), None),
                                "pickup_type": _as_int(r.get("pickup_type"), 0),
                                "drop_off_type": _as_int(r.get("drop_off_type"), 0),
                                "shape_dist_traveled": _as_float(
                                    r.get("shape_dist_traveled"), None
                                ),
                                "timepoint": _as_int(r.get("timepoint"), 0),
                            },
                        )
                    except CommandError:
                        raise
                    except Exception as e:
                        raise CommandError(f"In stop_times.txt line {i}: {e}")

            # ------------------
            # 7) Shapes (opcional)
            # ------------------
            if rows["shapes"]:
                for i, r in enumerate(rows["shapes"], start=2):
                    try:
                        # Unique together: (shape_id, shape_pt_sequence)
                        ShapeSchedule.objects.update_or_create(
                            shape_id=_as_str(r.get("shape_id")),
                            shape_pt_sequence=_as_int(r.get("shape_pt_sequence"), 0),
                            defaults={
                                "shape_pt_lat": _as_float(r.get("shape_pt_lat"), 0.0),
                                "shape_pt_lon": _as_float(r.get("shape_pt_lon"), 0.0),
                                "shape_dist_traveled": _as_float(
                                    r.get("shape_dist_traveled"), None
                                ),
                            },
                        )
                    except Exception as e:
                        raise CommandError(f"In shapes.txt line {i}: {e}")

            # ------------------
            # 8) Calendar Dates (opcional)
            # ------------------
            if rows["calendar_dates"]:
                for i, r in enumerate(rows["calendar_dates"], start=2):
                    try:
                        service_id = _as_str(r.get("service_id"))
                        if not CalendarSchedule.objects.filter(
                            pk=service_id
                        ).exists():
                            # Si no existe aún (por trips o calendar), créalo dummy.
                            CalendarSchedule.objects.update_or_create(
                                service_id=service_id,
                                defaults={
                                    "monday": 0,
                                    "tuesday": 0,
                                    "wednesday": 0,
                                    "thursday": 0,
                                    "friday": 0,
                                    "saturday": 0,
                                    "sunday": 0,
                                    "start_date": _as_date("2000-01-01"),
                                    "end_date": _as_date("2099-12-31"),
                                },
                            )
                        CalendarDateSchedule.objects.update_or_create(
                            service_id=service_id,
                            date=_as_date(r.get("date")),
                            defaults={
                                "exception_type": _as_int(r.get("exception_type"), 1)
                            },
                        )
                    except Exception as e:
                        raise CommandError(f"In calendar_dates.txt line {i}: {e}")

            # ------------------
            # 9) Feed Info (opcional)
            # ------------------
            if rows["feed_info"]:
                for i, r in enumerate(rows["feed_info"], start=2):
                    try:
                        FeedInfoSchedule.objects.update_or_create(
                            feed_publisher_name=_as_str(r.get("feed_publisher_name")),
                            feed_publisher_url=_as_str(r.get("feed_publisher_url")),
                            feed_lang=_as_str(r.get("feed_lang")),
                            defaults={
                                "feed_version": _as_str(r.get("feed_version")),
                                "feed_start_date": _as_date(r.get("feed_start_date")),
                                "feed_end_date": _as_date(r.get("feed_end_date")),
                                "feed_contact_email": _as_str(
                                    r.get("feed_contact_email"), None
                                ),
                                "feed_contact_url": _as_str(
                                    r.get("feed_contact_url"), None
                                ),
                            },
                        )
                    except Exception as e:
                        raise CommandError(f"In feed_info.txt line {i}: {e}")

        # fin transaction
    except IntegrityError as e:
        raise CommandError(f"Database integrity error: {e}")

    print("Import completed successfully.")
