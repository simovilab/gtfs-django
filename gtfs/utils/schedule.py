
# gtfs/schedule.py

from __future__ import annotations

import csv
import io
import zipfile
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union

from django.db import transaction

# Importa tus modelos reales (ajusta el import según tu estructura)
try:
    from gtfs.models import (
        Agency, Route, Trip, Stop, StopTime, Calendar, CalendarDate, Shape, FeedInfo
    )
except Exception:  # pragma: no cover - permite que el archivo importe aunque aún no existan los modelos
    Agency = Route = Trip = Stop = StopTime = Calendar = CalendarDate = Shape = FeedInfo = object  # type: ignore

logger = logging.getLogger(__name__)


# ----------------------------
# Dataclasses de resultados
# ----------------------------

@dataclass
class ImportResult:
    """Resumen de la importación."""
    zip_path: Path
    inserted: Dict[str, int] = field(default_factory=dict)   # tabla -> filas insertadas
    updated: Dict[str, int] = field(default_factory=dict)    # tabla -> filas actualizadas (si implementas upsert)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class ExportResult:
    """Resumen de la exportación."""
    output_zip: Path
    files_written: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class ValidationIssue:
    level: str  # "ERROR" | "WARNING"
    file: str
    rownum: Optional[int]
    field: Optional[str]
    message: str


@dataclass
class ValidationReport:
    source: str
    from_zip: bool
    issues: List[ValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.level == "ERROR"]

    @property
    def warnings(self) -> List[ValidationIssue]:
        return [i for i in self.issues if i.level == "WARNING"]


# ----------------------------
# Constantes y utilidades
# ----------------------------

# Orden recomendado de procesamiento (respeta dependencias FK)
GTFS_LOAD_ORDER = [
    "agency",
    "routes",
    "stops",
    "calendar",
    "calendar_dates",
    "shapes",
    "trips",
    "stop_times",
    "feed_info",
]

# Mapeo filename -> nombre de modelo Django (ajústalo a tus modelos reales)
MODEL_MAP = {
    "agency": Agency,
    "routes": Route,
    "stops": Stop,
    "calendar": Calendar,
    "calendar_dates": CalendarDate,
    "shapes": Shape,
    "trips": Trip,
    "stop_times": StopTime,
    "feed_info": FeedInfo,
}

REQUIRED_FILES = {"agency", "routes", "trips", "stops", "stop_times", "calendar"}

CSV_DIALECT = {
    "delimiter": ",",
    "quotechar": '"',
    "lineterminator": "\n",
}

def _open_zip(zip_path: Union[str, Path]) -> zipfile.ZipFile:
    """Abre un ZIP en modo lectura con manejo de errores básico."""
    zpath = Path(zip_path)
    if not zpath.exists():
        raise FileNotFoundError(f"No existe el archivo: {zpath}")
    return zipfile.ZipFile(zpath, "r")


def _read_csv_from_zip(zf: zipfile.ZipFile, name: str) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Lee un CSV (TXT GTFS) del ZIP y devuelve (headers, rows).
    No convierte tipos; deja todo como string para validación posterior.
    """
    fname = f"{name}.txt"
    if fname not in zf.namelist():
        return [], []  # el validador/llamador decidirá si esto es error o no

    with zf.open(fname, "r") as fp:
        # GTFS usa UTF-8 sin BOM por convención
        text = io.TextIOWrapper(fp, encoding="utf-8")
        reader = csv.DictReader(text)
        headers = reader.fieldnames or []
        rows = list(reader)
        return headers, rows


def _write_csv_to_zip(zf: zipfile.ZipFile, name: str, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    """Escribe un CSV en el ZIP con los headers dados."""
    data = io.StringIO()
    writer = csv.DictWriter(data, fieldnames=headers, **CSV_DIALECT)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    zf.writestr(f"{name}.txt", data.getvalue())



# Funciones gtfs Schedule


def import_gtfs_schedule(
    zip_path: Union[str, Path],
    *,
    schema: Optional[Dict[str, Any]] = None,
    strict: bool = True,
    replace: bool = False,
) -> ImportResult:
    """
    Importa un feed GTFS Schedule (ZIP -> BD Django).

    Parámetros
    ----------
    zip_path : str | Path
        Ruta al archivo .zip con los .txt GTFS.
    schema : dict | None
        Esquema machine-readable (tu JSON/YAML parseado). Si se provee, se usa para validar campos/tipos.
    strict : bool
        Si True, cualquier error de validación aborta la importación.
    replace : bool
        Si True, limpia tablas antes de cargar (truncate); si False, intenta inserciones (y opcionalmente upsert).

    Retorna
    -------
    ImportResult
        Estadísticas y/o errores de la importación.

    Flujo (a implementar con TODO)
    ------------------------------
    1) Abrir el ZIP y verificar archivos requeridos (REQUIRED_FILES).
    2) Validar contra `schema` (si se proporciona): campos requeridos, tipos, enums.
    3) transaction.atomic(): insertar por orden GTFS_LOAD_ORDER.
       - Opcional: bulk_create para rendimiento.
       - Manejar PK compuestas con UniqueConstraint si aplica.
    4) Registrar counts por tabla y cualquier warning/error.
    """
    result = ImportResult(zip_path=Path(zip_path))
    # TODO: abrir y validar archivos presentes vs REQUIRED_FILES
    try:
        with _open_zip(zip_path) as zf:
            available = {n.replace(".txt", "") for n in zf.namelist() if n.endswith(".txt")}
            missing = REQUIRED_FILES - available
            if missing:
                msg = f"Faltan archivos requeridos: {sorted(missing)}"
                result.errors.append(msg)
                if strict:
                    return result
                logger.warning(msg)
            # TODO: validación esquemática (si schema no es None)
            if replace:
                # TODO: borrar datos existentes (con cuidado con orden por FKs)
                pass

            with transaction.atomic():
                for table in GTFS_LOAD_ORDER:
                    if f"{table}.txt" not in zf.namelist():
                        continue  # puede ser opcional
                    headers, rows = _read_csv_from_zip(zf, table)
                    # TODO: mapear filas -> instancias de MODEL_MAP[table]
                    # TODO: convertir tipos (int/float/date/time) según schema
                    # TODO: bulk_create y actualizar result.inserted[table]
                    pass

    except Exception as exc:  # captura global para devolver en result
        logger.exception("Error importando GTFS")
        result.errors.append(str(exc))
    return result


def export_gtfs_schedule(
    output_zip_path: Union[str, Path],
    *,
    include_optional: bool = True,
    schema: Optional[Dict[str, Any]] = None,
) -> ExportResult:
    """
    Exporta la BD Django a un feed GTFS Schedule (BD -> ZIP).

    Parámetros
    ----------
    output_zip_path : str | Path
        Ruta destino para el archivo .zip a generar.
    include_optional : bool
        Si True, incluye tablas opcionales si existen filas (p.ej. shapes, feed_info).
    schema : dict | None
        Si se provee, se usa para ordenar columnas según el schema y asegurar compatibilidad.

    Retorna
    -------
    ExportResult
        Resumen de archivos escritos y posibles advertencias/errores.

    Flujo (a implementar con TODO)
    ------------------------------
    1) Consultar cada modelo en orden y construir listas de dicts (rows).
    2) Ordenar columnas según `schema` si está disponible.
    3) Escribir cada .txt en el ZIP con _write_csv_to_zip.
    """
    output_path = Path(output_zip_path)
    result = ExportResult(output_zip=output_path)

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            # Ejemplo de patrón por tabla (rellenar en TODO):
            for table in GTFS_LOAD_ORDER:
                Model = MODEL_MAP.get(table)
                if Model is object:
                    continue  # modelos aún no definidos en el esqueleto

                # TODO: si include_optional es False, saltar opcionales sin filas
                # TODO: query = Model.objects.all()
                # TODO: construir headers y rows según schema o metadatos del modelo
                # headers = [...]
                # rows = [ {...}, {...} ]
                # if not rows: continue
                # _write_csv_to_zip(zf, table, headers, rows)
                # result.files_written.append(f"{table}.txt")
                pass

    except Exception as exc:
        logger.exception("Error exportando GTFS")
        result.errors.append(str(exc))

    return result


def validate_gtfs_schedule(
    source: Union[str, Path],
    *,
    from_zip: bool = True,
    schema: Optional[Dict[str, Any]] = None,
    strict_types: bool = True,
) -> ValidationReport:
    """
    Valida un feed GTFS Schedule contra el esquema (desde ZIP o desde BD).

    Parámetros
    ----------
    source : str | Path
        Ruta a .zip (si from_zip=True) o identificador/contexto para lectura desde BD.
    from_zip : bool
        True para validar archivo ZIP; False para validar los datos en BD.
    schema : dict | None
        Esquema machine-readable (recomendado). Si None, se valida con reglas mínimas.
    strict_types : bool
        Si True, falla en tipos inválidos; si False, reporta WARNING pero continúa.

    Retorna
    -------
    ValidationReport
        Lista de issues (errors/warnings) con contexto (archivo, fila, campo).
    """
    report = ValidationReport(source=str(source), from_zip=from_zip)

    # TODO: implementar validaciones mínimas:
    # - Archivos requeridos presentes (cuando from_zip=True)
    # - Campos requeridos presentes por archivo
    # - Tipos: int/float/date/time (time puede exceder 24h)
    # - Rangos: lat/lon, enums (route_type, pickup/drop_off, etc.)
    # - Integridad referencial básica (FKs)
    # - PKs/unique (incluidas compuestas)

    try:
        if from_zip:
            with _open_zip(source) as zf:
                # Ejemplo de registro de issue (cuando implementes validaciones reales):
                # report.issues.append(ValidationIssue(
                #     level="ERROR", file="stops.txt", rownum=12, field="stop_lat",
                #     message="Fuera de rango (-90..90)"
                # ))
                pass
        else:
            # Validación leyendo directamente de la BD (queries a los modelos)
            pass

    except Exception as exc:
        logger.exception("Error validando GTFS")
        report.issues.append(ValidationIssue(
            level="ERROR", file="(general)", rownum=None, field=None, message=str(exc)
        ))

    return report





# Import GTFS models!


#def import_gtfs_schedule(zip_path: str) -> dict:
    """
    Importa un archivo GTFS (ZIP) a la base de datos Django.
    :param zip_path: ruta al archivo GTFS .zip
    :return: dict con estadísticas (registros insertados, errores, warnings)
    """


#def export_gtfs_schedule():
    #return "Exported GTFS Schedule"


#def validate_gtfs_schedule():
    #return "Validated GTFS Schedule"
