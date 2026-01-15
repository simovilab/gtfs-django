# gtfs/__init__.py

__all__ = ["__version__", "test_editable_install"]

__version__ = "0.1.0"

def test_editable_install():
    """Simple function to test if editable installation is working."""
    return "Version 2 - Changes are now reflected immediately!"


# ----------------------------------------------------------
# Carga diferida de modelos para evitar errores de registro
# ----------------------------------------------------------
import importlib

def autodiscover_models():
    """Carga los modelos GTFS (Schedule) solo cuando Django ya inicializó las apps."""
    try:
        importlib.import_module("gtfs.models_schedule")
    except ModuleNotFoundError:
        # Si aún no existe el archivo o no está listo, no romper el paquete
        pass


# ----------------------------------------------------------
# Exponer los nombres de los modelos Schedule
# ----------------------------------------------------------
__all__.extend([
    "Agency", "Route", "Trip", "Stop", "StopTime",
    "Calendar", "CalendarDate", "Shape", "FeedInfo",
])
