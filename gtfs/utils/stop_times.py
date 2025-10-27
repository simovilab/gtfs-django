# Temporal file for testing purposes only.

def estimate_stop_times(*args, **kwargs):
    """
    Función simulada para estimar tiempos de parada (ETA) durante pruebas.
    Retorna una lista vacía o valores determinísticos de ejemplo.
    """
    return [
        {"stop_id": "STOP_X", "arrival": {"time": 1730001800}},
        {"stop_id": "STOP_Y", "arrival": {"time": 1730002400}},
    ]
