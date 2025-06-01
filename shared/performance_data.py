# facturacion_app/shared/performance_data.py

PERFORMANCE_DATA_STORE = {
    'database': [],
    'operation': [],
    'time_ms': []
}

def add_performance_metric(database: str, operation: str, time_ms: float):
    """Agrega una nueva métrica de rendimiento al almacén."""
    PERFORMANCE_DATA_STORE['database'].append(database)
    PERFORMANCE_DATA_STORE['operation'].append(operation)
    PERFORMANCE_DATA_STORE['time_ms'].append(time_ms)

def get_performance_data_store() -> dict:
    """Devuelve el almacén de datos de rendimiento."""
    return PERFORMANCE_DATA_STORE

def clear_performance_data():
    """Limpia los datos de rendimiento almacenados."""
    PERFORMANCE_DATA_STORE['database'].clear()
    PERFORMANCE_DATA_STORE['operation'].clear()
    PERFORMANCE_DATA_STORE['time_ms'].clear()
