from typing import List, Tuple, Dict, Any
from application.ports.out.repository_port import RepositoryPort
from shared.performance_data import add_performance_metric, get_performance_data_store, clear_performance_data

class PerformanceService:
    """
    Servicio de aplicación para ejecutar pruebas de rendimiento y gestionar sus resultados.
    """
    def __init__(self, repository: RepositoryPort):
        self.repository = repository

    def run_performance_tests(self, db_type_selected: str, operations: List[Tuple[str, str]]) -> Dict[str, List[Any]]:
        """
        Ejecuta una lista de operaciones de prueba de rendimiento.

        Args:
            db_type_selected (str): El tipo de base de datos actual (ej. "PostgreSQL").
            operations (List[Tuple[str, str]]): Lista de tuplas, donde cada tupla contiene
                                                 (nombre_mostrado_operacion, nombre_metodo_en_repositorio).

        Returns:
            Dict[str, List[Any]]: El diccionario de datos de rendimiento actualizado.
        """
        # Considerar si limpiar los datos aquí o permitir que se acumulen.
        # Por ahora, no los limpiamos aquí para permitir múltiples ejecuciones si es necesario,
        # la UI podría ofrecer un botón para limpiar.
        # clear_performance_data() # Descomentar si se desea limpiar antes de cada ejecución.

        for op_name_display, op_method_name in operations:
            try:
                # Los métodos específicos del repositorio (search_client, etc.) ya usan measure_time.
                # Ellos devuelven (resultado, tiempo_ejecucion_ms).
                if hasattr(self.repository, op_method_name) and callable(getattr(self.repository, op_method_name)):
                    method_to_call = getattr(self.repository, op_method_name)
                    
                    # Estos métodos en el RepositoryPort tienen argumentos por defecto.
                    # Los llamamos sin argumentos adicionales aquí, usando esos defaults.
                    _result, exec_time = method_to_call()
                    
                    add_performance_metric(db_type_selected, op_name_display, exec_time)
                    print(f"PerformanceService: {db_type_selected} - {op_name_display}: OK ({exec_time:.2f} ms)")
                else:
                    print(f"PerformanceService: Método '{op_method_name}' no encontrado en el repositorio para la operación '{op_name_display}'.")
                    # Podríamos registrar un error o un tiempo inválido aquí si es necesario.
                    add_performance_metric(db_type_selected, op_name_display, -1.0) # Indicar error

            except Exception as e:
                print(f"PerformanceService: Error ejecutando {op_name_display} en {db_type_selected}: {str(e)}")
                add_performance_metric(db_type_selected, op_name_display, -1.0) # Indicar error
        
        return get_performance_data_store()

    def get_current_performance_data(self) -> Dict[str, List[Any]]:
        """Devuelve los datos de rendimiento acumulados."""
        return get_performance_data_store()

    def clear_all_performance_data(self) -> None:
        """Limpia todos los datos de rendimiento acumulados."""
        clear_performance_data()
