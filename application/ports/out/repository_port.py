import abc
import pandas as pd
from typing import Any, Callable, Tuple

class RepositoryPort(abc.ABC):
    """
    Puerto de repositorio que define las operaciones de persistencia de datos
    que la capa de aplicación espera.
    """

    @abc.abstractmethod
    def connect(self, **credentials: Any) -> None:
        """Establece la conexión con la base de datos."""
        pass

    @abc.abstractmethod
    def disconnect(self) -> None:
        """Cierra la conexión con la base de datos."""
        pass

    @abc.abstractmethod
    def create_tables(self) -> None:
        """Crea las tablas necesarias en la base de datos."""
        pass

    @abc.abstractmethod
    def create_stored_procedures(self) -> None:
        """Crea los procedimientos almacenados necesarios."""
        pass

    @abc.abstractmethod
    def generate_test_data(self, num_records_per_table: int = 10) -> None:
        """Genera datos de prueba en las tablas."""
        pass

    @abc.abstractmethod
    def fetch_all_records(self, table_name: str) -> pd.DataFrame:
        """Obtiene todos los registros de una tabla."""
        pass

    @abc.abstractmethod
    def insert_record(self, table_name: str, data: dict) -> Any:
        """Inserta un nuevo registro en una tabla."""
        pass

    @abc.abstractmethod
    def update_record(self, table_name: str, record_id: Any, data: dict) -> None:
        """Actualiza un registro existente en una tabla."""
        pass

    @abc.abstractmethod
    def delete_record(self, table_name: str, record_id: Any) -> None:
        """Elimina un registro de una tabla."""
        pass

    @abc.abstractmethod
    def execute_sp(self, sp_name: str, params: tuple = ()) -> Any:
        """Ejecuta un procedimiento almacenado."""
        pass
    
    @abc.abstractmethod
    def measure_time(self, operation_name: str, func_to_measure: Callable, *args: Any, **kwargs: Any) -> Tuple[Any, float]:
        """
        Mide el tiempo de ejecución de una función o método.
        Devuelve el resultado de la función y el tiempo de ejecución en milisegundos.
        """
        pass

    # Métodos específicos para pruebas de rendimiento (pueden ser implementados llamando a execute_sp o fetch_all_records)
    @abc.abstractmethod
    def search_client(self, client_id: int = 1) -> Tuple[Any, float]:
        pass

    @abc.abstractmethod
    def search_product(self, product_id: int = 1) -> Tuple[Any, float]:
        pass

    @abc.abstractmethod
    def generate_invoice(self, client_id: int = 1, staff_id: int = 1, products_json_str: str = '[{"producto_id": 1, "cantidad": 1}]') -> Tuple[Any, float]:
        pass

    @abc.abstractmethod
    def query_invoice(self, invoice_id: int = 1) -> Tuple[Any, float]:
        pass

    @abc.abstractmethod
    def sales_report(self) -> Tuple[Any, float]:
        pass
