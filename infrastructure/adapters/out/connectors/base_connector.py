from abc import ABC, abstractmethod
import pandas as pd
from typing import Any, Tuple, Callable

class BaseConnector(ABC):
    """
    Clase base abstracta para conectores de base de datos.
    Define la interfaz que todos los conectores deben implementar.
    """
    def __init__(self, db_type: str):
        self.db_type = db_type
        self.connection = None
        self.cursor = None

    @abstractmethod
    def connect(self, **credentials: Any) -> None:
        """Establece una conexión a la base de datos."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Cierra la conexión a la base de datos."""
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Any = None) -> Tuple[Any, float]:
        """Ejecuta una consulta SQL y devuelve el cursor y el tiempo de ejecución."""
        pass

    @abstractmethod
    def execute_sp(self, sp_name: str, params: Any) -> Tuple[Any, float]:
        """Ejecuta un procedimiento almacenado y devuelve el resultado y el tiempo de ejecución."""
        pass

    @abstractmethod
    def measure_time(self, operation_name: str, func: Callable, *args: Any, **kwargs: Any) -> Tuple[Any, float]:
        """Mide el tiempo de ejecución de una función."""
        pass

    @abstractmethod
    def create_tables(self) -> None:
        """Crea las tablas necesarias en la base de datos."""
        pass

    @abstractmethod
    def create_stored_procedures(self) -> None:
        """Crea los procedimientos almacenados necesarios en la base de datos."""
        pass

    @abstractmethod
    def generate_test_data(self) -> None:
        """Genera datos de prueba para las tablas."""
        pass

    @abstractmethod
    def fetch_all_records(self, table_name: str) -> pd.DataFrame:
        """Recupera todos los registros de una tabla."""
        pass

    @abstractmethod
    def insert_record(self, table_name: str, data: dict) -> Any:
        """Inserta un nuevo registro en una tabla."""
        pass

    @abstractmethod
    def update_record(self, table_name: str, record_id: Any, data: dict) -> None:
        """Actualiza un registro existente en una tabla."""
        pass

    @abstractmethod
    def delete_record(self, table_name: str, record_id: Any) -> None:
        """Elimina un registro de una tabla."""
        pass

    @abstractmethod
    def search_client(self, client_id: int = 1) -> Tuple[Any, float]:
        """Busca un cliente por ID."""
        pass

    @abstractmethod
    def search_product(self, product_id: int = 1) -> Tuple[Any, float]:
        """Busca un producto por ID."""
        pass

    @abstractmethod
    def generate_invoice(self, client_id: int, staff_id: int, products_json_str: str) -> Tuple[Any, float]:
        """Genera una factura."""
        pass

    @abstractmethod
    def query_invoice(self, invoice_id: int = 1) -> Tuple[Any, float]:
        """Consulta una factura por ID."""
        pass

    @abstractmethod
    def sales_report(self) -> Tuple[Any, float]:
        """Genera un informe de ventas."""
        pass

    @abstractmethod
    def is_table_empty(self, table_name: str) -> bool:
        """Verifica si una tabla está vacía."""
        pass
