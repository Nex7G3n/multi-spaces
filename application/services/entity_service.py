import pandas as pd
from typing import Any, Dict
from application.ports.out.repository_port import RepositoryPort

class EntityService:
    """
    Servicio de aplicación para gestionar operaciones CRUD en entidades.
    Utiliza un RepositoryPort para interactuar con la capa de persistencia.
    """
    def __init__(self, repository: RepositoryPort):
        self.repository = repository

    def get_entity_data(self, table_name: str) -> pd.DataFrame:
        """
        Obtiene todos los datos de una entidad (tabla).
        """
        try:
            # La conexión y desconexión se manejan a un nivel superior (ej. UI)
            # o el repositorio/conector las maneja internamente por operación si es necesario.
            # Aquí asumimos que el repositorio está listo para ser usado.
            df = self.repository.fetch_all_records(table_name)
            return df
        except Exception as e:
            # Considerar un logging más robusto aquí
            print(f"Error en EntityService al obtener datos de {table_name}: {e}")
            # Devolver un DataFrame vacío en caso de error para que la UI no falle.
            return pd.DataFrame()

    def add_entity(self, table_name: str, data: Dict[str, Any]) -> Any:
        """
        Agrega un nuevo registro a una entidad (tabla).
        Devuelve el ID del nuevo registro o algún indicador de éxito.
        """
        try:
            # Validaciones de datos podrían ir aquí antes de llamar al repositorio.
            # Por ejemplo, asegurar que los campos requeridos estén presentes.
            return self.repository.insert_record(table_name, data)
        except Exception as e:
            print(f"Error en EntityService al agregar registro a {table_name}: {e}")
            raise # Re-lanzar la excepción para que la capa superior la maneje (ej. mostrar error en UI)

    def update_entity(self, table_name: str, record_id: Any, data: Dict[str, Any]) -> None:
        """
        Actualiza un registro existente en una entidad (tabla).
        """
        try:
            # Validaciones de datos podrían ir aquí.
            self.repository.update_record(table_name, record_id, data)
        except Exception as e:
            print(f"Error en EntityService al actualizar registro {record_id} en {table_name}: {e}")
            raise

    def delete_entity(self, table_name: str, record_id: Any) -> None:
        """
        Elimina un registro de una entidad (tabla).
        """
        try:
            self.repository.delete_record(table_name, record_id)
        except Exception as e:
            print(f"Error en EntityService al eliminar registro {record_id} de {table_name}: {e}")
            raise
