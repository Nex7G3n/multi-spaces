import time
import pandas as pd
from typing import Any, Callable, Tuple, Type, Union

from application.ports.out.repository_port import RepositoryPort
# Asumimos que los conectores estarán disponibles en el path de Python
# Si están en un subdirectorio relativo, necesitaríamos ajustar el import.
# Por ahora, asumimos que se pueden importar directamente o se ajustará el PYTHONPATH.
# from connectors.postgres.postgres_connector import PostgreSQLConnector
# from connectors.sqlserver.sqlserver_connector import SQLServerConnector
# Para evitar errores de importación si los conectores no están en el path directo,
# los pasaremos como tipo en el constructor.

# Definición de un tipo para los conectores
# DatabaseConnector = Union[Type[PostgreSQLConnector], Type[SQLServerConnector]]
# Como no podemos importar directamente los conectores aquí sin causar un posible error
# si la estructura de 'connectors' no está en PYTHONPATH, usaremos 'Any' por ahora
# y confiaremos en layección de dependencias.
DatabaseConnectorInstance = Any


class DbRepository(RepositoryPort):
    """
    Implementación de RepositoryPort que utiliza un conector de base de datos específico.
    """
    def __init__(self, connector_instance: DatabaseConnectorInstance):
        self.connector: DatabaseConnectorInstance = connector_instance
        if not hasattr(self.connector, 'connect') or \
           not hasattr(self.connector, 'disconnect') or \
           not hasattr(self.connector, 'fetch_all_records'):
            # Esta es una verificación simple. Se podrían añadir más para asegurar compatibilidad.
            raise TypeError("El conector proporcionado no tiene los métodos esperados.")

    def connect(self, **credentials: Any) -> None:
        self.connector.connect(**credentials)

    def disconnect(self) -> None:
        if self.connector.connection:
            self.connector.disconnect()

    def create_tables(self) -> None:
        # La conexión y desconexión deben manejarse aquí si el método del conector no lo hace.
        # Asumimos que los métodos del conector manejan su propia conexión si es necesario,
        # o que se llama a connect() antes de estas operaciones.
        # Por seguridad, nos aseguramos de que esté conectado.
        if not self.connector.connection:
            # Esto es un problema, ¿cómo obtenemos las credenciales aquí?
            # La conexión principal debería manejarse a un nivel superior (ej. en streamlit_app.py)
            # y el repositorio asume que ya está conectado para operaciones específicas.
            # O, cada método del repositorio maneja connect/disconnect.
            # Optaremos por el segundo enfoque para encapsular.
            # PERO, esto requiere que las credenciales estén disponibles para el repositorio.
            # Esto complica el diseño.
            #
            # REVISIÓN DE DISEÑO:
            # El flujo original en main.py es:
            # 1. Conectar (una vez al inicio o al cambiar de DB).
            # 2. Realizar operaciones.
            # 3. Desconectar (al final o si hay error).
            #
            # El repositorio NO debería manejar la lógica de conexión global.
            # Debería asumir que el conector se le pasa ya configurado o que
            # los métodos connect/disconnect del repositorio se llaman explícitamente
            # desde la capa de aplicación o UI.
            #
            # Por lo tanto, los métodos como create_tables simplemente llaman al conector.
            pass # La conexión se maneja externamente o por el propio conector.
        
        # Si el conector no tiene estos métodos, fallará. Es responsabilidad del conector implementarlos.
        self.connector.create_tables()


    def create_stored_procedures(self) -> None:
        self.connector.create_stored_procedures()

    def generate_test_data(self, num_records_per_table: int = 10) -> None:
        # Algunos conectores pueden no tener este método con este parámetro.
        # Se podría añadir lógica para llamar a generate_test_data() si existe,
        # o manejarlo de otra forma.
        if hasattr(self.connector, 'generate_test_data'):
            # Verificar si el método del conector acepta el parámetro num_records_per_table
            # Esto es complejo sin introspección más profunda o una interfaz más estricta para conectores.
            # Por simplicidad, lo llamamos. Si falla, es un problema del conector.
            try:
                self.connector.generate_test_data(num_records_per_table=num_records_per_table)
            except TypeError: # El conector podría no aceptar el argumento
                 self.connector.generate_test_data()

    def fetch_all_records(self, table_name: str) -> pd.DataFrame:
        return self.connector.fetch_all_records(table_name)

    def insert_record(self, table_name: str, data: dict) -> Any:
        return self.connector.insert_record(table_name, data)

    def update_record(self, table_name: str, record_id: Any, data: dict) -> None:
        self.connector.update_record(table_name, record_id, data)

    def delete_record(self, table_name: str, record_id: Any) -> None:
        # El conector original no tiene un método delete_record(table_name, record_id)
        # sino delete_record(table_name, pk_value) donde pk_value es el ID.
        # Asumimos que record_id es pk_value.
        self.connector.delete_record(table_name, record_id)


    def execute_sp(self, sp_name: str, params: tuple = ()) -> Any:
        # El conector original tiene execute_sp(self, sp_name, params=None)
        # y execute_sp_with_results(self, sp_name, params=None)
        # Necesitamos decidir cuál usar o si RepositoryPort necesita dos métodos.
        # Por ahora, asumimos que execute_sp es para SPs que pueden o no devolver resultados.
        # Si el SP devuelve resultados, el conector debería manejarlos.
        return self.connector.execute_sp(sp_name, params)

    def measure_time(self, operation_name: str, func_to_measure: Callable, *args: Any, **kwargs: Any) -> Tuple[Any, float]:
        """
        Mide el tiempo de ejecución de una función o método del conector.
        """
        # Esta función es un poco genérica. Si func_to_measure es un método del propio conector,
        # el conector debería tener su propio método measure_time como en el main.py original.
        # Si el conector tiene `measure_time(self, operation_name, method_name_str_or_callable, *args)`
        # podríamos usar eso.
        
        # Replicando la lógica de measure_time del conector original:
        if hasattr(self.connector, operation_name) and callable(getattr(self.connector, operation_name)):
            # Si operation_name es un método directo del conector (ej. 'search_client')
            target_method = getattr(self.connector, operation_name)
            
            start_time = time.perf_counter()
            # Los argumentos para estos métodos específicos (search_client, etc.)
            # deben pasarse a través de *args o **kwargs si es necesario.
            # El puerto los define con parámetros por defecto.
            # Aquí, *args y **kwargs serían los parámetros para el método específico.
            result = target_method(*args, **kwargs)
            end_time = time.perf_counter()
            exec_time_ms = (end_time - start_time) * 1000
            return result, exec_time_ms
        elif func_to_measure: # Si se pasa una función explícita (podría ser un método del conector)
            start_time = time.perf_counter()
            result = func_to_measure(*args, **kwargs)
            end_time = time.perf_counter()
            exec_time_ms = (end_time - start_time) * 1000
            return result, exec_time_ms
        else:
            raise ValueError(f"No se pudo medir el tiempo para '{operation_name}'. Ni el método existe en el conector ni se proporcionó func_to_measure.")

    # Implementación de métodos específicos para pruebas de rendimiento
    # Estos llamarán a measure_time con el método correspondiente del conector.
    # Los argumentos por defecto vienen del RepositoryPort.

    def search_client(self, client_id: int = 1) -> Tuple[Any, float]:
        if not hasattr(self.connector, 'search_client'):
            raise NotImplementedError("El método 'search_client' no está implementado en el conector.")
        return self.measure_time('search_client', getattr(self.connector, 'search_client'), client_id)

    def search_product(self, product_id: int = 1) -> Tuple[Any, float]:
        if not hasattr(self.connector, 'search_product'):
            raise NotImplementedError("El método 'search_product' no está implementado en el conector.")
        return self.measure_time('search_product', getattr(self.connector, 'search_product'), product_id)

    def generate_invoice(self, client_id: int = 1, staff_id: int = 1, products_json_str: str = '[{"producto_id": 1, "cantidad": 1}]') -> Tuple[Any, float]:
        # El conector original tiene `generate_invoice` que llama a `sp_generar_factura`.
        # Y `sp_generar_factura` espera `productos_param` que puede ser `Json` o `str`.
        # El `postgres_connector` convierte la cadena JSON a `psycopg2.extras.Json`.
        # El `DbRepository` no debería conocer estos detalles.
        # El método `generate_invoice` del conector debe manejar la transformación.
        if not hasattr(self.connector, 'generate_invoice'):
            raise NotImplementedError("El método 'generate_invoice' no está implementado en el conector.")
        
        # El método del conector `generate_invoice` espera (cliente_id, personal_id, productos_json_str)
        return self.measure_time('generate_invoice', getattr(self.connector, 'generate_invoice'), client_id, staff_id, products_json_str)

    def query_invoice(self, invoice_id: int = 1) -> Tuple[Any, float]:
        if not hasattr(self.connector, 'query_invoice'):
            raise NotImplementedError("El método 'query_invoice' no está implementado en el conector.")
        return self.measure_time('query_invoice', getattr(self.connector, 'query_invoice'), invoice_id)

    def sales_report(self) -> Tuple[Any, float]:
        if not hasattr(self.connector, 'sales_report'):
            raise NotImplementedError("El método 'sales_report' no está implementado en el conector.")
        return self.measure_time('sales_report', getattr(self.connector, 'sales_report'))
