import redis
import json
import time
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple, Callable
from infrastructure.adapters.out.connectors.base_connector import BaseConnector

class RedisConnector(BaseConnector):
    def __init__(self):
        super().__init__("redis") # Call the constructor of the base class
        self.client = None
        self.host = None
        self.port = None
        self.password = None
        self.db = None

    def connect(self, host: str, port: int, password: str = "", db: int = 0, **kwargs):
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        try:
            self.client = redis.StrictRedis(
                host=self.host,
                port=self.port,
                password=self.password if self.password else None,
                db=self.db,
                decode_responses=True
            )
            self.client.ping()
            print(f"Conectado a Redis en {self.host}:{self.port}")
        except redis.exceptions.ConnectionError as e:
            raise ConnectionError(f"No se pudo conectar a Redis: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado al conectar a Redis: {e}")

    def disconnect(self):
        if self.client:
            self.client.close()
            self.client = None
            print("Desconectado de Redis.")

    def measure_time(self, operation_name: str, func: Callable, *args: Any, **kwargs: Any) -> Tuple[Any, float]:
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
        print(f"Tiempo de ejecución para {operation_name}: {execution_time:.2f} ms")
        return result, execution_time

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        # Redis no ejecuta "queries" SQL tradicionales.
        # Esta función se adaptará para operaciones clave-valor o comandos Redis.
        # Para simplificar, asumiremos que 'query' es un comando Redis y 'params' son sus argumentos.
        # Esto es una simplificación y podría necesitar una implementación más robusta
        # dependiendo de cómo se usen las "queries" en el contexto de Redis.
        try:
            if not self.client:
                raise ConnectionError("No hay conexión a Redis.")
            
            # Ejemplo básico: si la "query" es un comando simple como 'GET' o 'SET'
            # Esto es un placeholder y debe ser adaptado a los comandos reales que se usarán.
            command_parts = query.split()
            command = command_parts[0].upper()
            args = command_parts[1:]

            if command == "GET":
                key = args[0]
                return self.client.get(key)
            elif command == "SET":
                key = args[0]
                value = args[1]
                return self.client.set(key, value)
            elif command == "HGETALL":
                key = args[0]
                return self.client.hgetall(key)
            elif command == "HMSET": # HMSET está obsoleto, usar HSET
                key = args[0]
                mapping = {args[i]: args[i+1] for i in range(0, len(args), 2)}
                return self.client.hset(key, mapping=mapping)
            elif command == "LPUSH":
                key = args[0]
                values = args[1:]
                return self.client.lpush(key, *values)
            elif command == "LRANGE":
                key = args[0]
                start = int(args[1])
                end = int(args[2])
                return self.client.lrange(key, start, end)
            elif command == "DELETE":
                keys = args
                return self.client.delete(*keys)
            elif command == "EXISTS":
                key = args[0]
                return self.client.exists(key)
            elif command == "KEYS":
                pattern = args[0]
                return self.client.keys(pattern)
            else:
                # Intenta ejecutar el comando directamente si no es uno de los manejados explícitamente
                # Esto es peligroso y solo para demostración. En producción, se debe validar.
                return self.client.execute_command(command, *args)

        except redis.exceptions.RedisError as e:
            raise Exception(f"Error al ejecutar comando Redis: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado al ejecutar comando Redis: {e}")

    def execute_sp(self, sp_name: str, params: Any) -> Tuple[Any, float]:
        return self.measure_time(f"execute_sp_{sp_name}", self.call_stored_procedure, sp_name, params)

    def create_tables(self) -> None:
        print("Simulando creación de 'tablas' en Redis. Redis no tiene el concepto de tablas SQL.")
        # Aquí podrías inicializar contadores o claves de esquema si fuera necesario
        # Por ejemplo, para asegurar que los contadores de ID existen
        self.client.setnx("clientes:next_id", 1)
        self.client.setnx("productos:next_id", 1)
        self.client.setnx("personal:next_id", 1)
        self.client.setnx("facturas:next_id", 1)
        self.client.setnx("detalles_factura:next_id", 1)

    def create_stored_procedures(self) -> None:
        print("Simulando creación de 'procedimientos almacenados' en Redis. Se usarán scripts Lua.")
        # Aquí podrías cargar scripts Lua predefinidos si los tuvieras
        # Por ejemplo:
        # self.create_stored_procedure("my_lua_script", "return redis.call('GET', KEYS[1])")

    def fetch_all_records(self, table_name: str) -> pd.DataFrame:
        results, exec_time = self.measure_time(f"fetch_all_records_{table_name}", self.fetch_data, table_name)
        return pd.DataFrame(results)

    def insert_record(self, table_name: str, data: dict) -> Any:
        return self.measure_time(f"insert_record_{table_name}", self.insert_data, table_name, data)[0]

    def update_record(self, table_name: str, record_id: Any, data: dict) -> None:
        self.measure_time(f"update_record_{table_name}", self.update_data, table_name, "id", record_id, data)

    def delete_record(self, table_name: str, record_id: Any) -> None:
        self.measure_time(f"delete_record_{table_name}", self.delete_data, table_name, "id", record_id)

    def is_table_empty(self, table_name: str) -> bool:
        count, exec_time = self.measure_time(f"count_data_{table_name}", self.count_data, table_name)
        return count == 0

    def fetch_all(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        # Para Redis, esto podría significar obtener todos los campos de un HASH o todos los elementos de una LISTA.
        # Se necesita una convención sobre cómo se "consultan" los datos en Redis para que esto tenga sentido.
        # Por ahora, se asume que la "query" es una clave para un HASH y devuelve sus campos.
        try:
            result = self.execute_query(query, params)
            if isinstance(result, dict):
                # Si es un HASH, lo convertimos a una lista de un solo diccionario para mantener la consistencia
                return [result]
            elif isinstance(result, list):
                # Si es una lista (ej. LRANGE), la convertimos a una lista de diccionarios con una clave 'value'
                return [{"value": item} for item in result]
            elif result is not None:
                # Para otros tipos de datos escalares, envolver en un diccionario
                return [{"value": result}]
            return []
        except Exception as e:
            raise Exception(f"Error al obtener todos los datos de Redis: {e}")

    def fetch_one(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        # Similar a fetch_all, pero devuelve solo el primer resultado.
        try:
            results = self.fetch_all(query, params)
            return results[0] if results else None
        except Exception as e:
            raise Exception(f"Error al obtener un solo dato de Redis: {e}")

    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        # Redis no tiene un concepto de "esquema de tabla" como las bases de datos relacionales.
        # Esto es un placeholder. Podría implementarse para devolver un esquema esperado
        # basado en convenciones de la aplicación o metadatos almacenados en Redis.
        print(f"Advertencia: get_table_schema no es aplicable directamente a Redis para la tabla {table_name}.")
        return {}

    def get_existing_tables(self) -> List[str]:
        # Redis no tiene "tablas". Podríamos listar claves que sigan un patrón
        # o que representen colecciones lógicas.
        # Por ahora, devuelve una lista vacía o un placeholder.
        print("Advertencia: get_existing_tables no es aplicable directamente a Redis.")
        return []

    def create_table(self, table_name: str, columns: Dict[str, str]):
        # En Redis, "crear una tabla" podría significar establecer una convención de claves
        # o inicializar un HASH/SET/LIST vacío para esa "tabla".
        print(f"Simulando creación de 'tabla' {table_name} en Redis. No hay concepto de tabla SQL.")
        # Podríamos, por ejemplo, crear una clave para indicar la existencia de esta "tabla"
        self.client.set(f"schema:{table_name}:exists", "true")

    def drop_table(self, table_name: str):
        # En Redis, "eliminar una tabla" podría significar eliminar todas las claves
        # que pertenecen a esa "tabla" o la clave de convención de esquema.
        print(f"Simulando eliminación de 'tabla' {table_name} en Redis. No hay concepto de tabla SQL.")
        # Eliminar la clave de existencia y cualquier clave asociada si se sigue una convención
        keys_to_delete = self.client.keys(f"{table_name}:*")
        if keys_to_delete:
            self.client.delete(*keys_to_delete)
        self.client.delete(f"schema:{table_name}:exists")

    def create_index(self, table_name: str, column_name: str):
        # Redis tiene índices específicos (ej. RediSearch). Esto es un placeholder.
        print(f"Advertencia: create_index no es aplicable directamente a Redis sin RediSearch para {table_name}.{column_name}.")

    def create_stored_procedure(self, procedure_name: str, definition: str):
        # Redis no tiene procedimientos almacenados SQL. Podríamos usar scripts Lua.
        print(f"Simulando creación de 'procedimiento almacenado' {procedure_name} en Redis usando scripts Lua.")
        # Almacenar el script Lua en Redis para su posterior ejecución
        self.client.set(f"lua_script:{procedure_name}", definition)

    def call_stored_procedure(self, procedure_name: str, params: Optional[Dict[str, Any]] = None) -> Any:
        # Ejecutar un script Lua almacenado.
        script = self.client.get(f"lua_script:{procedure_name}")
        if not script:
            raise ValueError(f"Script Lua '{procedure_name}' no encontrado en Redis.")
        
        # Evaluar el script Lua. Los parámetros deben pasarse como argumentos.
        # Esto es una simplificación. La forma de pasar args y keys a EVAL es más compleja.
        # Para un uso real, se necesitaría una lógica más sofisticada.
        args = []
        if params:
            for k, v in params.items():
                args.append(str(v)) # Convertir todos los valores a string para Lua
        
        # Ejemplo muy simplificado: asume que el script no necesita KEYS y solo ARGV
        return self.client.eval(script, 0, *args)

    def insert_data(self, table_name: str, data: Dict[str, Any]):
        # En Redis, esto podría ser HMSET para HASHes o LPUSH para LISTAs, etc.
        # Asumimos que 'table_name' es un prefijo de clave y 'data' es un diccionario para un HASH.
        key = f"{table_name}:{data.get('id', self.client.incr(f'{table_name}:next_id'))}"
        if 'id' not in data:
            data['id'] = key.split(':')[-1] # Asegurar que el ID se guarda en los datos
        self.client.hset(key, mapping=data)
        print(f"Datos insertados en Redis bajo la clave {key}")
        return data['id']

    def update_data(self, table_name: str, identifier_column: str, identifier_value: Any, data: Dict[str, Any]):
        # Asumimos que 'identifier_column' es 'id' y 'identifier_value' es el ID de la clave.
        key = f"{table_name}:{identifier_value}"
        if not self.client.exists(key):
            raise ValueError(f"La clave {key} no existe para actualizar.")
        self.client.hset(key, mapping=data)
        print(f"Datos actualizados en Redis bajo la clave {key}")

    def delete_data(self, table_name: str, identifier_column: str, identifier_value: Any):
        # Asumimos que 'identifier_column' es 'id' y 'identifier_value' es el ID de la clave.
        key = f"{table_name}:{identifier_value}"
        if not self.client.exists(key):
            raise ValueError(f"La clave {key} no existe para eliminar.")
        self.client.delete(key)
        print(f"Datos eliminados de Redis bajo la clave {key}")

    def fetch_data(self, table_name: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        # Esto es complejo en Redis sin RediSearch.
        # Una implementación simple podría ser obtener todas las claves que coincidan con un patrón
        # y luego filtrar en el lado del cliente. Esto no es eficiente para grandes datasets.
        pattern = f"{table_name}:*"
        keys = self.client.keys(pattern)
        results = []
        for key in keys:
            data = self.client.hgetall(key)
            if data:
                # Convertir valores a tipos apropiados si es necesario (Redis devuelve strings)
                # Aquí se asume que los valores son strings y se mantienen así.
                item = {k: v for k, v in data.items()}
                
                # Aplicar filtros si existen
                match = True
                if filters:
                    for f_key, f_value in filters.items():
                        if f_key not in item or str(item[f_key]) != str(f_value):
                            match = False
                            break
                if match:
                    results.append(item)
        print(f"Datos obtenidos de Redis para la 'tabla' {table_name} con filtros {filters}")
        return results

    def count_data(self, table_name: str, filters: Optional[Dict[str, Any]] = None) -> int:
        # Similar a fetch_data, pero solo cuenta.
        return len(self.fetch_data(table_name, filters))

    def get_last_inserted_id(self, table_name: str) -> Optional[Any]:
        # Para Redis, esto podría ser el último ID de un contador si se usa uno.
        # Asumimos que usamos un contador `table_name:next_id`.
        last_id = self.client.get(f"{table_name}:next_id")
        return int(last_id) - 1 if last_id else None

    def generate_test_data(self, num_records_per_table: int = 10):
        print(f"Generando datos de prueba para Redis (simulado, {num_records_per_table} registros por 'tabla')...")
        
        # Limpiar datos existentes para evitar duplicados en cada ejecución
        self.client.delete("clientes:next_id", "productos:next_id", "personal:next_id", "facturas:next_id", "detalles_factura:next_id")
        self.client.delete(*self.client.keys("clientes:*"))
        self.client.delete(*self.client.keys("productos:*"))
        self.client.delete(*self.client.keys("personal:*"))
        self.client.delete(*self.client.keys("facturas:*"))
        self.client.delete(*self.client.keys("detalles_factura:*"))

        # Datos de Clientes
        for i in range(1, num_records_per_table + 1):
            client_data = {
                "id": str(i),
                "nombre": f"Cliente {i}",
                "email": f"cliente{i}@example.com",
                "telefono": f"111-222-00{i:02d}"
            }
            self.insert_data("clientes", client_data)

        # Datos de Productos
        for i in range(1, num_records_per_table + 1):
            product_data = {
                "id": str(i),
                "nombre": f"Producto {i}",
                "precio": str(10.0 + i),
                "stock": str(100 + i)
            }
            self.insert_data("productos", product_data)

        # Datos de Personal
        for i in range(1, num_records_per_table + 1):
            staff_data = {
                "id": str(i),
                "nombre": f"Personal {i}",
                "cargo": "Vendedor"
            }
            self.insert_data("personal", staff_data)

        # Datos de Facturas y Detalles de Factura (simulados)
        for i in range(1, num_records_per_table + 1):
            invoice_data = {
                "id": str(i),
                "cliente_id": str(i),
                "personal_id": str(i),
                "fecha": "2023-01-01",
                "total": str(50.0 + i)
            }
            self.insert_data("facturas", invoice_data)

            detail_data = {
                "id": str(detail_id),
                "factura_id": str(invoice_id),
                "producto_id": str(p["producto_id"]),
                "cantidad": str(p["cantidad"]),
                "precio_unitario": str(self.search_product(p["producto_id"])[0].get("precio", 0))
            }
            self.insert_data("detalles_factura", detail_data)
        
        return invoice_data, 0.0 # Return 0.0 for execution time for now

    def query_invoice(self, invoice_id: int) -> Tuple[Any, float]:
        key = f"facturas:{invoice_id}"
        invoice, exec_time = self.measure_time(f"query_invoice_{invoice_id}", self.fetch_one, f"HGETALL {key}")
        if invoice:
            # También obtener detalles de la factura
            details = self.fetch_data("detalles_factura", {"factura_id": str(invoice_id)})
            invoice["detalles"] = details
        return invoice, exec_time

    def sales_report(self) -> Tuple[Any, float]:
        # Simular un informe de ventas. Esto podría ser costoso en Redis sin RediSearch.
        # Aquí, simplemente recuperamos todas las facturas y las devolvemos.
        all_invoices_keys = self.client.keys("facturas:*")
        sales_data = []
        for key in all_invoices_keys:
            invoice = self.client.hgetall(key)
            if invoice:
                sales_data.append(invoice)
        return sales_data, 0.0 # Return 0.0 for execution time for now
