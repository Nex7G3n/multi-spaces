import time
from datetime import datetime, date
import json
import pandas as pd
import numpy as np
from psycopg2.extras import Json
from infrastructure.adapters.out.connectors.base_connector import BaseConnector

class PostgreSQLConnector(BaseConnector):
    def __init__(self, db_type="PostgreSQL"):
        super().__init__(db_type)
        self.db_type = db_type
        self.connection = None
        self.cursor = None

    def connect(self, host, database, user, password, port):
        try:
            import psycopg2
        except ImportError:
            raise RuntimeError("El conector 'psycopg2' no está instalado. Por favor, instala 'psycopg2-binary' (`pip install psycopg2-binary`).")
        
        conn_str = f"host={host} dbname={database} user={user} password={password} port={port}"
        self.connection = psycopg2.connect(conn_str)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            self.cursor = None

    def execute_query(self, query, params=None):
        start_time = time.time()
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            execution_time = (time.time() - start_time) * 1000  # ms
            return self.cursor, execution_time
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise # Re-lanzar la excepción para que la capa superior la maneje

    def execute_sp(self, sp_name, params):
        start_time = time.time()
        try:
            query = f"SELECT * FROM {sp_name}({', '.join(['%s' for _ in params])})"
            self.cursor.execute(query, params)
            result = self.cursor.fetchone()
            execution_time = (time.time() - start_time) * 1000  # ms
            return result, execution_time
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise # Re-lanzar la excepción para que la capa superior la maneje

    def measure_time(self, operation_name, func, *args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = (time.time() - start_time) * 1000  # ms
        return result, execution_time

    def create_tables(self):
        queries = [
            """CREATE TABLE IF NOT EXISTS Clientes (
                cliente_id SERIAL PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(100),
                telefono VARCHAR(20),
                direccion VARCHAR(200)
            );""",
            """CREATE TABLE IF NOT EXISTS Personal (
                personal_id SERIAL PRIMARY KEY,
                nombre VARCHAR(100),
                rol VARCHAR(50)
            );""",
            """CREATE TABLE IF NOT EXISTS Producto (
                producto_id SERIAL PRIMARY KEY,
                nombre VARCHAR(100),
                precio DECIMAL(10,2),
                stock INT
            );""",
            """CREATE TABLE IF NOT EXISTS Factura (
                factura_id SERIAL PRIMARY KEY,
                cliente_id INT REFERENCES Clientes(cliente_id),
                personal_id INT REFERENCES Personal(personal_id),
                fecha TIMESTAMP,
                total DECIMAL(10,2)
            );""",
            """CREATE TABLE IF NOT EXISTS Detalle_Factura (
                detalle_id SERIAL PRIMARY KEY,
                factura_id INT REFERENCES Factura(factura_id),
                producto_id INT REFERENCES Producto(producto_id),
                cantidad INT,
                precio_unitario DECIMAL(10,2),
                subtotal DECIMAL(10,2)
            );"""
        ]

        for query in queries:
            try:
                print(f"DEBUG CONNECTOR: Ejecutando query de creación de tabla (IF NOT EXISTS): {query[:100]}...")
                self.execute_query(query)
                self.connection.commit()
                print("DEBUG CONNECTOR: Commit realizado para creación de tabla.")
                
                # Specific check for Clientes table after creation
                if "CREATE TABLE IF NOT EXISTS Clientes" in query:
                    try:
                        check_query = """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'clientes' AND column_name = 'cliente_id';
                        """
                        self.cursor.execute(check_query)
                        result = self.cursor.fetchone()
                        if not result:
                            raise RuntimeError("La columna 'cliente_id' no se encontró en la tabla 'Clientes' después de la creación. Verifique el esquema de la base de datos PostgreSQL.")
                        print("DEBUG CONNECTOR: Verificación de columna 'cliente_id' en 'Clientes' exitosa.")
                    except Exception as check_e:
                        print(f"ERROR CONNECTOR: Falló la verificación de la tabla Clientes en PostgreSQL: {check_e}")
                        raise # Re-lanzar para que el error sea visible
            except Exception as e:
                print(f"ERROR CONNECTOR al ejecutar query de creación de tabla en PostgreSQL: {e}")
                self.connection.rollback()
                print("DEBUG CONNECTOR: Rollback realizado para creación de tabla.")
    def create_stored_procedures(self):
        sp_query = """
        DROP FUNCTION IF EXISTS sp_generar_factura(INT, INT, JSONB);
        CREATE OR REPLACE FUNCTION sp_generar_factura(
            p_cliente_id INT,
            p_personal_id INT,
            p_productos_json JSONB
        )
        RETURNS TABLE (factura_id INT, total DECIMAL(10,2)) AS $$
        DECLARE
            v_factura_id INT;
            v_total DECIMAL(10,2) := 0;
            producto_rec RECORD;
        BEGIN
            -- Obtener el próximo ID de factura (ya es SERIAL, pero se usa MAX+1 para consistencia)
            SELECT COALESCE(MAX(Factura.factura_id), 0) + 1 INTO v_factura_id FROM Factura;

            -- Insertar la factura con total=0 temporalmente
            INSERT INTO Factura (factura_id, cliente_id, personal_id, fecha, total)
            VALUES (v_factura_id, p_cliente_id, p_personal_id, NOW(), 0);

            -- Procesar los productos
            FOR producto_rec IN
                SELECT * FROM jsonb_to_recordset(p_productos_json) AS x(producto_id INT, cantidad INT)
            LOOP
                DECLARE
                    v_precio_unitario DECIMAL(10,2);
                    v_subtotal DECIMAL(10,2);
                BEGIN
                    SELECT precio INTO v_precio_unitario
                    FROM Producto
                    WHERE producto_id = producto_rec.producto_id;

                    IF v_precio_unitario IS NULL THEN
                        RAISE EXCEPTION 'Producto con ID % no encontrado.', producto_rec.producto_id;
                    END IF;

                    v_subtotal := producto_rec.cantidad * v_precio_unitario;

                    INSERT INTO Detalle_Factura (
                        factura_id, producto_id, cantidad, precio_unitario, subtotal
                    ) VALUES (
                        v_factura_id, producto_rec.producto_id, producto_rec.cantidad,
                        v_precio_unitario, v_subtotal
                    );

                    -- UPDATE Producto -- Comentado temporalmente para depuración
                    -- SET stock = stock - producto_rec.cantidad
                    -- WHERE producto_id = producto_rec.producto_id;

                    v_total := v_total + v_subtotal;
                END;
            END LOOP;

            -- Actualizar total de factura
            UPDATE Factura
            SET total = v_total
            WHERE Factura.factura_id = v_factura_id;

            RETURN QUERY SELECT v_factura_id, v_total;
        END;
        $$ LANGUAGE plpgsql;
        """
        try:
            print(f"DEBUG CONNECTOR: Ejecutando query de creación de SP: {sp_query[:100]}...")
            self.execute_query(sp_query)
            self.connection.commit()
            print("DEBUG CONNECTOR: Commit realizado para creación de SP.")
        except Exception as e:
            print(f"ERROR CONNECTOR al crear SP en PostgreSQL: {e}")
            self.connection.rollback()
            print("DEBUG CONNECTOR: Rollback realizado para creación de SP.")

    def generate_test_data(self):
        num_records = 500
        clientes_data = []
        personal_data = []
        productos_data = []

        for i in range(1, num_records + 1):
            # En este caso, no necesitamos proveer cliente_id ni personal_id ni producto_id,
            # porque esos campos son SERIAL y se autogeneran.
            clientes_data.append((f'Cliente {i}', f'cliente{i}@example.com', f'111-222-{i:04d}', f'Dir {i}'))
            personal_data.append((f'Vendedor {i}', 'Vendedor'))
            productos_data.append((f'Producto {i}', 10.00 + i * 0.5, 100 + i))

        try:
            # Solo insertar datos de prueba si las tablas están vacías
            if self.is_table_empty("Clientes"):
                print("DEBUG CONNECTOR: Insertando datos de prueba para Clientes.")
                self.cursor.executemany(
                    "INSERT INTO Clientes (nombre, email, telefono, direccion) VALUES (%s, %s, %s, %s)",
                    clientes_data
                )
            else:
                print("DEBUG CONNECTOR: La tabla Clientes no está vacía, omitiendo inserción de datos de prueba.")

            if self.is_table_empty("Personal"):
                print("DEBUG CONNECTOR: Insertando datos de prueba para Personal.")
                self.cursor.executemany(
                    "INSERT INTO Personal (nombre, rol) VALUES (%s, %s)",
                    personal_data
                )
            else:
                print("DEBUG CONNECTOR: La tabla Personal no está vacía, omitiendo inserción de datos de prueba.")

            if self.is_table_empty("Producto"):
                print("DEBUG CONNECTOR: Insertando datos de prueba para Producto.")
                self.cursor.executemany(
                    "INSERT INTO Producto (nombre, precio, stock) VALUES (%s, %s, %s)",
                    productos_data
                )
            else:
                print("DEBUG CONNECTOR: La tabla Producto no está vacía, omitiendo inserción de datos de prueba.")

            self.connection.commit()
            print("DEBUG CONNECTOR: Commit realizado para datos de prueba (si se insertaron).")
        except Exception as e:
            print(f"ERROR CONNECTOR al generar datos de prueba en PostgreSQL: {e}")
            self.connection.rollback()
            print("DEBUG CONNECTOR: Rollback realizado para datos de prueba.")

    def fetch_all_records(self, table_name):
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        columns = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        return pd.DataFrame(data, columns=columns)

    def insert_record(self, table_name, data):
        processed_data = {}
        for k, v in data.items():
            if isinstance(v, (int, np.integer)):
                processed_data[k] = int(v)
            elif isinstance(v, (float, np.floating)):
                processed_data[k] = float(v)
            elif isinstance(v, date):
                processed_data[k] = v.strftime('%Y-%m-%d')
            else:
                processed_data[k] = v

        # Asegurarse de no incluir la columna PK (serial) en el INSERT
        columns = ', '.join(processed_data.keys())
        placeholders = ', '.join(['%s'] * len(processed_data.values()))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            print(f"DEBUG CONNECTOR: Intentando INSERT en {table_name}. Query: {query}. Params: {tuple(processed_data.values())}")
            self.cursor.execute(query, tuple(processed_data.values()))
            self.connection.commit()
            print(f"DEBUG CONNECTOR: Commit realizado para INSERT en {table_name}.")
        except Exception as e:
            print(f"ERROR CONNECTOR al insertar registro en {table_name}: {e}")
            self.connection.rollback()
            print(f"DEBUG CONNECTOR: Rollback realizado para INSERT en {table_name}.")
            raise

    def update_record(self, table_name, record_id, data):
        processed_data = {}
        for k, v in data.items():
            if isinstance(v, (int, np.integer)):
                processed_data[k] = int(v)
            elif isinstance(v, (float, np.floating)):
                processed_data[k] = float(v)
            elif isinstance(v, date):
                processed_data[k] = v.strftime('%Y-%m-%d')
            else:
                processed_data[k] = v

        set_clause = ', '.join([f"{col} = %s" for col in processed_data.keys()])
        
        # Determinar la columna de clave primaria dinámicamente
        pk_col_map = {
            'clientes': 'cliente_id',
            'personal': 'personal_id',
            'producto': 'producto_id',
            'factura': 'factura_id',
            'detalle_factura': 'detalle_id'
        }
        
        # Asegurarse de que la tabla exista en el mapeo para evitar errores
        pk_col = pk_col_map.get(table_name.lower(), 'id') # 'id' como fallback, aunque debería estar en el map

        query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_col} = %s"
        processed_record_id = int(record_id) if isinstance(record_id, (np.integer, np.int64)) else record_id
        params_for_query = tuple(list(processed_data.values()) + [processed_record_id])
        
        try:
            print(f"DEBUG CONNECTOR: Intentando UPDATE en {table_name} con ID {record_id}.")
            print(f"DEBUG CONNECTOR: PK Columna: {pk_col}")
            print(f"DEBUG CONNECTOR: Query de UPDATE: {query}")
            print(f"DEBUG CONNECTOR: Parámetros de UPDATE: {params_for_query}")
            self.cursor.execute(query, params_for_query)
            self.connection.commit()
            print(f"DEBUG CONNECTOR: Commit realizado para UPDATE en {table_name} ID {record_id}.")
        except Exception as e:
            print(f"ERROR CONNECTOR al actualizar registro (ID: {record_id}) en {table_name}: {e}")
            self.connection.rollback()
            print(f"DEBUG CONNECTOR: Rollback realizado para UPDATE en {table_name} ID {record_id}.")
            raise

    def delete_record(self, table_name, record_id):
        # Determinar la columna de clave primaria dinámicamente
        pk_col_map = {
            'clientes': 'cliente_id',
            'personal': 'personal_id',
            'producto': 'producto_id',
            'factura': 'factura_id',
            'detalle_factura': 'detalle_id'  # Asumiendo que Detalle_Factura también sigue este patrón
        }
        
        # Asegurarse de que la tabla exista en el mapeo para evitar errores
        # Si no está en el mapeo, se podría usar un fallback o lanzar un error,
        # pero para este caso, nos enfocamos en las tablas conocidas.
        pk_col = pk_col_map.get(table_name.lower())
        
        if not pk_col:
            # Fallback o manejo de error si la tabla no está en el mapeo
            # Por ejemplo, podríamos asumir 'id' o lanzar una excepción.
            # Para este caso, si no se encuentra, podría ser un problema.
            # Sin embargo, el error original es específico para 'Clientes', así que nos enfocamos en eso.
            # Si table_name es 'Detalle_Factura', el mapeo anterior lo cubre.
            # Si es una tabla desconocida, esto podría fallar o necesitar un manejo más robusto.
            # Por ahora, mantenemos la lógica original para 'detalle_id' si no está en el map.
            # Aunque lo ideal sería que todas las tablas usen el map.
            # Reconsiderando: el error es sobre 'Clientes', así que el map es la clave.
            # Si la tabla no está en el map, es mejor lanzar un error o tener un default claro.
            # Para el problema específico, 'clientes' estará en el map.
            # Si la tabla es 'Detalle_Factura', el map lo cubre.
            # Si es otra tabla no mapeada, podría haber problemas.
            # Vamos a asegurar que el pk_col se resuelva correctamente para las tablas conocidas.
            # Si table_name.lower() no está en pk_col_map, pk_col será None.
            # Esto causaría un error en la query f-string.
            # Es mejor tener un default o lanzar un error explícito.
            # Para el caso de 'Detalle_Factura', el nombre de la columna es 'detalle_id'.
            # El mapeo ya lo incluye.
            # Si una tabla no está en el mapeo, es un caso no manejado.
            # Por seguridad, si no se encuentra, lanzamos un error o usamos un default.
            # El error original es sobre 'Clientes', que sí está en el map.
            print(f"ADVERTENCIA: Nombre de tabla '{table_name}' no encontrado en pk_col_map para delete_record. Usando un fallback genérico o podría fallar.")
            # Podríamos usar un fallback como: pk_col = f"{table_name.lower()}_id" o simplemente 'id'
            # Pero es mejor ser explícito. El error original es sobre 'Clientes'.
            # El mapeo es la solución correcta para las tablas definidas.
            if table_name.lower() == 'detalle_factura': # Caso especial si no estuviera en el map
                 pk_col = 'detalle_id'
            else:
                 # Si no está en el map y no es un caso especial, esto es un problema.
                 # Para el error original, 'clientes' está en el map.
                 # Si pk_col sigue siendo None aquí, la query fallará.
                 # Es mejor asegurarse que el map cubra todos los casos o lanzar un error.
                 # El map actual cubre 'detalle_factura'.
                 # Si pk_col es None, la query fallará. Esto es bueno para detectar problemas.
                 # No obstante, el código original tenía un if/else, así que lo replicamos con el map.
                 pk_col = pk_col_map.get(table_name.lower(), f"{table_name.lower()}_id") # Fallback al comportamiento anterior si no está en el map

        final_record_id = record_id
        if isinstance(record_id, (np.integer, np.int64)): # Asegurar tipo nativo
            final_record_id = int(record_id)

        query = f"DELETE FROM {table_name} WHERE {pk_col} = %s"
        try:
            print(f"DEBUG CONNECTOR: Intentando DELETE en {table_name} con ID {final_record_id} (Tipo: {type(final_record_id)}). Query: {query}. PK Col: {pk_col}")
            self.cursor.execute(query, (final_record_id,))

            if self.cursor.rowcount == 0:
                print(f"ADVERTENCIA CONNECTOR: DELETE en {table_name} con ID {final_record_id} no afectó ninguna fila. El registro podría no existir o el ID es incorrecto.")
            
            self.connection.commit()
            print(f"DEBUG CONNECTOR: Commit realizado para DELETE en {table_name} ID {final_record_id}. Filas afectadas: {self.cursor.rowcount}")
        except Exception as e:
            print(f"ERROR CONNECTOR al eliminar registro (ID: {record_id}) en {table_name}: {e}")
            self.connection.rollback()
            print(f"DEBUG CONNECTOR: Rollback realizado para DELETE en {table_name} ID {record_id}.")
            raise

    def search_client(self, client_id: int = 1):
        query = "SELECT * FROM Clientes WHERE cliente_id = %s LIMIT 1"
        cursor, exec_time = self.execute_query(query, (client_id,))
        return cursor.fetchone(), exec_time

    def search_product(self, product_id: int = 1):
        query = "SELECT * FROM Producto WHERE producto_id = %s LIMIT 1"
        cursor, exec_time = self.execute_query(query, (product_id,))
        return cursor.fetchone(), exec_time

    def generate_invoice(self, client_id: int, staff_id: int, products_json_str: str):
        # Convertir la cadena JSON a un objeto Json de psycopg2 para PostgreSQL
        productos_json_obj = Json(json.loads(products_json_str))
        
        # Llamar al procedimiento almacenado
        result, exec_time = self.execute_sp("sp_generar_factura", (client_id, staff_id, productos_json_obj))
        self.connection.commit() # Asegurar que la transacción se confirme
        return result, exec_time # execute_sp ya devuelve el resultado y el tiempo

    def query_invoice(self, invoice_id: int = 1):
        query = "SELECT * FROM Factura WHERE factura_id = %s LIMIT 1"
        cursor, exec_time = self.execute_query(query, (invoice_id,))
        return cursor.fetchone(), exec_time

    def sales_report(self):
        query = """
        SELECT 
            p.nombre AS producto, 
            SUM(df.cantidad) AS total_vendido, 
            SUM(df.subtotal) AS ingresos_totales
        FROM Detalle_Factura df
        JOIN Producto p ON df.producto_id = p.producto_id
        GROUP BY p.nombre
        ORDER BY ingresos_totales DESC;
        """
        cursor, exec_time = self.execute_query(query)
        return cursor.fetchall(), exec_time

    def is_table_empty(self, table_name):
        query = f"SELECT COUNT(*) FROM {table_name}"
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        return count == 0
