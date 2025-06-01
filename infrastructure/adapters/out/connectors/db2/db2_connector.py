import time
from datetime import datetime, date
import json
import pandas as pd
import numpy as np
from infrastructure.adapters.out.connectors.base_connector import BaseConnector

class DB2Connector(BaseConnector):
    def __init__(self, db_type="DB2"):
        super().__init__(db_type)

    def connect(self, database, host, port, protocol, uid, pwd):
        try:
            import ibm_db
        except ImportError:
            raise RuntimeError("El conector 'ibm_db' no está instalado o el controlador CLI de Db2 no está configurado. Por favor, instala 'ibm_db' (`pip install ibm_db`) y asegúrate de que el controlador CLI de Db2 esté instalado y las variables de entorno (IBM_DB_HOME, PATH) estén configuradas correctamente.")
        
        # DSN (Data Source Name) connection string
        conn_str = (
            f"DATABASE={database};"
            f"HOSTNAME={host};"
            f"PORT={port};"
            f"PROTOCOL={protocol};"
            f"UID={uid};"
            f"PWD={pwd};"
        )
        self.connection = ibm_db.connect(conn_str, "", "") # uid and pwd are in conn_str
        self.cursor = ibm_db.cursor(self.connection)

    def disconnect(self):
        if self.connection:
            ibm_db.close(self.connection)
            self.connection = None
            self.cursor = None

    def execute_query(self, query, params=None):
        start_time = time.time()
        try:
            if params:
                stmt = ibm_db.prepare(self.connection, query)
                ibm_db.execute(stmt, params)
                cursor_result = stmt # ibm_db returns statement handle
            else:
                cursor_result = ibm_db.exec_immediate(self.connection, query)
            
            execution_time = (time.time() - start_time) * 1000  # ms
            return cursor_result, execution_time
        except Exception as e:
            if self.connection:
                ibm_db.rollback(self.connection)
            raise # Re-lanzar la excepción para que la capa superior la maneje

    def execute_sp(self, sp_name, params):
        start_time = time.time()
        try:
            # DB2 usa CALL para procedimientos almacenados
            # Los parámetros se pasan como placeholders '?'
            param_placeholders = ', '.join(['?' for _ in params])
            query = f"CALL {sp_name}({param_placeholders})"
            
            stmt = ibm_db.prepare(self.connection, query)
            ibm_db.execute(stmt, params)
            
            # Para SPs que devuelven resultados, ibm_db puede requerir un fetch.
            # Si el SP devuelve un conjunto de resultados, se puede usar ibm_db.fetch_tuple.
            # Si devuelve parámetros de salida, se pueden obtener después de la ejecución.
            
            # Asumimos que el SP devuelve un conjunto de resultados o un valor.
            result = ibm_db.fetch_tuple(stmt) # Intenta obtener el primer resultado
            
            execution_time = (time.time() - start_time) * 1000  # ms
            return result, execution_time
        except Exception as e:
            if self.connection:
                ibm_db.rollback(self.connection)
            raise # Re-lanzar la excepción para que la capa superior la maneje

    def measure_time(self, operation_name, func, *args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = (time.time() - start_time) * 1000  # ms
        return result, execution_time

    def create_tables(self):
        queries = [
            """CREATE TABLE Clientes (
                cliente_id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
                nombre VARCHAR(100),
                email VARCHAR(100),
                telefono VARCHAR(20),
                direccion VARCHAR(200),
                PRIMARY KEY (cliente_id)
            );""",
            """CREATE TABLE Personal (
                personal_id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
                nombre VARCHAR(100),
                rol VARCHAR(50),
                PRIMARY KEY (personal_id)
            );""",
            """CREATE TABLE Producto (
                producto_id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
                nombre VARCHAR(100),
                precio DECIMAL(10,2),
                stock INTEGER,
                PRIMARY KEY (producto_id)
            );""",
            """CREATE TABLE Factura (
                factura_id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
                cliente_id INTEGER,
                personal_id INTEGER,
                fecha TIMESTAMP,
                total DECIMAL(10,2),
                PRIMARY KEY (factura_id),
                FOREIGN KEY (cliente_id) REFERENCES Clientes(cliente_id),
                FOREIGN KEY (personal_id) REFERENCES Personal(personal_id)
            );""",
            """CREATE TABLE Detalle_Factura (
                detalle_id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
                factura_id INTEGER,
                producto_id INTEGER,
                cantidad INTEGER,
                precio_unitario DECIMAL(10,2),
                subtotal DECIMAL(10,2),
                PRIMARY KEY (detalle_id),
                FOREIGN KEY (factura_id) REFERENCES Factura(factura_id),
                FOREIGN KEY (producto_id) REFERENCES Producto(producto_id)
            );"""
        ]

        for query in queries:
            try:
                print(f"DEBUG CONNECTOR: Ejecutando query de creación de tabla en DB2: {query[:100]}...")
                self.execute_query(query)
                ibm_db.commit(self.connection)
                print("DEBUG CONNECTOR: Commit realizado para creación de tabla.")
            except Exception as e:
                print(f"ERROR CONNECTOR al ejecutar query de creación de tabla en DB2: {e}")
                ibm_db.rollback(self.connection)
                print("DEBUG CONNECTOR: Rollback realizado para creación de tabla.")

    def create_stored_procedures(self):
        # DB2 SQL PL para procedimientos almacenados
        sp_query = """
        CREATE OR REPLACE PROCEDURE sp_generar_factura(
            IN p_cliente_id INT,
            IN p_personal_id INT,
            IN p_productos_json CLOB(1M)
        )
        LANGUAGE SQL
        BEGIN
            DECLARE v_factura_id INT;
            DECLARE v_total DECIMAL(10,2) DEFAULT 0;
            DECLARE v_producto_id INT;
            DECLARE v_cantidad INT;
            DECLARE v_precio_unitario DECIMAL(10,2);
            DECLARE v_subtotal DECIMAL(10,2);
            DECLARE C1 CURSOR FOR
                SELECT T.producto_id, T.cantidad
                FROM JSON_TABLE(p_productos_json, '$[*]' COLUMNS (
                    producto_id INT PATH '$.producto_id',
                    cantidad INT PATH '$.cantidad'
                )) AS T;

            -- Insertar la factura con total=0 temporalmente
            INSERT INTO Factura (cliente_id, personal_id, fecha, total)
            VALUES (p_cliente_id, p_personal_id, CURRENT TIMESTAMP, 0);

            SET v_factura_id = IDENTITY_VAL_LOCAL(); -- Obtener el ID de la factura recién insertada

            OPEN C1;
            FETCH FROM C1 INTO v_producto_id, v_cantidad;
            WHILE (SQLCODE <> 100) DO
                SELECT precio INTO v_precio_unitario
                FROM Producto
                WHERE producto_id = v_producto_id;

                IF v_precio_unitario IS NULL THEN
                    SIGNAL SQLSTATE '70001' SET MESSAGE_TEXT = 'Producto no encontrado.';
                END IF;

                SET v_subtotal = v_cantidad * v_precio_unitario;

                INSERT INTO Detalle_Factura (
                    factura_id, producto_id, cantidad, precio_unitario, subtotal
                ) VALUES (
                    v_factura_id, v_producto_id, v_cantidad,
                    v_precio_unitario, v_subtotal
                );

                SET v_total = v_total + v_subtotal;
                FETCH FROM C1 INTO v_producto_id, v_cantidad;
            END WHILE;
            CLOSE C1;

            -- Actualizar total de factura
            UPDATE Factura
            SET total = v_total
            WHERE factura_id = v_factura_id;

            -- Devolver el ID de la factura y el total (DB2 no devuelve fácilmente un conjunto de resultados de un SP así)
            -- Para devolver resultados, se usaría un SELECT final o parámetros de salida.
            -- Aquí, asumimos que el SP solo realiza la operación y el cliente consulta después.
            -- Si se necesita un resultado, se puede usar un SELECT final y el cliente lo fetch.
            -- Por simplicidad, el SP solo realiza la operación.
            -- Si se necesita devolver valores, se usarían parámetros OUT.
            -- Por ejemplo:
            -- OUT o_factura_id INT, OUT o_total DECIMAL(10,2)
            -- SET o_factura_id = v_factura_id;
            -- SET o_total = v_total;
            -- Para este caso, el SP de PostgreSQL devolvía una tabla.
            -- Adaptaremos para que devuelva un SELECT final.
            SELECT v_factura_id AS factura_id, v_total AS total FROM SYSIBM.SYSDUMMY1;
        END;
        """
        try:
            print(f"DEBUG CONNECTOR: Ejecutando query de creación de SP en DB2: {sp_query[:100]}...")
            self.execute_query(sp_query)
            ibm_db.commit(self.connection)
            print("DEBUG CONNECTOR: Commit realizado para creación de SP.")
        except Exception as e:
            print(f"ERROR CONNECTOR al crear SP en DB2: {e}")
            ibm_db.rollback(self.connection)
            print("DEBUG CONNECTOR: Rollback realizado para creación de SP.")

    def generate_test_data(self):
        num_records = 500
        clientes_data = []
        personal_data = []
        productos_data = []

        for i in range(1, num_records + 1):
            clientes_data.append((f'Cliente {i}', f'cliente{i}@example.com', f'111-222-{i:04d}', f'Dir {i}'))
            personal_data.append((f'Vendedor {i}', 'Vendedor'))
            productos_data.append((f'Producto {i}', 10.00 + i * 0.5, 100 + i))

        try:
            if self.is_table_empty("Clientes"):
                print("DEBUG CONNECTOR: Insertando datos de prueba para Clientes.")
                stmt = ibm_db.prepare(self.connection, "INSERT INTO Clientes (nombre, email, telefono, direccion) VALUES (?, ?, ?, ?)")
                for data_row in clientes_data:
                    ibm_db.execute(stmt, data_row)
            else:
                print("DEBUG CONNECTOR: La tabla Clientes no está vacía, omitiendo inserción de datos de prueba.")

            if self.is_table_empty("Personal"):
                print("DEBUG CONNECTOR: Insertando datos de prueba para Personal.")
                stmt = ibm_db.prepare(self.connection, "INSERT INTO Personal (nombre, rol) VALUES (?, ?)")
                for data_row in personal_data:
                    ibm_db.execute(stmt, data_row)
            else:
                print("DEBUG CONNECTOR: La tabla Personal no está vacía, omitiendo inserción de datos de prueba.")

            if self.is_table_empty("Producto"):
                print("DEBUG CONNECTOR: Insertando datos de prueba para Producto.")
                stmt = ibm_db.prepare(self.connection, "INSERT INTO Producto (nombre, precio, stock) VALUES (?, ?, ?)")
                for data_row in productos_data:
                    ibm_db.execute(stmt, data_row)
            else:
                print("DEBUG CONNECTOR: La tabla Producto no está vacía, omitiendo inserción de datos de prueba.")

            ibm_db.commit(self.connection)
            print("DEBUG CONNECTOR: Commit realizado para datos de prueba (si se insertaron).")
        except Exception as e:
            print(f"ERROR CONNECTOR al generar datos de prueba en DB2: {e}")
            ibm_db.rollback(self.connection)
            print("DEBUG CONNECTOR: Rollback realizado para datos de prueba.")

    def fetch_all_records(self, table_name):
        query = f"SELECT * FROM {table_name}"
        stmt, exec_time = self.execute_query(query)
        
        columns = []
        for i in range(ibm_db.num_fields(stmt)):
            columns.append(ibm_db.field_name(stmt, i))
        
        data = []
        row = ibm_db.fetch_tuple(stmt)
        while row:
            data.append(row)
            row = ibm_db.fetch_tuple(stmt)
        
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

        columns = ', '.join(processed_data.keys())
        placeholders = ', '.join(['?' for _ in processed_data.values()])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            print(f"DEBUG CONNECTOR: Intentando INSERT en {table_name}. Query: {query}. Params: {tuple(processed_data.values())}")
            stmt = ibm_db.prepare(self.connection, query)
            ibm_db.execute(stmt, tuple(processed_data.values()))
            ibm_db.commit(self.connection)
            print(f"DEBUG CONNECTOR: Commit realizado para INSERT en {table_name}.")
        except Exception as e:
            print(f"ERROR CONNECTOR al insertar registro en {table_name}: {e}")
            ibm_db.rollback(self.connection)
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

        set_clause = ', '.join([f"{col} = ?" for col in processed_data.keys()])
        
        pk_col_map = {
            'clientes': 'cliente_id',
            'personal': 'personal_id',
            'producto': 'producto_id',
            'factura': 'factura_id',
            'detalle_factura': 'detalle_id'
        }
        pk_col = pk_col_map.get(table_name.lower(), 'id')

        query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_col} = ?"
        processed_record_id = int(record_id) if isinstance(record_id, (np.integer, np.int64)) else record_id
        params_for_query = tuple(list(processed_data.values()) + [processed_record_id])
        
        try:
            print(f"DEBUG CONNECTOR: Intentando UPDATE en {table_name} con ID {record_id}.")
            print(f"DEBUG CONNECTOR: PK Columna: {pk_col}")
            print(f"DEBUG CONNECTOR: Query de UPDATE: {query}")
            print(f"DEBUG CONNECTOR: Parámetros de UPDATE: {params_for_query}")
            stmt = ibm_db.prepare(self.connection, query)
            ibm_db.execute(stmt, params_for_query)
            ibm_db.commit(self.connection)
            print(f"DEBUG CONNECTOR: Commit realizado para UPDATE en {table_name} ID {record_id}.")
        except Exception as e:
            print(f"ERROR CONNECTOR al actualizar registro (ID: {record_id}) en {table_name}: {e}")
            ibm_db.rollback(self.connection)
            print(f"DEBUG CONNECTOR: Rollback realizado para UPDATE en {table_name} ID {record_id}.")
            raise

    def delete_record(self, table_name, record_id):
        pk_col_map = {
            'clientes': 'cliente_id',
            'personal': 'personal_id',
            'producto': 'producto_id',
            'factura': 'factura_id',
            'detalle_factura': 'detalle_id'
        }
        pk_col = pk_col_map.get(table_name.lower(), f"{table_name.lower()}_id")

        final_record_id = record_id
        if isinstance(record_id, (np.integer, np.int64)):
            final_record_id = int(record_id)

        query = f"DELETE FROM {table_name} WHERE {pk_col} = ?"
        try:
            print(f"DEBUG CONNECTOR: Intentando DELETE en {table_name} con ID {final_record_id} (Tipo: {type(final_record_id)}). Query: {query}. PK Col: {pk_col}")
            stmt = ibm_db.prepare(self.connection, query)
            ibm_db.execute(stmt, (final_record_id,))

            if ibm_db.num_rows(stmt) == 0:
                print(f"ADVERTENCIA CONNECTOR: DELETE en {table_name} con ID {final_record_id} no afectó ninguna fila. El registro podría no existir o el ID es incorrecto.")
            
            ibm_db.commit(self.connection)
            print(f"DEBUG CONNECTOR: Commit realizado para DELETE en {table_name} ID {final_record_id}. Filas afectadas: {ibm_db.num_rows(stmt)}")
        except Exception as e:
            print(f"ERROR CONNECTOR al eliminar registro (ID: {record_id}) en {table_name}: {e}")
            ibm_db.rollback(self.connection)
            print(f"DEBUG CONNECTOR: Rollback realizado para DELETE en {table_name} ID {record_id}.")
            raise

    def search_client(self, client_id: int = 1):
        query = "SELECT * FROM Clientes WHERE cliente_id = ? FETCH FIRST 1 ROW ONLY"
        stmt, exec_time = self.execute_query(query, (client_id,))
        return ibm_db.fetch_tuple(stmt), exec_time

    def search_product(self, product_id: int = 1):
        query = "SELECT * FROM Producto WHERE producto_id = ? FETCH FIRST 1 ROW ONLY"
        stmt, exec_time = self.execute_query(query, (product_id,))
        return ibm_db.fetch_tuple(stmt), exec_time

    def generate_invoice(self, client_id: int, staff_id: int, products_json_str: str):
        # Para DB2, el JSON se pasa como CLOB
        result, exec_time = self.execute_sp("sp_generar_factura", (client_id, staff_id, products_json_str))
        ibm_db.commit(self.connection)
        return result, exec_time

    def query_invoice(self, invoice_id: int = 1):
        query = "SELECT * FROM Factura WHERE factura_id = ? FETCH FIRST 1 ROW ONLY"
        stmt, exec_time = self.execute_query(query, (invoice_id,))
        return ibm_db.fetch_tuple(stmt), exec_time

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
        stmt, exec_time = self.execute_query(query)
        
        columns = []
        for i in range(ibm_db.num_fields(stmt)):
            columns.append(ibm_db.field_name(stmt, i))
        
        data = []
        row = ibm_db.fetch_tuple(stmt)
        while row:
            data.append(row)
            row = ibm_db.fetch_tuple(stmt)
        
        return data, exec_time # Devolver la lista de tuplas y el tiempo

    def is_table_empty(self, table_name):
        query = f"SELECT COUNT(*) FROM {table_name}"
        stmt, _ = self.execute_query(query)
        count = ibm_db.fetch_tuple(stmt)[0]
        return count == 0
