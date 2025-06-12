import time
from datetime import datetime, date
import json
import pandas as pd
import numpy as np
from infrastructure.adapters.out.connectors.base_connector import BaseConnector

class MySQLConnector(BaseConnector):
    def __init__(self, db_type="MySQL"):
        super().__init__(db_type)

    def connect(self, host, database, user, password, port):
        try:
            import mysql.connector
        except ImportError:
            raise RuntimeError("El conector 'mysql.connector' no está instalado. Por favor, instala 'mysql-connector-python' (`pip install mysql-connector-python`).")
        
        self.connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
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
            param_placeholders = ', '.join(['%s' for _ in params])
            query = f"CALL {sp_name}({param_placeholders})"
            
            self.cursor.execute(query, params)

            result = None
            if self.cursor.description:
                result = self.cursor.fetchone()

            # Consumir posibles resultados adicionales para evitar
            # "Commands out of sync" en conexiones MySQL
            while self.cursor.nextset():
                pass

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
                cliente_id INT AUTO_INCREMENT PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(100),
                telefono VARCHAR(20),
                direccion VARCHAR(200)
            );""",
            """CREATE TABLE IF NOT EXISTS Personal (
                personal_id INT AUTO_INCREMENT PRIMARY KEY,
                nombre VARCHAR(100),
                rol VARCHAR(50)
            );""",
            """CREATE TABLE IF NOT EXISTS Producto (
                producto_id INT AUTO_INCREMENT PRIMARY KEY,
                nombre VARCHAR(100),
                precio DECIMAL(10,2),
                stock INT
            );""",
            """CREATE TABLE IF NOT EXISTS Factura (
                factura_id INT AUTO_INCREMENT PRIMARY KEY,
                cliente_id INT,
                personal_id INT,
                fecha DATETIME,
                total DECIMAL(10,2),
                FOREIGN KEY (cliente_id) REFERENCES Clientes(cliente_id),
                FOREIGN KEY (personal_id) REFERENCES Personal(personal_id)
            );""",
            """CREATE TABLE IF NOT EXISTS Detalle_Factura (
                detalle_id INT AUTO_INCREMENT PRIMARY KEY,
                factura_id INT,
                producto_id INT,
                cantidad INT,
                precio_unitario DECIMAL(10,2),
                subtotal DECIMAL(10,2),
                FOREIGN KEY (factura_id) REFERENCES Factura(factura_id),
                FOREIGN KEY (producto_id) REFERENCES Producto(producto_id)
            );"""
        ]

        for query in queries:
            try:
                print(f"DEBUG CONNECTOR: Ejecutando query de creación de tabla (IF NOT EXISTS): {query[:100]}...")
                self.execute_query(query)
                self.connection.commit()
                print("DEBUG CONNECTOR: Commit realizado para creación de tabla.")
            except Exception as e:
                print(f"ERROR CONNECTOR al ejecutar query de creación de tabla en MySQL: {e}")
                self.connection.rollback()
                print("DEBUG CONNECTOR: Rollback realizado para creación de tabla.")

    def create_stored_procedures(self):
        sp_query = """
        DROP PROCEDURE IF EXISTS sp_generar_factura;
        DELIMITER //
        CREATE PROCEDURE sp_generar_factura(
            IN p_cliente_id INT,
            IN p_personal_id INT,
            IN p_productos_json JSON
        )
        BEGIN
            DECLARE v_factura_id INT;
            DECLARE v_total DECIMAL(10,2) DEFAULT 0;
            DECLARE i INT DEFAULT 0;
            DECLARE num_productos INT;
            DECLARE current_producto_id INT;
            DECLARE current_cantidad INT;
            DECLARE v_precio_unitario DECIMAL(10,2);
            DECLARE v_subtotal DECIMAL(10,2);

            -- Insertar la factura con total=0 temporalmente
            INSERT INTO Factura (cliente_id, personal_id, fecha, total)
            VALUES (p_cliente_id, p_personal_id, NOW(), 0);

            SET v_factura_id = LAST_INSERT_ID(); -- Obtener el ID de la factura recién insertada

            SET num_productos = JSON_LENGTH(p_productos_json);

            WHILE i < num_productos DO
                SET current_producto_id = JSON_EXTRACT(p_productos_json, CONCAT('$[', i, '].producto_id'));
                SET current_cantidad = JSON_EXTRACT(p_productos_json, CONCAT('$[', i, '].cantidad'));

                SELECT precio INTO v_precio_unitario
                FROM Producto
                WHERE producto_id = current_producto_id;

                IF v_precio_unitario IS NULL THEN
                    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Producto no encontrado.';
                END IF;

                SET v_subtotal = current_cantidad * v_precio_unitario;

                INSERT INTO Detalle_Factura (
                    factura_id, producto_id, cantidad, precio_unitario, subtotal
                ) VALUES (
                    v_factura_id, current_producto_id, current_cantidad,
                    v_precio_unitario, v_subtotal
                );

                SET v_total = v_total + v_subtotal;
                SET i = i + 1;
            END WHILE;

            -- Actualizar total de factura
            UPDATE Factura
            SET total = v_total
            WHERE factura_id = v_factura_id;

            -- Devolver el ID de la factura y el total
            SELECT v_factura_id AS factura_id, v_total AS total;
        END //
        DELIMITER ;
        """
        sp_query_cleaned = sp_query.replace('DELIMITER //', '').replace('DELIMITER ;', '').strip()

        try:
            print(f"DEBUG CONNECTOR: Ejecutando query de creación de SP en MySQL: {sp_query_cleaned[:100]}...")

            for _ in self.cursor.execute(sp_query_cleaned, multi=True):
                # Consumir todos los resultados intermedios de cada sentencia para
                # prevenir errores 'Commands out of sync'
                pass

            self.connection.commit()
            print("DEBUG CONNECTOR: Commit realizado para creación de SP.")
        except Exception as e:
            print(f"ERROR CONNECTOR al crear SP en MySQL: {e}")
            self.connection.rollback()
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
            print(f"ERROR CONNECTOR al generar datos de prueba en MySQL: {e}")
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
        
        pk_col_map = {
            'clientes': 'cliente_id',
            'personal': 'personal_id',
            'producto': 'producto_id',
            'factura': 'factura_id',
            'detalle_factura': 'detalle_id'
        }
        pk_col = pk_col_map.get(table_name.lower(), 'id')

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
        result, exec_time = self.execute_sp("sp_generar_factura", (client_id, staff_id, products_json_str))
        self.connection.commit()
        return result, exec_time

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
