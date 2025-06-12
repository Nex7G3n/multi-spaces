import time
from datetime import datetime, date
import json
import pandas as pd
import numpy as np
from infrastructure.adapters.out.connectors.base_connector import BaseConnector

class SQLServerConnector(BaseConnector):
    def __init__(self, db_type="SQLServer"):
        super().__init__(db_type)

    def connect(self, server, database, username, password, port):
        try:
            import pyodbc
        except ImportError:
            raise RuntimeError("El conector 'pyodbc' no está instalado o el controlador ODBC para SQL Server no está configurado. Por favor, instala 'pyodbc' (`pip install pyodbc`) y asegúrate de tener el 'ODBC Driver 17 for SQL Server' instalado en tu sistema.")
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
            # f"Trusted_Connection=yes;"
        )
        self.connection = pyodbc.connect(conn_str)
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
            # SQL Server usa EXEC para procedimientos almacenados
            # Los parámetros se pasan directamente o como @param_name = ?
            # Para un SP que devuelve resultados, se puede usar SELECT * FROM OPENROWSET
            # o simplemente ejecutar y luego fetchall/fetchone.
            # Asumimos que el SP devuelve un conjunto de resultados o un valor.
            
            # Construir la llamada al SP con placeholders '?'
            param_placeholders = ', '.join(['?' for _ in params])
            query = f"EXEC {sp_name} {param_placeholders}"
            
            self.cursor.execute(query, params)
            
            # Intentar obtener el primer conjunto de resultados si el SP devuelve algo
            result = None
            if self.cursor.description: # Si hay resultados disponibles
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
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Clientes' and xtype='U')
               CREATE TABLE Clientes (
                   cliente_id INT IDENTITY(1,1) PRIMARY KEY,
                   nombre VARCHAR(100),
                   email VARCHAR(100),
                   telefono VARCHAR(20),
                   direccion VARCHAR(200)
               );""",
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Personal' and xtype='U')
               CREATE TABLE Personal (
                   personal_id INT IDENTITY(1,1) PRIMARY KEY,
                   nombre VARCHAR(100),
                   rol VARCHAR(50)
               );""",
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Producto' and xtype='U')
               CREATE TABLE Producto (
                   producto_id INT IDENTITY(1,1) PRIMARY KEY,
                   nombre VARCHAR(100),
                   precio DECIMAL(10,2),
                   stock INT
               );""",
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Factura' and xtype='U')
               CREATE TABLE Factura (
                   factura_id INT IDENTITY(1,1) PRIMARY KEY,
                   cliente_id INT FOREIGN KEY REFERENCES Clientes(cliente_id),
                   personal_id INT FOREIGN KEY REFERENCES Personal(personal_id),
                   fecha DATETIME,
                   total DECIMAL(10,2)
               );""",
            """IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Detalle_Factura' and xtype='U')
               CREATE TABLE Detalle_Factura (
                   detalle_id INT IDENTITY(1,1) PRIMARY KEY,
                   factura_id INT FOREIGN KEY REFERENCES Factura(factura_id),
                   producto_id INT FOREIGN KEY REFERENCES Producto(producto_id),
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
            except Exception as e:
                print(f"ERROR CONNECTOR al ejecutar query de creación de tabla en SQL Server: {e}")
                self.connection.rollback()
                print("DEBUG CONNECTOR: Rollback realizado para creación de tabla.")

    def create_stored_procedures(self):
        sp_query = """
        IF OBJECT_ID('sp_generar_factura', 'P') IS NOT NULL
            DROP PROCEDURE sp_generar_factura;
        GO
        CREATE PROCEDURE sp_generar_factura
            @p_cliente_id INT,
            @p_personal_id INT,
            @p_productos_json NVARCHAR(MAX)
        AS
        BEGIN
            SET NOCOUNT ON;
            DECLARE @v_factura_id INT;
            DECLARE @v_total DECIMAL(10,2) = 0;

            -- Insertar la factura con total=0 temporalmente
            INSERT INTO Factura (cliente_id, personal_id, fecha, total)
            VALUES (@p_cliente_id, @p_personal_id, GETDATE(), 0);

            SET @v_factura_id = SCOPE_IDENTITY(); -- Obtener el ID de la factura recién insertada

            -- Procesar los productos desde JSON
            INSERT INTO Detalle_Factura (factura_id, producto_id, cantidad, precio_unitario, subtotal)
            SELECT
                @v_factura_id,
                JSON_VALUE(p.value, '$.producto_id'),
                JSON_VALUE(p.value, '$.cantidad'),
                prod.precio,
                CAST(JSON_VALUE(p.value, '$.cantidad') AS DECIMAL(10,2)) * prod.precio
            FROM OPENJSON(@p_productos_json) AS p
            JOIN Producto AS prod ON JSON_VALUE(p.value, '$.producto_id') = prod.producto_id;

            -- Calcular el total de la factura
            SELECT @v_total = SUM(subtotal)
            FROM Detalle_Factura
            WHERE factura_id = @v_factura_id;

            -- Actualizar el total de la factura
            UPDATE Factura
            SET total = @v_total
            WHERE factura_id = @v_factura_id;

            -- Devolver el ID de la factura y el total
            SELECT @v_factura_id AS factura_id, @v_total AS total;
        END;
        """
        # pyodbc no soporta el comando GO. Necesitamos dividir el script.
        # Para este SP, el DROP y CREATE se pueden ejecutar por separado.
        # O, si el SP ya existe, el CREATE OR ALTER es mejor.
        # Para simplificar, ejecutaremos el DROP y luego el CREATE.
        
        # Dividir el script por GO
        commands = sp_query.replace('GO\n', 'GO\n--SPLIT--\n').split('--SPLIT--\n')
        
        for cmd in commands:
            cmd = cmd.strip()
            if cmd:
                try:
                    print(f"DEBUG CONNECTOR: Ejecutando query de creación de SP en SQL Server: {cmd[:100]}...")
                    self.execute_query(cmd)
                    self.connection.commit()
                    print("DEBUG CONNECTOR: Commit realizado para creación de SP.")
                except Exception as e:
                    print(f"ERROR CONNECTOR al crear SP en SQL Server: {e}")
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
                    "INSERT INTO Clientes (nombre, email, telefono, direccion) VALUES (?, ?, ?, ?)",
                    clientes_data
                )
            else:
                print("DEBUG CONNECTOR: La tabla Clientes no está vacía, omitiendo inserción de datos de prueba.")

            if self.is_table_empty("Personal"):
                print("DEBUG CONNECTOR: Insertando datos de prueba para Personal.")
                self.cursor.executemany(
                    "INSERT INTO Personal (nombre, rol) VALUES (?, ?)",
                    personal_data
                )
            else:
                print("DEBUG CONNECTOR: La tabla Personal no está vacía, omitiendo inserción de datos de prueba.")

            if self.is_table_empty("Producto"):
                print("DEBUG CONNECTOR: Insertando datos de prueba para Producto.")
                self.cursor.executemany(
                    "INSERT INTO Producto (nombre, precio, stock) VALUES (?, ?, ?)",
                    productos_data
                )
            else:
                print("DEBUG CONNECTOR: La tabla Producto no está vacía, omitiendo inserción de datos de prueba.")

            self.connection.commit()
            print("DEBUG CONNECTOR: Commit realizado para datos de prueba (si se insertaron).")
        except Exception as e:
            print(f"ERROR CONNECTOR al generar datos de prueba en SQL Server: {e}")
            self.connection.rollback()
            print("DEBUG CONNECTOR: Rollback realizado para datos de prueba.")

    def fetch_all_records(self, table_name):
        query = f"SELECT * FROM {table_name}"
        self.cursor.execute(query)
        columns = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        return pd.DataFrame.from_records(data, columns=columns)

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

        query = f"DELETE FROM {table_name} WHERE {pk_col} = ?"
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
        query = "SELECT * FROM Clientes WHERE cliente_id = ? LIMIT 1"
        cursor, exec_time = self.execute_query(query, (client_id,))
        return cursor.fetchone(), exec_time

    def search_product(self, product_id: int = 1):
        query = "SELECT * FROM Producto WHERE producto_id = ? LIMIT 1"
        cursor, exec_time = self.execute_query(query, (product_id,))
        return cursor.fetchone(), exec_time

    def generate_invoice(self, client_id: int, staff_id: int, products_json_str: str):
        # Para SQL Server, el JSON se pasa como NVARCHAR(MAX)
        # El SP en SQL Server usará OPENJSON para parsearlo.
        result, exec_time = self.execute_sp("sp_generar_factura", (client_id, staff_id, products_json_str))
        self.connection.commit()
        return result, exec_time

    def query_invoice(self, invoice_id: int = 1):
        query = "SELECT * FROM Factura WHERE factura_id = ? LIMIT 1"
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
