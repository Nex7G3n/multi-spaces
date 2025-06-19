import time
import json
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.io.asyncioreactor import AsyncioConnection
import pandas as pd
from infrastructure.adapters.out.connectors.base_connector import BaseConnector


class CassandraConnector(BaseConnector):
    """Simple connector for Apache Cassandra using cassandra-driver."""

    def __init__(self, db_type: str = "Cassandra"):
        super().__init__(db_type)
        self.cluster = None
        self.session = None

    def connect(self, host, database, user, password, port):
        auth_provider = PlainTextAuthProvider(username=user, password=password)
        self.cluster = Cluster(
            [host],
            port=port,
            auth_provider=auth_provider,
            connection_class=AsyncioConnection
        )
        self.session = self.cluster.connect()
        self.session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {database} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        )
        self.session.set_keyspace(database)
        self.connection = self.session
        self.cursor = self.session

    def disconnect(self):
        if self.session:
            self.session.shutdown()
            self.session = None
        if self.cluster:
            self.cluster.shutdown()
            self.cluster = None
        self.connection = None
        self.cursor = None

    def execute_query(self, query, params=None):
        start_time = time.time()
        result = self.session.execute(query, params or [])
        exec_time = (time.time() - start_time) * 1000
        return result, exec_time

    def execute_sp(self, sp_name, params):
        raise NotImplementedError("Cassandra does not support stored procedures")

    def measure_time(self, operation_name, func, *args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        exec_time = (time.time() - start) * 1000
        return result, exec_time

    def create_tables(self):
        queries = [
            """CREATE TABLE IF NOT EXISTS clientes (\n                cliente_id int PRIMARY KEY,\n                nombre text,\n                email text,\n                telefono text,\n                direccion text\n            )""",
            """CREATE TABLE IF NOT EXISTS personal (\n                personal_id int PRIMARY KEY,\n                nombre text,\n                rol text\n            )""",
            """CREATE TABLE IF NOT EXISTS producto (\n                producto_id int PRIMARY KEY,\n                nombre text,\n                precio decimal,\n                stock int\n            )""",
            """CREATE TABLE IF NOT EXISTS factura (\n                factura_id int PRIMARY KEY,\n                cliente_id int,\n                personal_id int,\n                fecha timestamp,\n                total decimal\n            )""",
            """CREATE TABLE IF NOT EXISTS detalle_factura (\n                detalle_id int PRIMARY KEY,\n                factura_id int,\n                producto_id int,\n                cantidad int,\n                precio_unitario decimal,\n                subtotal decimal\n            )""",
        ]
        for q in queries:
            self.session.execute(q)

    def create_stored_procedures(self):
        # Not applicable for Cassandra
        pass

    def generate_test_data(self, num_records_per_table: int = 10):
        for i in range(1, num_records_per_table + 1):
            self.session.execute(
                "INSERT INTO clientes (cliente_id, nombre, email, telefono, direccion) VALUES (%s, %s, %s, %s, %s)",
                (i, f"Cliente {i}", f"cliente{i}@example.com", f"111-222-{i:04d}", f"Dir {i}")
            )
            self.session.execute(
                "INSERT INTO personal (personal_id, nombre, rol) VALUES (%s, %s, %s)",
                (i, f"Personal {i}", "Vendedor")
            )
            self.session.execute(
                "INSERT INTO producto (producto_id, nombre, precio, stock) VALUES (%s, %s, %s, %s)",
                (i, f"Producto {i}", 10.0 + i, 100 + i)
            )
        # Facturas y detalles
        for i in range(1, num_records_per_table + 1):
            self.session.execute(
                "INSERT INTO factura (factura_id, cliente_id, personal_id, fecha, total) VALUES (%s, %s, %s, %s, %s)",
                (i, i, i, datetime.utcnow(), 0.0)
            )
            for j in range(1, 3):
                self.session.execute(
                    "INSERT INTO detalle_factura (detalle_id, factura_id, producto_id, cantidad, precio_unitario, subtotal) VALUES (%s, %s, %s, %s, %s, %s)",
                    (i * 100 + j, i, j, j, 10.0 + j, (10.0 + j) * j)
                )

    def fetch_all_records(self, table_name):
        rows = self.session.execute(f"SELECT * FROM {table_name}")
        return pd.DataFrame([dict(r._asdict()) for r in rows])

    def insert_record(self, table_name, data):
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.session.execute(query, tuple(data.values()))
        return True

    def update_record(self, table_name, record_id, data):
        set_clause = ", ".join([f"{k}=%s" for k in data.keys()])
        pk_col = self._pk_column(table_name)
        query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_col}=%s"
        self.session.execute(query, tuple(data.values()) + (record_id,))

    def delete_record(self, table_name, record_id):
        pk_col = self._pk_column(table_name)
        self.session.execute(f"DELETE FROM {table_name} WHERE {pk_col}=%s", (record_id,))

    def _pk_column(self, table_name: str) -> str:
        mapping = {
            "clientes": "cliente_id",
            "personal": "personal_id",
            "producto": "producto_id",
            "factura": "factura_id",
            "detalle_factura": "detalle_id",
        }
        return mapping.get(table_name.lower(), "id")

    def search_client(self, client_id: int = 1):
        return self.measure_time(
            "search_client",
            lambda cid: self.session.execute(
                "SELECT * FROM clientes WHERE cliente_id=%s", (cid,)
            ).one(),
            client_id,
        )

    def search_product(self, product_id: int = 1):
        return self.measure_time(
            "search_product",
            lambda pid: self.session.execute(
                "SELECT * FROM producto WHERE producto_id=%s", (pid,)
            ).one(),
            product_id,
        )

    def generate_invoice(self, client_id: int, staff_id: int, products_json_str: str):
        def _generate():
            products = json.loads(products_json_str)
            result = self.session.execute("SELECT MAX(factura_id) FROM factura").one()
            factura_id = (result[0] or 0) + 1
            total = 0.0
            self.session.execute(
                "INSERT INTO factura (factura_id, cliente_id, personal_id, fecha, total) VALUES (%s, %s, %s, %s, %s)",
                (factura_id, client_id, staff_id, datetime.utcnow(), 0.0)
            )
            detail_id = 0
            for item in products:
                detail_id += 1
                precio = float(item.get('precio', 10.0))
                cantidad = int(item.get('cantidad', 1))
                subtotal = precio * cantidad
                self.session.execute(
                    "INSERT INTO detalle_factura (detalle_id, factura_id, producto_id, cantidad, precio_unitario, subtotal) VALUES (%s, %s, %s, %s, %s, %s)",
                    (detail_id, factura_id, item.get('producto_id'), cantidad, precio, subtotal)
                )
                total += subtotal
            self.session.execute(
                "UPDATE factura SET total=%s WHERE factura_id=%s",
                (total, factura_id)
            )
            return {"factura_id": factura_id, "total": total}

        return self.measure_time("generate_invoice", _generate)

    def query_invoice(self, invoice_id: int = 1):
        def _query(iid):
            inv = self.session.execute(
                "SELECT * FROM factura WHERE factura_id=%s", (iid,)
            ).one()
            details = self.session.execute(
                "SELECT * FROM detalle_factura WHERE factura_id=%s", (iid,)
            )
            inv_dict = dict(inv._asdict()) if inv else None
            if inv_dict:
                inv_dict["detalles"] = [dict(r._asdict()) for r in details]
            return inv_dict

        return self.measure_time("query_invoice", _query, invoice_id)

    def sales_report(self):
        def _report():
            rows = self.session.execute("SELECT * FROM factura")
            return [dict(r._asdict()) for r in rows]

        return self.measure_time("sales_report", _report)

    def is_table_empty(self, table_name: str) -> bool:
        row = self.session.execute(f"SELECT COUNT(*) FROM {table_name}").one()
        return (row[0] if row else 0) == 0
