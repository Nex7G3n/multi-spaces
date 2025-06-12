import time
import json
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from infrastructure.adapters.out.connectors.base_connector import BaseConnector


class MongoDBConnector(BaseConnector):
    """Simple connector using pymongo for basic operations."""

    def __init__(self, db_type: str = "MongoDB"):
        super().__init__(db_type)
        self.client = None
        self.db = None

    def connect(self, host, database, user, password, port):
        uri = f"mongodb://{user}:{password}@{host}:{port}/"
        self.client = MongoClient(uri)
        self.connection = self.client
        self.db = self.client[database]

    def disconnect(self):
        if self.client:
            self.client.close()
            self.client = None
            self.connection = None
            self.cursor = None
            self.db = None

    # MongoDB does not use SQL queries or stored procedures
    def execute_query(self, query, params=None):  # type: ignore[override]
        raise NotImplementedError("execute_query is not supported for MongoDB")

    def execute_sp(self, sp_name, params):  # type: ignore[override]
        raise NotImplementedError("execute_sp is not supported for MongoDB")

    def measure_time(self, operation_name, func, *args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = (time.time() - start_time) * 1000
        return result, execution_time

    def create_tables(self):
        # Collections are created automatically when inserting documents
        pass

    def create_stored_procedures(self):
        # Not applicable for MongoDB
        pass

    def generate_test_data(self, num_records_per_table: int = 10):
        if self.is_table_empty("Clientes"):
            clientes = [
                {
                    "cliente_id": i,
                    "nombre": f"Cliente {i}",
                    "email": f"cliente{i}@example.com",
                    "telefono": f"111-222-{i:04d}",
                    "direccion": f"Dir {i}",
                }
                for i in range(1, num_records_per_table + 1)
            ]
            self.db["Clientes"].insert_many(clientes)

        if self.is_table_empty("Personal"):
            personal = [
                {"personal_id": i, "nombre": f"Vendedor {i}", "rol": "Vendedor"}
                for i in range(1, num_records_per_table + 1)
            ]
            self.db["Personal"].insert_many(personal)

        if self.is_table_empty("Producto"):
            productos = [
                {
                    "producto_id": i,
                    "nombre": f"Producto {i}",
                    "precio": 10.0 + i * 0.5,
                    "stock": 100 + i,
                }
                for i in range(1, num_records_per_table + 1)
            ]
            self.db["Producto"].insert_many(productos)

    def fetch_all_records(self, table_name):
        docs = list(self.db[table_name].find({}, {"_id": 0}))
        return pd.DataFrame(docs)

    def insert_record(self, table_name, data):
        result = self.db[table_name].insert_one(data)
        return result.inserted_id

    def update_record(self, table_name, record_id, data):
        pk_col_map = {
            "clientes": "cliente_id",
            "personal": "personal_id",
            "producto": "producto_id",
            "factura": "factura_id",
            "detalle_factura": "detalle_id",
        }
        pk_col = pk_col_map.get(table_name.lower(), "_id")
        self.db[table_name].update_one({pk_col: record_id}, {"$set": data})

    def delete_record(self, table_name, record_id):
        pk_col_map = {
            "clientes": "cliente_id",
            "personal": "personal_id",
            "producto": "producto_id",
            "factura": "factura_id",
            "detalle_factura": "detalle_id",
        }
        pk_col = pk_col_map.get(table_name.lower(), "_id")
        self.db[table_name].delete_one({pk_col: record_id})

    def search_client(self, client_id: int = 1):
        return self.measure_time(
            "search_client",
            lambda cid: self.db["Clientes"].find_one({"cliente_id": cid}, {"_id": 0}),
            client_id,
        )

    def search_product(self, product_id: int = 1):
        return self.measure_time(
            "search_product",
            lambda pid: self.db["Producto"].find_one({"producto_id": pid}, {"_id": 0}),
            product_id,
        )

    def generate_invoice(self, client_id: int, staff_id: int, products_json_str: str):
        def _generate():
            products = json.loads(products_json_str)
            factura_id = self.db["Factura"].count_documents({}) + 1
            total = 0.0
            self.db["Factura"].insert_one(
                {
                    "factura_id": factura_id,
                    "cliente_id": client_id,
                    "personal_id": staff_id,
                    "fecha": datetime.utcnow(),
                    "total": 0.0,
                }
            )
            for item in products:
                prod = self.db["Producto"].find_one({"producto_id": item["producto_id"]}) or {}
                precio = float(prod.get("precio", 0))
                subtotal = precio * float(item.get("cantidad", 0))
                detalle_id = self.db["Detalle_Factura"].count_documents({}) + 1
                self.db["Detalle_Factura"].insert_one(
                    {
                        "detalle_id": detalle_id,
                        "factura_id": factura_id,
                        "producto_id": item["producto_id"],
                        "cantidad": item["cantidad"],
                        "precio_unitario": precio,
                        "subtotal": subtotal,
                    }
                )
                total += subtotal
            self.db["Factura"].update_one({"factura_id": factura_id}, {"$set": {"total": total}})
            return {"factura_id": factura_id, "total": total}

        return self.measure_time("generate_invoice", _generate)

    def query_invoice(self, invoice_id: int = 1):
        return self.measure_time(
            "query_invoice",
            lambda iid: self.db["Factura"].find_one({"factura_id": iid}, {"_id": 0}),
            invoice_id,
        )

    def sales_report(self):
        def _report():
            pipeline = [
                {
                    "$lookup": {
                        "from": "Producto",
                        "localField": "producto_id",
                        "foreignField": "producto_id",
                        "as": "prod",
                    }
                },
                {"$unwind": "$prod"},
                {
                    "$group": {
                        "_id": "$prod.nombre",
                        "total_vendido": {"$sum": "$cantidad"},
                        "ingresos_totales": {"$sum": "$subtotal"},
                    }
                },
                {
                    "$project": {
                        "producto": "$_id",
                        "total_vendido": 1,
                        "ingresos_totales": 1,
                        "_id": 0,
                    }
                },
                {"$sort": {"ingresos_totales": -1}},
            ]
            return list(self.db["Detalle_Factura"].aggregate(pipeline))

        return self.measure_time("sales_report", _report)

    def is_table_empty(self, table_name):
        return self.db[table_name].count_documents({}) == 0
