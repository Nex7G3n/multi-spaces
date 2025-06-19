import streamlit as st
from infrastructure.adapters.out.connectors.postgres.postgres_connector import PostgreSQLConnector
from infrastructure.adapters.out.connectors.sqlserver.sqlserver_connector import SQLServerConnector
from infrastructure.adapters.out.connectors.mysql.mysql_connector import MySQLConnector
from infrastructure.adapters.out.connectors.mongodb.mongodb_connector import MongoDBConnector
from infrastructure.adapters.out.connectors.redis.redis_connector import RedisConnector
from infrastructure.adapters.out.connectors.cassandra.cassandra_connector import CassandraConnector
from infrastructure.adapters.out.persistence.repositories.db_repository import DbRepository
from infrastructure.adapters.out.persistence.utils.db_credentials_helper import get_db_credentials
from application.services.performance_service import PerformanceService
from shared.performance_data import clear_performance_data
from infrastructure.adapters.in_.ui.views.results_view import render_performance_results

CONNECTOR_MAP = {
    "PostgreSQL": PostgreSQLConnector,
    "SQLServer": SQLServerConnector,
    "MySQL": MySQLConnector,
    "MongoDB": MongoDBConnector,
    "Redis": RedisConnector,
    "Cassandra": CassandraConnector,
}

def multi_spaces_tab_view(defaults: dict):
    st.header("Multi-Spaces")
    st.write("Configure las credenciales para cada base de datos y ejecute las pruebas simultáneamente.")

    creds = {}
    for db_type, def_vals in defaults.items():
        with st.expander(db_type, expanded=False):
            host = st.text_input(f"Host {db_type}", value=def_vals.get("host", "localhost"), key=f"{db_type}_host_multi")
            port = st.text_input(f"Puerto {db_type}", value=str(def_vals.get("port", "")), key=f"{db_type}_port_multi")
            database = st.text_input(f"Base de Datos {db_type}", value=def_vals.get("database", ""), key=f"{db_type}_db_multi")
            user = st.text_input(f"Usuario {db_type}", value=def_vals.get("user", ""), key=f"{db_type}_user_multi")
            password = st.text_input(f"Contraseña {db_type}", type="password", value=def_vals.get("password", ""), key=f"{db_type}_pwd_multi")
            creds[db_type] = get_db_credentials(db_type, host, port, database, user, password)

    if st.button("Ejecutar Test en Todas"):
        clear_performance_data()
        test_operations = [
            ("Búsqueda de cliente", "search_client"),
            ("Búsqueda de producto", "search_product"),
            ("Generación de factura", "generate_invoice"),
            ("Consulta de factura", "query_invoice"),
            ("Reporte de ventas", "sales_report"),
        ]
        last_repo = None
        for db_type, db_creds in creds.items():
            connector_cls = CONNECTOR_MAP.get(db_type)
            if not connector_cls:
                st.warning(f"Conector no soportado para {db_type}")
                continue
            connector = connector_cls()
            try:
                connector.connect(**db_creds)
                repo = DbRepository(connector_instance=connector)
                last_repo = repo
                repo.create_tables()
                repo.create_stored_procedures()
                repo.generate_test_data()
                perf_service = PerformanceService(repo)
                perf_service.run_performance_tests(db_type, test_operations)
                st.success(f"Pruebas completadas en {db_type}")
            except Exception as e:
                st.error(f"Error en {db_type}: {e}")
            finally:
                try:
                    connector.disconnect()
                except Exception:
                    pass

        st.info("Pruebas finalizadas. Resultados a continuación:")
        if last_repo:
            perf_service_summary = PerformanceService(last_repo)
            render_performance_results(perf_service_summary)
