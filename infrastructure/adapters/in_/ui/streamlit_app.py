import streamlit as st
import json
from pathlib import Path
from infrastructure.adapters.out.connectors.postgres.postgres_connector import PostgreSQLConnector
from infrastructure.adapters.out.connectors.sqlserver.sqlserver_connector import SQLServerConnector
from infrastructure.adapters.out.connectors.mysql.mysql_connector import MySQLConnector
from infrastructure.adapters.out.connectors.mongodb.mongodb_connector import MongoDBConnector
from infrastructure.adapters.out.connectors.redis.redis_connector import RedisConnector
from infrastructure.adapters.out.connectors.cassandra.cassandra_connector import CassandraConnector
from infrastructure.adapters.out.persistence.repositories.db_repository import DbRepository
from infrastructure.adapters.out.persistence.utils.db_credentials_helper import get_db_credentials
from application.services.entity_service import EntityService
from application.services.performance_service import PerformanceService
from application.services.billing_service import BillingService
from infrastructure.adapters.in_.ui.views.maintainers_view import maintainers_tab_view
from infrastructure.adapters.in_.ui.views.performance_view import performance_test_view
from infrastructure.adapters.in_.ui.views.results_view import results_tab_view
from infrastructure.adapters.in_.ui.views.billing_view import billing_tab_view

st.set_page_config(page_title="Comparación de Bases de Datos", layout="wide")

st.sidebar.write(f"Streamlit Version: {st.__version__}")

# Cargar credenciales por defecto desde el archivo JSON en la raíz del proyecto
DEFAULT_CREDENTIALS_FILE = Path(__file__).resolve().parents[4] / "default_db_credentials.json"
with open(DEFAULT_CREDENTIALS_FILE, "r") as f:
    DB_DEFAULTS = json.load(f)

AVAILABLE_DB_TYPES = tuple(DB_DEFAULTS.keys())

try:
    # Asegurarse de que todos los conectores se puedan importar
    from infrastructure.adapters.out.connectors.postgres.postgres_connector import PostgreSQLConnector
    from infrastructure.adapters.out.connectors.sqlserver.sqlserver_connector import SQLServerConnector
    from infrastructure.adapters.out.connectors.mysql.mysql_connector import MySQLConnector
    from infrastructure.adapters.out.connectors.mongodb.mongodb_connector import MongoDBConnector
    from infrastructure.adapters.out.connectors.redis.redis_connector import RedisConnector
    from infrastructure.adapters.out.connectors.cassandra.cassandra_connector import CassandraConnector
except ImportError as e:
    st.error(f"Error crítico al importar conectores: {e}. Verifique la configuración de PYTHONPATH y la estructura del proyecto.")
    st.stop()



def initialize_services(db_connector_instance):
    """Inicializa y devuelve los servicios de aplicación."""
    repository = DbRepository(connector_instance=db_connector_instance)
    
    entity_service = EntityService(repository)
    performance_service = PerformanceService(repository)
    billing_service = BillingService(repository)
    
    return entity_service, performance_service, billing_service, repository

def run_app():
    st.title("Sistema de Comparación de Bases de Datos para Facturación")

    if 'db_type_selected' not in st.session_state:
        st.session_state.db_type_selected = None
    if 'credentials' not in st.session_state:
        st.session_state.credentials = None
    if 'db_connector_instance' not in st.session_state:
        st.session_state.db_connector_instance = None
    if 'repository' not in st.session_state:
        st.session_state.repository = None
    if 'entity_service' not in st.session_state:
        st.session_state.entity_service = None
    if 'performance_service' not in st.session_state:
        st.session_state.performance_service = None
    if 'billing_service' not in st.session_state:
        st.session_state.billing_service = None
    if 'db_initialized_schema' not in st.session_state:
        st.session_state.db_initialized_schema = False
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "Mantenedores" # Pestaña por defecto

    st.sidebar.header("Configuración de Conexión")
    selected_db_type_sidebar = st.sidebar.selectbox(
        "Seleccione el tipo de Base de Datos",
        AVAILABLE_DB_TYPES,
        key="db_type_selector_sidebar",
        index=0 if not st.session_state.db_type_selected else AVAILABLE_DB_TYPES.index(st.session_state.db_type_selected)
    )

    st.sidebar.subheader(f"Credenciales para {selected_db_type_sidebar}")
    db_host = st.sidebar.text_input("Host", value=st.session_state.credentials.get("host", "localhost") if st.session_state.credentials else "localhost")
    
    # Obtener valores por defecto desde el archivo de configuración
    defaults = DB_DEFAULTS.get(selected_db_type_sidebar, DB_DEFAULTS.get("PostgreSQL"))
    default_port = str(defaults.get("port", 5432))
    db_port = st.sidebar.text_input("Puerto", value=str(st.session_state.credentials.get("port", default_port) if st.session_state.credentials else default_port))

    default_dbname = defaults.get("database", "postgres")
    db_name = st.sidebar.text_input("Base de Datos", value=st.session_state.credentials.get("database", default_dbname) if st.session_state.credentials else default_dbname)

    default_user = defaults.get("user", "postgres")
    user_field_in_creds = defaults.get("user_field", "user")
    db_user = st.sidebar.text_input("Usuario", value=st.session_state.credentials.get(user_field_in_creds, default_user) if st.session_state.credentials else default_user)
    
    default_password = defaults.get("password", "")
    db_password = st.sidebar.text_input("Contraseña", type="password", value=st.session_state.credentials.get("password", default_password) if st.session_state.credentials else default_password)

    if st.sidebar.button("Conectar y Configurar Base de Datos"):
        st.session_state.db_type_selected = selected_db_type_sidebar
        st.session_state.credentials = get_db_credentials(
            selected_db_type_sidebar, db_host, db_port, db_name, db_user, db_password
        )

        connector_instance_to_use = None
        if selected_db_type_sidebar == "PostgreSQL":
            connector_instance_to_use = PostgreSQLConnector()
        elif selected_db_type_sidebar == "SQLServer":
            connector_instance_to_use = SQLServerConnector()
        elif selected_db_type_sidebar == "MySQL":
            connector_instance_to_use = MySQLConnector()
        elif selected_db_type_sidebar == "MongoDB":
            connector_instance_to_use = MongoDBConnector()
        elif selected_db_type_sidebar == "Redis":
            connector_instance_to_use = RedisConnector()
        elif selected_db_type_sidebar == "Cassandra":
            connector_instance_to_use = CassandraConnector()
        
        if connector_instance_to_use:
            st.session_state.db_connector_instance = connector_instance_to_use
            try:
                st.session_state.db_connector_instance.connect(**st.session_state.credentials)
                st.sidebar.success(f"Conectado exitosamente a {selected_db_type_sidebar}!")

                services_tuple = initialize_services(st.session_state.db_connector_instance)
                st.session_state.entity_service = services_tuple[0]
                st.session_state.performance_service = services_tuple[1]
                st.session_state.billing_service = services_tuple[2]
                st.session_state.repository = services_tuple[3]

                if not st.session_state.db_initialized_schema:
                     with st.spinner(f"Realizando migraciones y generando datos iniciales en {selected_db_type_sidebar}..."):
                        st.session_state.repository.create_tables()
                        st.session_state.repository.create_stored_procedures()
                        st.session_state.repository.generate_test_data()
                     st.session_state.db_initialized_schema = True
                     st.sidebar.info("Migraciones y datos iniciales completados.")
                else:
                     st.sidebar.info("Las tablas y datos ya fueron inicializados en una sesión anterior o para esta BD.")

            except Exception as e:
                st.sidebar.error(f"Error en conexión o configuración: {str(e)}")
                st.session_state.db_connector_instance = None
                st.session_state.repository = None 
                st.session_state.db_initialized_schema = False
            finally:
                pass
        else:
            st.sidebar.error("Tipo de base de datos no soportado o conector no disponible.")

    if not st.session_state.db_connector_instance or not st.session_state.repository:
        st.warning("Por favor, configure y conecte a una base de datos usando la barra lateral.")
        return

    if not all([st.session_state.entity_service, st.session_state.performance_service, st.session_state.billing_service]):
        st.error("Los servicios de aplicación no se inicializaron correctamente. Intente reconectar.")
        return

    tab_mantenedores, tab_pruebas, tab_resultados, tab_facturacion = st.tabs([
        "Mantenedores",
        "Ejecutar Pruebas de Rendimiento",
        "Resultados y Estadísticas",
        "Proceso de Facturación"
    ])

    with tab_mantenedores:
        maintainers_tab_view(st.session_state.entity_service)

    with tab_pruebas:
        performance_test_view(st.session_state.performance_service, st.session_state.db_type_selected)

    with tab_resultados:
        results_tab_view(st.session_state.performance_service)

    with tab_facturacion:
        billing_tab_view(
            st.session_state.billing_service,
            st.session_state.entity_service,
            list(AVAILABLE_DB_TYPES),
            st.session_state.db_type_selected
        )
