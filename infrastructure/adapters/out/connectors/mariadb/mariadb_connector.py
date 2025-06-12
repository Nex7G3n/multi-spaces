from infrastructure.adapters.out.connectors.mysql.mysql_connector import MySQLConnector

class MariaDBConnector(MySQLConnector):
    """Connector para MariaDB reutilizando la lógica de MySQLConnector."""

    def __init__(self, db_type: str = "MariaDB"):
        super().__init__(db_type)

    def connect(self, host: str, database: str, user: str, password: str, port: int):
        try:
            import mariadb
        except ImportError:
            raise RuntimeError(
                "El conector 'mariadb' no está instalado. Por favor, instala 'mariadb' (`pip install mariadb`)."
            )

        self.connection = mariadb.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )
        self.cursor = self.connection.cursor(buffered=True)
