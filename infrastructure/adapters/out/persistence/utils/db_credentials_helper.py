def get_db_credentials(db_type: str, host: str, port: str, dbname: str, user: str, password: str) -> dict:
    """
    Genera un diccionario de credenciales basado en el tipo de base de datos.
    """
    if db_type == "PostgreSQL":
        return {
            "host": host,
            "port": int(port) if port else 5432, # Asegurar que el puerto sea entero
            "database": dbname,
            "user": user,
            "password": password
        }
    elif db_type == "SQLServer":
        return {
            "server": host,
            "port": int(port) if port else 1433,
            "database": dbname,
            "username": user,
            "password": password
        }
    elif db_type == "MySQL":
        return {
            "host": host,
            "port": int(port) if port else 3306,
            "database": dbname,
            "user": user,
            "password": password
        }
    elif db_type == "DB2":
        return {
            "database": dbname,
            "host": host,
            "port": int(port) if port else 50000,
            "protocol": "TCPIP", # Protocolo común para DB2
            "uid": user,
            "pwd": password
        }
    elif db_type == "Oracle":
        return {
            "host": host,
            "port": int(port) if port else 1521,
            "service_name": dbname,
            "user": user,
            "password": password,
        }
    elif db_type == "MongoDB":
        return {
            "host": host,
            "port": int(port) if port else 27017,
            "database": dbname,
            "user": user,
            "password": password,
        }
    elif db_type == "Redis":
        return {
            "host": host,
            "port": int(port) if port else 6379, # Puerto por defecto de Redis
            "password": password,
            "database": int(dbname) if dbname else 0 # DB por defecto de Redis
        }
    return {}
