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
    elif db_type == "SQLServer": # Asumiendo que se reactivará SQLServer
        return {
            "server": host,
            "port": int(port) if port else 1433, # Asegurar que el puerto sea entero
            "database": dbname,
            "username": user,
            "password": password
            # Considerar añadir 'driver' si es necesario para SQLServerConnector
        }
    return {}
