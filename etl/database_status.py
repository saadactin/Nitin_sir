import pyodbc

def check_all_databases(server_config):
    """
    Check the status of all user databases on a SQL Server instance.
    server_config: dict with keys: server, username, password, skip_databases (optional list)
    Returns: dict of {dbName: {'status': 'up'/'down', 'error': ...}}
    """
    result = {}
    skip_dbs = server_config.get("skip_databases", [])

    try:
        # Connect to master database to list all databases
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server_config['server']};"
            f"DATABASE=master;"
            f"UID={server_config['username']};"
            f"PWD={server_config['password']};"
            "TrustServerCertificate=yes;"
            "Encrypt=no;"
        )
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        
        # Get all user databases
        cursor.execute("""
            SELECT name 
            FROM sys.databases 
            WHERE database_id > 4
        """)
        db_list = [row[0] for row in cursor.fetchall()]
        conn.close()

        for db_name in db_list:
            if db_name in skip_dbs:
                continue
            try:
                # Check individual database
                conn_str_db = (
                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                    f"SERVER={server_config['server']};"
                    f"DATABASE={db_name};"
                    f"UID={server_config['username']};"
                    f"PWD={server_config['password']};"
                    "TrustServerCertificate=yes;"
                    "Encrypt=no;"
                )
                conn_db = pyodbc.connect(conn_str_db, timeout=5)
                cursor_db = conn_db.cursor()
                cursor_db.execute("SELECT 1")
                conn_db.close()
                result[db_name] = {"status": "up"}
            except Exception as e:
                result[db_name] = {"status": "down", "error": str(e)}

    except Exception as e:
        # If we cannot connect to the server at all
        result["server_error"] = str(e)

    return result
