# backend/sql_discovery.py
import pyodbc

def get_sql_servers_and_databases() -> dict:
    servers = ['localhost', 'MYSERVER2']  # Add all servers you want to check
    result = {}

    for server in servers:
        try:
            conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};UID=sa;PWD=root;'
            with pyodbc.connect(conn_str, timeout=5) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")  # skip system DBs
                dbs = [row[0] for row in cursor.fetchall()]
                result[server] = dbs
        except Exception as e:
            result[server] = []
            print(f"Cannot connect to {server}: {e}")

    return result
