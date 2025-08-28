import os
import pyodbc
import yaml

# ---------------- Load Config ----------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "db_connections.yaml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SQL_SERVERS = config.get("sqlservers", {})

# ---------------- SQL Server Helper ----------------
def get_connection(conf, db_name=None):
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={conf['server']};UID={conf['username']};PWD={conf['password']};"
        "TrustServerCertificate=yes;"
    )
    if db_name:
        conn_str += f"Database={db_name};"
    return pyodbc.connect(conn_str, timeout=5)

def list_databases(conf):
    try:
        conn = get_connection(conf)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name 
            FROM sys.databases 
            WHERE name NOT IN ('master','tempdb','model','msdb')
            ORDER BY name
        """)
        dbs = [row[0] for row in cursor.fetchall()]
        conn.close()
        return dbs
    except Exception as e:
        print(f"Error connecting to {conf['server']}: {e}")
        return []

def get_all_servers_and_databases():
    result = {}
    for server_name, conf in SQL_SERVERS.items():
        result[server_name] = list_databases(conf)
    return result
