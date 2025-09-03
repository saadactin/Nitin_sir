import pyodbc

SQL_SERVERS = {
    "server1": {
        "server": "192.168.18.124",
        "username": "SA",
        "password": "root",
    }
}

def get_connection(db_name=None):
    server_info = SQL_SERVERS["server1"]
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server_info['server']};"
        f"UID={server_info['username']};"
        f"PWD={server_info['password']};"
        "TrustServerCertificate=yes;"
    )
    if db_name:
        conn_str += f"Database={db_name};"
    return pyodbc.connect(conn_str, timeout=5)


def list_all_databases():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")  # exclude system DBs
    databases = [row[0] for row in cursor.fetchall()]
    conn.close()
    return databases


def get_database_details(db_name):
    conn = get_connection(db_name)
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("SELECT t.name FROM sys.tables t ORDER BY t.name")
    tables = [row[0] for row in cursor.fetchall()]

    table_info = []
    for tbl in tables:
        # Row count
        cursor.execute(f"SELECT COUNT(*) FROM [{tbl}]")
        row_count = cursor.fetchone()[0]

        # Table size (MB)
        cursor.execute(f"""
            SELECT CAST(SUM(a.total_pages) * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb
            FROM sys.tables t
            JOIN sys.indexes i ON t.object_id = i.object_id
            JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            JOIN sys.allocation_units a ON p.partition_id = a.container_id
            WHERE t.name = '{tbl}'
        """)
        size_mb = cursor.fetchone()[0] or 0

        table_info.append({
            "table_name": tbl,
            "rows": row_count,
            "size_mb": size_mb
        })

    # Object count
    cursor.execute("SELECT COUNT(*) FROM sys.objects")
    object_count = cursor.fetchone()[0]

    conn.close()
    return table_info, object_count
