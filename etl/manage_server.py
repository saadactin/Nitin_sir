import os
import sys
import yaml
import logging
import pyodbc
import subprocess
from flask import Blueprint, render_template, request, jsonify

# ---------------- Blueprint ----------------
manage_server_bp = Blueprint("manage_server", __name__, template_folder="templates")

# ---------------- Config Path ----------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "db_connections.yaml")

# ---------------- Config Helpers ----------------
def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    return {}

def save_config(config):
    with open(CONFIG_PATH, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False)
    logging.info("✅ Config saved successfully.")

# ---------------- Connection Helpers ----------------
def get_connection(server, port, username, password, db_name=None):
    """Return a live SQL Server connection."""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server},{port};UID={username};PWD={password};"
        "TrustServerCertificate=yes;Encrypt=no;"
    )
    if db_name:
        conn_str += f"Database={db_name};"
    return pyodbc.connect(conn_str, timeout=5)

def test_sql_connection(server, port, username, password):
    """Try connecting to SQL Server to validate credentials."""
    try:
        with get_connection(server, port, username, password) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
        return True, None
    except Exception as e:
        logging.error(f"❌ Connection failed to {server}:{port} → {e}")
        return False, str(e)

def list_all_databases(server_conf):
    """Return all non-system databases for a server."""
    try:
        conn = get_connection(
            server_conf["server"], server_conf["port"],
            server_conf["username"], server_conf["password"]
        )
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")  # skip master, model, msdb, tempdb
        dbs = [row[0] for row in cursor.fetchall()]
        conn.close()
        return dbs
    except Exception as e:
        logging.error(f"Error listing databases: {e}")
        return []

def get_database_details(server_conf, db_name):
    """Return tables in a database with row count."""
    try:
        conn = get_connection(
            server_conf["server"], server_conf["port"],
            server_conf["username"], server_conf["password"],
            db_name
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT t.name AS table_name, SUM(p.rows) AS row_count
            FROM sys.tables t
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            WHERE p.index_id IN (0,1)
            GROUP BY t.name
            ORDER BY t.name
        """)
        tables = [{"name": row[0], "rows": row[1]} for row in cursor.fetchall()]
        conn.close()
        return tables, None
    except Exception as e:
        logging.error(f"Error fetching tables for {db_name}: {e}")
        return [], str(e)

# ---------------- Hybrid Sync Runner ----------------
def run_hybrid_sync(server_name, db_name=None, table_name=None):
    """Run hybrid_sync.py with optional db/table context."""
    try:
        env = os.environ.copy()
        env["SELECTED_SERVER"] = server_name
        if db_name:
            env["SELECTED_DATABASE"] = db_name
        if table_name:
            env["SELECTED_TABLE"] = table_name

        script_path = os.path.join(os.path.dirname(__file__), "hybrid_sync.py")
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env
        )
        status = "success" if result.returncode == 0 else "error"
        return {
            "status": status,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        }
    except Exception as e:
        logging.error(f"Error running sync for {server_name}/{db_name}/{table_name}: {e}")
        return {"status": "error", "error": str(e)}

# ---------------- Routes ----------------

@manage_server_bp.route("/manage-servers")
def manage_servers_page():
    """Render Manage Servers page with registered servers."""
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    return render_template("manage_servers.html", sqlservers=sqlservers)

@manage_server_bp.route("/manage/servers/<server_name>")
def manage_databases(server_name):
    """Page to list databases for a server."""
    config = load_config()
    servers = config.get("sqlservers", {})
    if server_name not in servers:
        return render_template("manage_databases.html", server_name=server_name, databases=[])

    server_cfg = servers[server_name]
    databases = list_all_databases(server_cfg)
    return render_template("manage_databases.html", server_name=server_name, databases=databases)

@manage_server_bp.route("/manage/servers/<server_name>/<db_name>/tables")
def manage_tables(server_name, db_name):
    """Page to list tables for a given database (exclude views)."""
    config = load_config()
    server_cfg = config["sqlservers"].get(server_name)
    if not server_cfg:
        return jsonify({"error": "Server not found"}), 404

    try:
        conn = get_connection(
            server_cfg["server"], server_cfg["port"],
            server_cfg["username"], server_cfg["password"],
            db_name
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        logging.error(f"❌ Error fetching tables for {db_name}: {e}")
        tables = []

    return render_template("manage_tables.html", server_name=server_name, db_name=db_name, tables=tables)

# ---------------- APIs ----------------

@manage_server_bp.route("/api/sqlservers/list")
def api_sqlservers_list():
    """Return JSON of all SQL servers."""
    config = load_config()
    return jsonify(config.get("sqlservers", {}))

@manage_server_bp.route("/api/sqlservers/add", methods=["POST"])
def api_sqlservers_add():
    """Add a new SQL server and test connection."""
    data = request.json or {}
    name = data.get("name")
    server = data.get("server")
    port = data.get("port", 1433)
    username = data.get("username")
    password = data.get("password")

    if not all([name, server, username, password]):
        return jsonify({"status": "error", "error": "Missing required fields"}), 400

    ok, err = test_sql_connection(server, port, username, password)
    if not ok:
        return jsonify({"status": "error", "error": f"Connection failed: {err}"}), 400

    config = load_config()
    if "sqlservers" not in config:
        config["sqlservers"] = {}
    config["sqlservers"][name] = {
        "server": server,
        "port": port,
        "username": username,
        "password": password,
        "sync_mode": "hybrid",
        "check_new_databases": True,
        "skip_databases": []
    }
    save_config(config)

    return jsonify({"status": "ok", "message": f"Server {name} added successfully"})

@manage_server_bp.route("/api/sqlservers/delete/<server_name>", methods=["DELETE"])
def api_sqlservers_delete(server_name):
    """Delete a SQL server from config."""
    config = load_config()
    if "sqlservers" in config and server_name in config["sqlservers"]:
        del config["sqlservers"][server_name]
        save_config(config)
        return jsonify({"status": "ok", "message": f"Server {server_name} deleted"})
    return jsonify({"status": "error", "error": "Server not found"}), 404

@manage_server_bp.route("/api/databases/<server_name>")
def api_list_databases(server_name):
    """List all databases + tables for a given server."""
    config = load_config()
    server_conf = config.get("sqlservers", {}).get(server_name)
    if not server_conf:
        return jsonify([])

    try:
        dbs = list_all_databases(server_conf)
        db_details = []
        for db in dbs:
            tables, _ = get_database_details(server_conf, db)
            db_details.append({
                "name": db,
                "tables": tables
            })
        return jsonify(db_details)
    except Exception as e:
        logging.error(f"Error fetching DBs for {server_name}: {e}")
        return jsonify([])

# ---------------- Sync APIs ----------------

@manage_server_bp.route("/api/sync/server/<server_name>", methods=["POST"])
def api_sync_server(server_name):
    """Run hybrid sync for a server."""
    return jsonify(run_hybrid_sync(server_name))

@manage_server_bp.route("/api/sync/database/<server_name>/<db_name>", methods=["POST"])
def api_sync_database(server_name, db_name):
    """Run hybrid sync for a specific database."""
    return jsonify(run_hybrid_sync(server_name, db_name=db_name))

@manage_server_bp.route("/api/sync/table/<server_name>/<db_name>/<table_name>", methods=["POST"])
def api_sync_table(server_name, db_name, table_name):
    """Run hybrid sync for a specific table."""
    return jsonify(run_hybrid_sync(server_name, db_name=db_name, table_name=table_name))
