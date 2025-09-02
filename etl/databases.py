import os
import yaml
import pyodbc
import logging
from flask import Blueprint, jsonify, render_template

databases_bp = Blueprint("databases", __name__, template_folder="templates")

# ---------------- Config Loader ----------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "db_connections.yaml")

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    return {}

# ---------------- List Databases ----------------
@databases_bp.route("/databases/<server_name>")
def list_databases(server_name):
    config = load_config()
    servers = config.get("sqlservers", {})

    if server_name not in servers:
        return jsonify({"error": f"Server {server_name} not found in config"}), 404

    server_conf = servers[server_name]
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server_conf['server']};"
        f"UID={server_conf['username']};"
        f"PWD={server_conf['password']}"
    )

    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4;")
        dbs = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error fetching databases from {server_name}: {e}")
        return jsonify({"error": str(e)}), 500

    # Render a page showing the databases
    return render_template("manage_databases.html", server_name=server_name, databases=dbs)

# ---------------- List Tables of a Database ----------------
@databases_bp.route("/databases/<server_name>/<db_name>/tables")
def list_tables(server_name, db_name):
    config = load_config()
    servers = config.get("sqlservers", {})

    if server_name not in servers:
        return jsonify({"error": f"Server {server_name} not found in config"}), 404

    server_conf = servers[server_name]
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server_conf['server']};"
        f"DATABASE={db_name};"
        f"UID={server_conf['username']};"
        f"PWD={server_conf['password']}"
    )

    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error fetching tables from {db_name} on {server_name}: {e}")
        return jsonify({"error": str(e)}), 500

    return render_template("manage_tables.html", server_name=server_name, db_name=db_name, tables=tables)
