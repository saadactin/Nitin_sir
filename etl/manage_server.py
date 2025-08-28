# manage_server.py
import os
import yaml
import pyodbc
import logging
from flask import Blueprint, render_template, request, jsonify
from auth import login_required   # ✅ your custom auth

# ---------------- Config Path ----------------
CONFIG_PATH = os.environ.get("DB_CONFIG_PATH") or os.path.join(
    os.path.dirname(__file__), "db_connections.yaml"
)

# ---------------- Config Helpers ----------------
def load_config():
    """Load YAML config from file."""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    return {}

def save_config(config):
    """Save config back to YAML file."""
    with open(CONFIG_PATH, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False)
    logging.info("✅ Config saved successfully.")

# ---------------- Blueprint ----------------
manage_server_bp = Blueprint("manage_server", __name__, template_folder="templates")

# ---------------- Logging ----------------
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------- SQL Helper ----------------
def list_all_databases(server, username, password, timeout=5):
    """Connect to SQL Server and return list of databases."""
    try:
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={server};UID={username};PWD={password};"
            "TrustServerCertificate=yes;Encrypt=no;"
        )
        with pyodbc.connect(conn_str, timeout=timeout) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name 
                FROM sys.databases 
                WHERE name NOT IN ('master','tempdb','model','msdb')
                ORDER BY name
            """)
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"❌ Connection failed to {server}: {e}")
        return []

# ---------------- Server + DB Fetcher ----------------
def get_sql_servers_and_databases():
    """Return dict of {server_name: [databases]} from config."""
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    servers_with_dbs = {}

    for name, details in sqlservers.items():
        try:
            dbs = list_all_databases(details["server"], details["username"], details["password"])
            servers_with_dbs[name] = dbs
        except Exception as e:
            logging.error(f"⚠️ Failed to fetch DBs for {name}: {e}")
            servers_with_dbs[name] = []
    return servers_with_dbs

# ---------------- Blueprint Routes ----------------
@manage_server_bp.route("/manage-servers")
@login_required(roles=["admin", "operator"])
def manage_servers_page():
    """Render Manage Servers page with registered servers."""
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    return render_template("manage_servers.html", sqlservers=sqlservers)

@manage_server_bp.route("/all-servers")
@login_required(roles=["admin", "operator", "viewer"])
def all_servers():
    """Show all servers and their databases."""
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    servers_with_dbs = get_sql_servers_and_databases()
    return render_template("all_servers.html",
                           sqlservers=sqlservers,
                           servers_with_dbs=servers_with_dbs)

# ---------------- API Routes ----------------
@manage_server_bp.route("/api/sqlservers/list")
@login_required(roles=["admin", "operator", "viewer"])
def api_sqlservers_list():
    """Return JSON of all SQL servers."""
    config = load_config()
    return jsonify(config.get("sqlservers", {}))

@manage_server_bp.route("/api/sqlservers/add", methods=["POST"])
@login_required(roles=["admin"])
def api_sqlservers_add():
    """Add a new SQL server and return its databases."""
    data = request.json
    name = data.get("name")
    server = data.get("server")
    username = data.get("username")
    password = data.get("password")
    sync_mode = data.get("sync_mode", "hybrid")

    # Fetch DBs
    dbs = list_all_databases(server, username, password)
    if not dbs:
        return jsonify({"status": "error", "error": "Connection failed"}), 400

    # Load + update config
    config = load_config()
    if "sqlservers" not in config:
        config["sqlservers"] = {}

    config["sqlservers"][name] = {
        "server": server,
        "username": username,
        "password": password,
        "sync_mode": sync_mode,
        "check_new_databases": True,
        "skip_databases": []
    }
    save_config(config)

    return jsonify({"status": "ok", "databases": dbs})

@manage_server_bp.route("/api/sqlservers/delete/<server_name>", methods=["DELETE"])
@login_required(roles=["admin"])
def api_sqlservers_delete(server_name):
    """Delete a SQL server from config."""
    config = load_config()
    if "sqlservers" in config and server_name in config["sqlservers"]:
        del config["sqlservers"][server_name]
        save_config(config)
        return jsonify({"status": "ok"})
    return jsonify({"status": "error", "error": "Server not found"}), 404
