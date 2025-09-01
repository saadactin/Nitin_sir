import os
import yaml
import pyodbc
import logging
from flask import Blueprint, render_template, request, jsonify
from auth import login_required

# ---------------- Blueprint ----------------
manage_server_bp = Blueprint("manage_server", __name__, template_folder="templates")

# ---------------- Logging ----------------
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------- Config Path ----------------
CONFIG_PATH = os.environ.get("DB_CONFIG_PATH") or os.path.join(os.path.dirname(__file__), "db_connections.yaml")
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"Config file not found at {CONFIG_PATH}")

# ---------------- Helper Functions ----------------
def load_config():
    """Load YAML config."""
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f) or {}

def save_config(config):
    """Save YAML config."""
    with open(CONFIG_PATH, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False)
    logging.info("Config saved successfully.")

def test_sql_connection(server, username, password, timeout=5):
    """Test SQL Server connection and return list of databases."""
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
            databases = [row[0] for row in cursor.fetchall()]
            logging.info(f"Connection successful to {server}. Databases: {databases}")
            return {"status": "ok", "databases": databases}
    except Exception as e:
        logging.error(f"Connection failed to {server}: {e}")
        return {"status": "error", "error": str(e)}

# ---------------- Blueprint Routes ----------------
@manage_server_bp.route("/manage-servers")
@login_required(roles=["admin","operator"])
def manage_servers_page():
    """Render the Manage Servers page."""
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    return render_template("manage_servers.html", sqlservers=sqlservers)

# ---------------- API Routes ----------------
@manage_server_bp.route("/api/sqlservers/list")
@login_required(roles=["admin","operator"])
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

    # Test connection and fetch databases
    result = test_sql_connection(server, username, password)
    if result["status"] == "error":
        return jsonify({"status": "error", "error": result["error"]}), 400

    # Load existing config
    config = load_config()

    # Ensure sqlservers section exists
    if "sqlservers" not in config:
        config["sqlservers"] = {}

    # Add new server
    config["sqlservers"][name] = {
        "server": server,
        "username": username,
        "password": password,
        "sync_mode": sync_mode,
        "check_new_databases": True,
        "skip_databases": []
    }

    # Save config without overwriting other sections like postgres
    save_config(config)

    # Return status + databases
    return jsonify({"status": "ok", "databases": result.get("databases", [])})

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
