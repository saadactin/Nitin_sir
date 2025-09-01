import os
import sys
import uuid
import time
import threading
import logging
import subprocess
import pyodbc
import traceback  # add this at the top if you still want tracebacks logged

import yaml
from flask import Flask, render_template, request, jsonify, session

from comprehensive_logging import comprehensive_logger
from view_details_database import list_all_databases, get_database_details
from database_status import check_all_databases
from auth import init_auth, login_required
from manage_server import manage_server_bp  # Blueprint

# ---------------- Flask App ----------------
app = Flask(__name__)
init_auth(app)

# ---------------- Logging ----------------
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------- In-memory schedule storage ----------------
schedules = {}      # job_id -> schedule info
stop_flags = {}     # job_id -> stop signal
lock = threading.Lock()  # For thread-safe schedule manipulation

# ---------------- Context Processor ----------------
@app.context_processor
def inject_user_role():
    return {"current_role": session.get("role")}

# ---------------- Config Loader ----------------
def resolve_config_path():
    candidates = [
        os.environ.get("DB_CONFIG_PATH"),
        os.path.join(os.path.dirname(__file__), "../config/db_connections.yaml"),
        os.path.join(os.path.dirname(__file__), "config/db_connections.yaml"),
        r"C:\Nitin_sir\config\db_connections.yaml",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            logging.debug(f"Config file found: {p}")
            return p
    raise FileNotFoundError("Could not find db_connections.yaml. Set DB_CONFIG_PATH env var.")

CONFIG_PATH = resolve_config_path()

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f) or {}

def save_config(config):
    with open(CONFIG_PATH, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False)
    logging.info("Config saved successfully.")

# Ensure 'sqlservers' section exists
config = load_config()
if "sqlservers" not in config:
    config["sqlservers"] = {}
    save_config(config)

# ---------------- SQL Server Helper ----------------
def get_sql_servers_and_databases() -> dict:
    config = load_config()
    servers = config.get("sqlservers", {})
    result = {}
    for server_name, conf in servers.items():
        server_host = conf.get("server", "localhost")
        username = conf.get("username", "sa")
        password = conf.get("password", "root")
        try:
            conn_str = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={server_host};UID={username};PWD={password};"
                "TrustServerCertificate=yes;Encrypt=no;"
            )
            with pyodbc.connect(conn_str, timeout=5) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT name FROM sys.databases
                    WHERE name NOT IN ('master','tempdb','model','msdb')
                    ORDER BY name
                """)
                result[server_name] = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            result[server_name] = []
            logging.error(f"Cannot connect to {server_host}: {e}")
    return result

# ---------------- Run Sync ----------------
def run_sync(server, database):
    try:
        logging.info(f"[SYNC] Running sync for {server}/{database}")
        script_path = os.path.join(os.path.dirname(__file__), "hybrid_sync.py")
        env = os.environ.copy()
        env["DB_NAME"] = database
        env["SELECTED_SERVER"] = server
        subprocess.run([sys.executable, script_path], capture_output=True, text=True, env=env)
        logging.info(f"[SYNC] Finished {server}/{database}")
    except Exception as e:
        logging.error(f"[SYNC] Error running sync for {server}/{database}: {e}")

# ---------------- Manual Scheduler ----------------
def schedule_job(job_id, server, database, interval=None, run_time=None):
    def job_loop():
        logging.info(f"[JOB {job_id}] Started for {server}/{database}")
        while not stop_flags.get(job_id, False):
            if interval:
                run_sync(server, database)
                time.sleep(interval)
            elif run_time:
                now = time.localtime()
                target_seconds = run_time[0]*3600 + run_time[1]*60
                current_seconds = now.tm_hour*3600 + now.tm_min*60 + now.tm_sec
                sleep_time = (target_seconds - current_seconds) % (24*3600)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                if not stop_flags.get(job_id, False):
                    run_sync(server, database)
        logging.info(f"[JOB {job_id}] Stopped")
    threading.Thread(target=job_loop, daemon=True).start()

# ---------------- Register Blueprint ----------------
app.register_blueprint(manage_server_bp)

# ---------------- Routes ----------------

@app.route("/")
@login_required(roles=["admin","operator","viewer"])
def index():
    sqlservers = get_sql_servers_and_databases()
    pg_conf = config.get("postgresql", {})

    db_status = {}
    for server_name, db_list in sqlservers.items():
        status_dict = check_all_databases({"server": server_name, **config["sqlservers"].get(server_name, {})})
        for db_name, status_info in status_dict.items():
            db_status[db_name] = status_info.get("status", "down")

    return render_template("index.html", sqlservers=sqlservers, db_status=db_status, pg_conf=pg_conf)

# ----------- All Servers Page -----------
@app.route("/all-servers")
@login_required(roles=["admin","operator","viewer"])
def all_servers():
    config = load_config()
    sqlservers = config.get("sqlservers", {})
    return render_template("all_servers.html", sqlservers=sqlservers)

# ----------- API to list servers -----------
# ----------- API to list servers -----------
# ----------- API to list servers -----------
@app.route("/api/sqlservers/list")
@login_required(roles=["admin", "operator", "viewer"])
def list_sqlservers():
    config = load_config()
    servers = config.get("sqlservers", {})
    return jsonify(servers)


def get_sql_servers_and_databases() -> dict:
    config = load_config()
    servers = config.get("sqlservers", {})
    result = {}
    for server_name, conf in servers.items():
        server_host = conf.get("server", "localhost")
        username = conf.get("username", "sa")
        password = conf.get("password", "root")
        logging.debug(f"Trying to connect to SQL Server: {server_host}")
        try:
            conn_str = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"SERVER={server_host};UID={username};PWD={password};"
                "TrustServerCertificate=yes;Encrypt=no;"
            )
            with pyodbc.connect(conn_str, timeout=5) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT name 
                    FROM sys.databases 
                    WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')
                    ORDER BY name
                """)
                dbs = [row[0] for row in cursor.fetchall()]
                result[server_name] = dbs
        except Exception as e:
            result[server_name] = []
            logging.error(f"Cannot connect to {server_host}: {e}")
    return result


# ----------- API to list databases for a server -----------
@app.route("/api/sqlservers/<server_name>/databases", methods=["GET"])
@login_required(roles=["admin", "operator", "viewer"])
def list_sqlserver_databases(server_name):
    all_servers = get_sql_servers_and_databases()

    if server_name not in all_servers:
        return jsonify({
            "status": "error",
            "error": f"Server '{server_name}' not found in YAML"
        }), 404

    return jsonify({
        "status": "ok",
        "server": server_name,
        "databases": all_servers[server_name],
        "count": len(all_servers[server_name])
    })

# ---------------- Other Routes ----------------
@app.route("/dashboard")
@login_required(roles=["admin","operator","viewer"])
def dashboard():
    data = comprehensive_logger.get_dashboard_data()
    return render_template("dashboard.html", data=data)

@app.route("/migration-control")
@login_required(roles=["admin","operator","viewer"])
def migration_control():
    sqlservers = get_sql_servers_and_databases()
    pg_conf = {
        "host": comprehensive_logger.engine.url.host,
        "port": comprehensive_logger.engine.url.port,
        "database": comprehensive_logger.engine.url.database,
        "username": comprehensive_logger.engine.url.username
    }
    dbs = list_all_databases()
    return render_template("migration_control.html", sqlservers=sqlservers, pg_conf=pg_conf, databases=dbs)

@app.route("/run-sync/<server_name>/<db_name>")
@login_required(roles=["admin","operator"])
def run_sync_manual(server_name, db_name):
    try:
        run_sync(server_name, db_name)
        return jsonify({"status": "success"})
    except Exception as e:
        logging.error(f"Error running sync for {server_name}/{db_name}: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/database/<db_name>")
@login_required(roles=["admin","operator","viewer"])
def database_details(db_name):
    tables, object_count = get_database_details(db_name)
    return render_template("tables.html", db_name=db_name, tables=tables, object_count=object_count)

# ---------------- Schedule Routes ----------------
@app.route("/schedule")
@login_required(roles=["admin","operator"])
def schedule_page():
    sqlservers = get_sql_servers_and_databases()
    return render_template("schedule.html", sqlservers=sqlservers, schedules=list(schedules.values()))

@app.route("/api/schedule/add", methods=["POST"])
@login_required(roles=["admin","operator"])
def add_schedule():
    server = request.form["server"]
    database = request.form["database"]
    sched_type = request.form["type"]
    job_id = str(uuid.uuid4())

    with lock:
        stop_flags[job_id] = False
        if sched_type == "interval":
            minutes = int(request.form.get("minutes", 60))
            schedules[job_id] = {"id": job_id, "server": server, "database": database, "type": "interval", "details": f"Every {minutes} minutes"}
            schedule_job(job_id, server, database, interval=minutes*60)
        else:
            time_str = request.form.get("time", "02:00")
            hour, minute = map(int, time_str.split(":"))
            schedules[job_id] = {"id": job_id, "server": server, "database": database, "type": "daily", "details": f"Daily at {time_str}"}
            schedule_job(job_id, server, database, run_time=(hour, minute))

    return jsonify({"status": "ok", "job_id": job_id})

@app.route("/api/schedule/delete/<job_id>", methods=["DELETE"])
@login_required(roles=["admin","operator"])
def delete_schedule(job_id):
    with lock:
        if job_id in schedules:
            stop_flags[job_id] = True
            del schedules[job_id]
            return jsonify({"status": "deleted"})
    return jsonify({"status": "not found"}), 404

# ---------------- Main ----------------
if __name__ == "__main__":
    logging.info("Starting Flask app...")
    app.run(debug=True)
