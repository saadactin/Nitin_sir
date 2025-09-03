import os
import sys
import uuid
import time
import threading
import logging
import subprocess
import pyodbc
import yaml
from flask import (
    Flask, render_template, request, jsonify,
    session, redirect, url_for, flash
)

# ---- Blueprints & Local Modules ----
from manage_server import manage_server_bp          # your "manage-servers" pages/API
from databases import databases_bp           # your /api/databases/<server> API
from comprehensive_logging import comprehensive_logger
from view_details_database import list_all_databases, get_database_details
from database_status import check_all_databases
from auth import init_auth, login_required

# ---------------- Flask App ----------------
app = Flask(__name__)

# Init auth AFTER app is created
init_auth(app)  # role-based authentication

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ---------------- Register Blueprints (AFTER app exists) ----------------
# You can add url_prefix if you want these under a common path, e.g. url_prefix="/api"
app.register_blueprint(manage_server_bp)
app.register_blueprint(databases_bp)   # <--- register here

# ---------------- In-memory schedule storage ----------------
schedules = {}      # job_id -> schedule info
stop_flags = {}     # job_id -> stop signal

# ---------------- Context Processor ----------------
@app.context_processor
def inject_user_role():
    """Expose current_role to all templates."""
    return {"current_role": session.get("role")}

# ---------------- Config Loader ----------------
def resolve_config_path():
    """Find db_connections.yaml from known locations."""
    candidates = [
        os.environ.get("DB_CONFIG_PATH"),
        os.path.normpath(os.path.join(os.path.dirname(__file__), "db_connections.yaml")),
        os.path.normpath(os.path.join(os.path.dirname(__file__), "config/db_connections.yaml")),
        r"C:\Nitin_sir\etl\db_connections.yaml",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            logging.debug(f"Config file found: {p}")
            return p
    raise FileNotFoundError("Could not find db_connections.yaml. Set DB_CONFIG_PATH env var.")

CONFIG_PATH = resolve_config_path()
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f) or {}

# ---------------- SQL Server Helper ----------------
def get_sql_servers_and_databases() -> dict:
    """Return dict of {server_name: [databases]} for all SQL Servers."""
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
            logging.error(f"❌ Cannot connect to {server_host}: {e}")
    return result

def schedule_job(job_id, server, database=None, interval=None, run_time=None):
    """Schedule a sync job (whole server or single database)."""

    def job_loop():
        logging.info(f"[JOB {job_id}] Started for {server}/{database or 'ALL'}")
        while not stop_flags.get(job_id, False):
            if interval:
                # Run job immediately, then wait
                if database:
                    run_sync(server, database)   # single DB sync
                else:
                    logging.info(f"[JOB {job_id}] Running whole server sync: {server}")
                    subprocess.run(
                        [sys.executable, os.path.join(os.path.dirname(__file__), "hybrid_sync.py")],
                        env={**os.environ, "SELECTED_SERVER": server},
                        capture_output=True,
                        text=True
                    )
                time.sleep(interval)

            elif run_time:
                # Wait until the next target time of day
                now = time.localtime()
                target_seconds = run_time[0] * 3600 + run_time[1] * 60
                current_seconds = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec
                sleep_time = (target_seconds - current_seconds) % (24 * 3600)
                if sleep_time > 0:
                    time.sleep(sleep_time)

                if not stop_flags.get(job_id, False):
                    if database:
                        run_sync(server, database)
                    else:
                        logging.info(f"[JOB {job_id}] Running whole server sync: {server}")
                        subprocess.run(
                            [sys.executable, os.path.join(os.path.dirname(__file__), "hybrid_sync.py")],
                            env={**os.environ, "SELECTED_SERVER": server},
                            capture_output=True,
                            text=True
                        )
        logging.info(f"[JOB {job_id}] Stopped")

    t = threading.Thread(target=job_loop, daemon=True)
    t.start()


# ---------------- Run Sync ----------------
def run_sync(server, database):
    """Execute hybrid_sync.py for a given server/database."""
    try:
        logging.info(f"[SYNC] Running sync for {server}/{database}")
        script_path = os.path.join(os.path.dirname(__file__), "hybrid_sync.py")
        env = os.environ.copy()
        env["DB_NAME"] = database
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env
        )
        logging.info(f"[SYNC] Finished {server}/{database}, rc={result.returncode}")
    except Exception as e:
        logging.error(f"[SYNC] Error running sync for {server}/{database}: {e}")

# ---------------- Manual Scheduler ----------------
def schedule_job(job_id, server, database, interval=None, run_time=None):
    """Schedule a job to sync periodically or at fixed time daily."""
    def job_loop():
        logging.info(f"[JOB {job_id}] Started for {server}/{database}")
        while not stop_flags.get(job_id, False):
            if interval:
                run_sync(server, database)
                time.sleep(interval)
            elif run_time:
                now = time.localtime()
                target_seconds = run_time[0] * 3600 + run_time[1] * 60
                current_seconds = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec
                sleep_time = (target_seconds - current_seconds) % (24 * 3600)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                if not stop_flags.get(job_id, False):
                    run_sync(server, database)
        logging.info(f"[JOB {job_id}] Stopped")

    t = threading.Thread(target=job_loop, daemon=True)
    t.start()

# ---------------- Routes ----------------
@app.route("/")
@login_required(roles=["admin", "operator", "viewer"])
def index():
    sqlservers = config.get("sqlservers", {})
    pg_conf = config.get("postgresql", {})

    db_status = {}
    for server_name, conf in sqlservers.items():
        status_dict = check_all_databases(conf)
        for db_name, status_info in status_dict.items():
            db_status[db_name] = status_info.get("status", "down")

    databases = list(db_status.keys())
    return render_template("index.html", databases=databases, db_status=db_status, pg_conf=pg_conf)

@app.route("/dashboard")
@login_required(roles=["admin", "operator", "viewer"])
def dashboard():
    data = comprehensive_logger.get_dashboard_data()
    return render_template("dashboard.html", data=data)

@app.route("/migration-control")
@login_required(roles=["admin", "operator", "viewer"])
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

@app.route("/get-sqlservers")
@login_required(roles=["admin", "operator", "viewer"])
def get_sqlservers_api():
    return jsonify(get_sql_servers_and_databases())

@app.route("/run-sync/<server_name>/<db_name>")
@login_required(roles=["admin", "operator"])
def run_sync_database(server_name, db_name):
    """Run hybrid_sync.py for a selected database manually."""
    try:
        env = os.environ.copy()
        env["SELECTED_SERVER"] = server_name
        env["SELECTED_DB"] = db_name  # Pass database name
        script_path = os.path.join(os.path.dirname(__file__), "hybrid_sync.py")
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env
        )
        status = "success" if result.returncode == 0 else "error"
        return jsonify({
            "status": status,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        })
    except Exception as e:
        logging.error(f"Error running sync for {server_name}/{db_name}: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/run-sync/<server_name>/<db_name>/<table_name>")
@login_required(roles=["admin", "operator"])
def run_sync_table(server_name, db_name, table_name):
    """Run hybrid_sync.py for a selected table manually."""
    try:
        env = os.environ.copy()
        env["SELECTED_SERVER"] = server_name
        env["SELECTED_DB"] = db_name
        env["SELECTED_TABLE"] = table_name  # Pass table name
        script_path = os.path.join(os.path.dirname(__file__), "hybrid_sync.py")
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env
        )
        status = "success" if result.returncode == 0 else "error"
        return jsonify({
            "status": status,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        })
    except Exception as e:
        logging.error(f"Error running sync for {server_name}/{db_name}/{table_name}: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route("/databases")
@login_required(roles=["admin", "operator", "viewer"])
def databases():
    dbs = list_all_databases()
    return render_template("databases.html", databases=dbs)

@app.route("/database/<db_name>")
@login_required(roles=["admin", "operator", "viewer"])
def database_details(db_name):
    tables, object_count = get_database_details(db_name)
    return render_template("tables.html", db_name=db_name, tables=tables, object_count=object_count)

# ---------------- Schedule Routes ----------------
@app.route("/schedule")
@login_required(roles=["admin", "operator"])
def schedule_page():
    servers = get_sql_servers_and_databases()  # get dict of servers→databases
    return render_template(
        "schedule.html",
        sqlservers=servers,
        schedules=schedules
    )

@app.route("/home")
def home():
    # If you want auth here too, add the decorator; redirect is fine
    return redirect(url_for("index"))

@app.route("/api/schedule/add", methods=["POST"])
@login_required(roles=["admin", "operator"])
def add_schedule():
    """Add a new schedule for whole server or single database."""
    server = request.form["server"]
    database = request.form.get("database")  # optional
    sched_type = request.form["type"]
    job_id = str(uuid.uuid4())

    stop_flags[job_id] = False

    if sched_type == "interval":
        minutes = int(request.form.get("minutes", 60))
        schedules[job_id] = {
            "id": job_id,
            "server": server,
            "database": database,
            "type": "interval",
            "details": f"Every {minutes} minutes"
        }
        schedule_job(job_id, server, database, interval=minutes * 60)

    elif sched_type == "daily":
        time_str = request.form.get("time", "02:00")
        hour, minute = map(int, time_str.split(":"))
        schedules[job_id] = {
            "id": job_id,
            "server": server,
            "database": database,
            "type": "daily",
            "details": f"Daily at {time_str}"
        }
        schedule_job(job_id, server, database, run_time=(hour, minute))

    return jsonify({"status": "ok", "job_id": job_id})
@app.route("/api/schedule/delete/<job_id>", methods=["DELETE"])
@login_required(roles=["admin", "operator"])
def delete_schedule(job_id):
    if job_id in schedules:
        stop_flags[job_id] = True
        del schedules[job_id]
        return jsonify({"status": "deleted"})
    return jsonify({"status": "not found"}), 404

# ---------------- Main ----------------
if __name__ == "__main__":
    logging.info("🚀 Starting Flask app with manual Scheduler...")
    app.run(debug=True)
