from flask import Blueprint, render_template, request, jsonify
import mysql.connector
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import uuid

# Define Blueprint
scheduler_bp = Blueprint("scheduler", __name__, url_prefix="/schedule", template_folder="templates")

# APScheduler instance (runs in background)
scheduler = BackgroundScheduler()
scheduler.start()

# Store jobs in memory
jobs = {}

# -------- DB Utility --------
def get_mysql_databases():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",          # 👉 change to your MySQL user
        password="password"   # 👉 change to your MySQL password
    )
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    databases = [db[0] for db in cursor.fetchall()]
    cursor.close()
    conn.close()
    return databases

# -------- Dummy Sync Function --------
def run_sync_job(server, db):
    print(f"[SCHEDULED] Running sync for {server}/{db}")

# -------- Routes --------
# Main schedule page (list schedules & databases)
@scheduler_bp.route("/", methods=["GET"])
def schedule_page():
    databases = get_mysql_databases()  # ✅ dynamic DB list
    return render_template(
        "schedule.html",
        server_name="MySQL-Server",
        databases=databases,
        schedules=list(jobs.values())
    )

# Add schedule
@scheduler_bp.route("/api/add", methods=["POST"])
def add_schedule():
    server = request.form["server"]
    database = request.form["database"]
    schedule_type = request.form["type"]

    job_id = str(uuid.uuid4())

    if schedule_type == "interval":
        minutes = int(request.form.get("minutes", 60))
        trigger = IntervalTrigger(minutes=minutes)
        details = f"Every {minutes} minutes"
    else:  # cron (daily)
        time_str = request.form.get("time", "02:00")
        hour, minute = map(int, time_str.split(":"))
        trigger = CronTrigger(hour=hour, minute=minute)
        details = f"Daily at {time_str}"

    # Schedule job
    scheduler.add_job(run_sync_job, trigger, args=[server, database], id=job_id)

    # Save in memory
    jobs[job_id] = {
        "id": job_id,
        "server": server,
        "database": database,
        "type": schedule_type,
        "details": details,
    }

    return jsonify({"status": "ok", "job_id": job_id})

# Delete schedule
@scheduler_bp.route("/api/delete/<job_id>", methods=["DELETE"])
def delete_schedule(job_id):
    if job_id in jobs:
        scheduler.remove_job(job_id)
        del jobs[job_id]
        return jsonify({"status": "deleted"})
    return jsonify({"status": "not found"}), 404
