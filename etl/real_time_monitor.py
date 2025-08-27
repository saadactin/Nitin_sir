#!/usr/bin/env python3
"""
Real-time Migration Monitoring Dashboard
Provides live monitoring of SQL Server to PostgreSQL migration
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import threading
import time

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

from comprehensive_logging import comprehensive_logger
from flask import Flask, render_template_string, jsonify, request
import psutil

app = Flask(__name__)

# Load HTML template from external file for better structure
with open("dashboard_template.html", "r", encoding="utf-8") as f:
    DASHBOARD_TEMPLATE = f.read()

@app.route('/')
def dashboard():
    data = comprehensive_logger.get_dashboard_data()

    latest_run = data.get('latest_run', {})
    consistency_issues = data.get('consistency_issues', [])
    active_alerts = data.get('active_alerts', [])
    system_health = data.get('system_health', [])

    return render_template_string(DASHBOARD_TEMPLATE,
        last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        total_servers=latest_run.get('total_servers', 0),
        total_databases=latest_run.get('total_databases', 0),
        total_tables=latest_run.get('total_tables', 0),
        total_rows_migrated=latest_run.get('total_rows_inserted', 0),
        successful_syncs=latest_run.get('successful_syncs', 0),
        failed_syncs=latest_run.get('failed_syncs', 0),
        avg_consistency_score=latest_run.get('avg_consistency_score', 95.0),
        avg_sync_duration=latest_run.get('avg_sync_duration', 30.0),
        consistency_issues=consistency_issues,
        active_alerts=active_alerts,
        system_health=system_health
    )

@app.route('/api/dashboard-data')
def api_dashboard_data():
    data = comprehensive_logger.get_dashboard_data()

    latest_run = data.get('latest_run', {})
    recent_runs = data.get('recent_runs', [])

    total_syncs = sum(run.get('successful_syncs', 0) + run.get('failed_syncs', 0) for run in recent_runs)
    avg_sync_duration = latest_run.get('avg_sync_duration', 30.0)
    avg_consistency_score = latest_run.get('avg_consistency_score', 95.0)

    return jsonify({
        'total_servers': latest_run.get('total_servers', 0),
        'total_databases': latest_run.get('total_databases', 0),
        'total_tables': latest_run.get('total_tables', 0),
        'total_rows_migrated': latest_run.get('total_rows_inserted', 0),
        'successful_syncs': latest_run.get('successful_syncs', 0),
        'failed_syncs': latest_run.get('failed_syncs', 0),
        'avg_consistency_score': avg_consistency_score,
        'avg_sync_duration': avg_sync_duration,
        'consistency_issues': data.get('consistency_issues', []),
        'active_alerts': data.get('active_alerts', []),
        'system_health': data.get('system_health', []),
        'recent_runs': recent_runs
    })

@app.route('/api/health')
def health_check():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

def start_monitor(host='localhost', port=5000, debug=False):
    print(f"\U0001F680 Starting Real-time Migration Monitor...")
    print(f"\U0001F4CA Dashboard will be available at: http://{host}:{port}")
    print(f"\U0001F50D API endpoint: http://{host}:{port}/api/dashboard-data")
    print(f"\U0001F49A Health check: http://{host}:{port}/api/health")
    print(f"⏹️  Press Ctrl+C to stop the monitor")

    try:
        app.run(host=host, port=port, debug=debug)
    except KeyboardInterrupt:
        print("\n🛑 Stopping monitor...")
        print("✅ Monitor stopped")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Real-time Migration Monitor')
    parser.add_argument('--host', default='localhost', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')

    args = parser.parse_args()

    start_monitor(host=args.host, port=args.port, debug=args.debug)
