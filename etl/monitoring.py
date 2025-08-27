import os
import yaml
import pandas as pd
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, MetaData, inspect
from pathlib import Path
import json
from typing import Dict, List, Tuple, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load configuration
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/db_connections.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

pg_conf = config['postgresql']

class MigrationMonitor:
    """Comprehensive monitoring system for SQL Server to PostgreSQL migration"""
    
    def __init__(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://{pg_conf['username']}:{pg_conf['password']}@{pg_conf['host']}:{pg_conf['port']}/{pg_conf['database']}"
        )
        self.setup_monitoring_tables()
        
    def setup_monitoring_tables(self):
        """Create monitoring tables if they don't exist"""
        tables = {
            'migration_metrics': """
                CREATE TABLE IF NOT EXISTS migration_metrics (
                    id SERIAL PRIMARY KEY,
                    server_name VARCHAR(100),
                    database_name VARCHAR(100),
                    schema_name VARCHAR(100),
                    table_name VARCHAR(100),
                    sync_type VARCHAR(20),
                    source_row_count INTEGER,
                    target_row_count INTEGER,
                    rows_processed INTEGER,
                    rows_inserted INTEGER,
                    rows_updated INTEGER,
                    rows_skipped INTEGER,
                    sync_duration_seconds DECIMAL(10,2),
                    sync_status VARCHAR(20),
                    error_message TEXT,
                    sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_consistency_status VARCHAR(20),
                    data_consistency_percentage DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'sync_summary': """
                CREATE TABLE IF NOT EXISTS sync_summary (
                    id SERIAL PRIMARY KEY,
                    sync_session_id VARCHAR(50),
                    server_name VARCHAR(100),
                    database_name VARCHAR(100),
                    total_tables INTEGER,
                    successful_syncs INTEGER,
                    failed_syncs INTEGER,
                    skipped_tables INTEGER,
                    total_rows_processed INTEGER,
                    total_rows_inserted INTEGER,
                    sync_start_time TIMESTAMP,
                    sync_end_time TIMESTAMP,
                    total_duration_seconds DECIMAL(10,2),
                    overall_status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'data_consistency_checks': """
                CREATE TABLE IF NOT EXISTS data_consistency_checks (
                    id SERIAL PRIMARY KEY,
                    server_name VARCHAR(100),
                    database_name VARCHAR(100),
                    schema_name VARCHAR(100),
                    table_name VARCHAR(100),
                    source_row_count INTEGER,
                    target_row_count INTEGER,
                    missing_rows INTEGER,
                    extra_rows INTEGER,
                    consistency_percentage DECIMAL(5,2),
                    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(20),
                    details TEXT
                )
            """,
            'alerts': """
                CREATE TABLE IF NOT EXISTS alerts (
                    id SERIAL PRIMARY KEY,
                    alert_type VARCHAR(50),
                    severity VARCHAR(20),
                    server_name VARCHAR(100),
                    database_name VARCHAR(100),
                    schema_name VARCHAR(100),
                    table_name VARCHAR(100),
                    message TEXT,
                    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved BOOLEAN DEFAULT FALSE,
                    resolved_at TIMESTAMP,
                    resolved_by VARCHAR(100)
                )
            """,
            'dashboard_metrics': """
                CREATE TABLE IF NOT EXISTS dashboard_metrics (
                    id SERIAL PRIMARY KEY,
                    metric_date DATE,
                    total_servers INTEGER,
                    total_databases INTEGER,
                    total_tables INTEGER,
                    total_rows_migrated INTEGER,
                    successful_syncs INTEGER,
                    failed_syncs INTEGER,
                    data_consistency_score DECIMAL(5,2),
                    avg_sync_duration_seconds DECIMAL(10,2),
                    active_alerts INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }
        
        with self.engine.connect() as conn:
            for table_name, create_sql in tables.items():
                conn.execute(text(create_sql))
                conn.commit()
                logging.info(f"Monitoring table {table_name} created/verified")
    
    def log_sync_metric(self, server_name: str, database_name: str, schema_name: str, 
                       table_name: str, sync_type: str, source_count: int, target_count: int,
                       rows_processed: int, rows_inserted: int, duration: float, 
                       status: str, error_msg: str = None):
        """Log detailed sync metrics"""
        consistency_status = 'CONSISTENT' if source_count == target_count else 'INCONSISTENT'
        consistency_percentage = (target_count / source_count * 100) if source_count > 0 else 0
        
        query = """
        INSERT INTO migration_metrics 
        (server_name, database_name, schema_name, table_name, sync_type, source_row_count, 
         target_row_count, rows_processed, rows_inserted, sync_duration_seconds, sync_status, 
         error_message, data_consistency_status, data_consistency_percentage)
        VALUES (:server_name, :database_name, :schema_name, :table_name, :sync_type, :source_count,
                :target_count, :rows_processed, :rows_inserted, :duration, :status, :error_msg,
                :consistency_status, :consistency_percentage)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'server_name': server_name,
                'database_name': database_name,
                'schema_name': schema_name,
                'table_name': table_name,
                'sync_type': sync_type,
                'source_count': source_count,
                'target_count': target_count,
                'rows_processed': rows_processed,
                'rows_inserted': rows_inserted,
                'duration': duration,
                'status': status,
                'error_msg': error_msg,
                'consistency_status': consistency_status,
                'consistency_percentage': consistency_percentage
            })
            conn.commit()
    
    def log_sync_summary(self, session_id: str, server_name: str, database_name: str,
                        total_tables: int, successful_syncs: int, failed_syncs: int,
                        skipped_tables: int, total_rows_processed: int, total_rows_inserted: int,
                        start_time: datetime, end_time: datetime, overall_status: str):
        """Log sync session summary"""
        duration = (end_time - start_time).total_seconds()
        
        query = """
        INSERT INTO sync_summary 
        (sync_session_id, server_name, database_name, total_tables, successful_syncs, 
         failed_syncs, skipped_tables, total_rows_processed, total_rows_inserted, 
         sync_start_time, sync_end_time, total_duration_seconds, overall_status)
        VALUES (:session_id, :server_name, :database_name, :total_tables, :successful_syncs,
                :failed_syncs, :skipped_tables, :total_rows_processed, :total_rows_inserted,
                :start_time, :end_time, :duration, :overall_status)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'session_id': session_id,
                'server_name': server_name,
                'database_name': database_name,
                'total_tables': total_tables,
                'successful_syncs': successful_syncs,
                'failed_syncs': failed_syncs,
                'skipped_tables': skipped_tables,
                'total_rows_processed': total_rows_processed,
                'total_rows_inserted': total_rows_inserted,
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration,
                'overall_status': overall_status
            })
            conn.commit()
    
    def log_alert(self, alert_type: str, severity: str, server_name: str, database_name: str,
                  schema_name: str, table_name: str, message: str):
        """Log alerts for monitoring"""
        query = """
        INSERT INTO alerts (alert_type, severity, server_name, database_name, schema_name, table_name, message)
        VALUES (:alert_type, :severity, :server_name, :database_name, :schema_name, :table_name, :message)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'alert_type': alert_type,
                'severity': severity,
                'server_name': server_name,
                'database_name': database_name,
                'schema_name': schema_name,
                'table_name': table_name,
                'message': message
            })
            conn.commit()
    
    def check_data_consistency(self, server_name: str, database_name: str, schema_name: str, 
                              table_name: str, source_count: int, target_count: int):
        """Perform data consistency check and log results"""
        missing_rows = max(0, source_count - target_count)
        extra_rows = max(0, target_count - source_count)
        consistency_percentage = (target_count / source_count * 100) if source_count > 0 else 0
        
        status = 'CONSISTENT' if source_count == target_count else 'INCONSISTENT'
        details = f"Source: {source_count}, Target: {target_count}, Missing: {missing_rows}, Extra: {extra_rows}"
        
        query = """
        INSERT INTO data_consistency_checks 
        (server_name, database_name, schema_name, table_name, source_row_count, target_row_count,
         missing_rows, extra_rows, consistency_percentage, status, details)
        VALUES (:server_name, :database_name, :schema_name, :table_name, :source_count, :target_count,
                :missing_rows, :extra_rows, :consistency_percentage, :status, :details)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'server_name': server_name,
                'database_name': database_name,
                'schema_name': schema_name,
                'table_name': table_name,
                'source_count': source_count,
                'target_count': target_count,
                'missing_rows': missing_rows,
                'extra_rows': extra_rows,
                'consistency_percentage': consistency_percentage,
                'status': status,
                'details': details
            })
            conn.commit()
        
        # Log alert if inconsistent
        if status == 'INCONSISTENT':
            self.log_alert(
                'DATA_CONSISTENCY',
                'HIGH' if consistency_percentage < 95 else 'MEDIUM',
                server_name, database_name, schema_name, table_name,
                f"Data inconsistency detected: {details}"
            )
    
    def get_dashboard_data(self) -> Dict:
        """Get comprehensive dashboard data"""
        dashboard_data = {}
        
        # Overall metrics
        overall_query = """
        SELECT 
            COUNT(DISTINCT server_name) as total_servers,
            COUNT(DISTINCT database_name) as total_databases,
            COUNT(DISTINCT table_name) as total_tables,
            SUM(rows_inserted) as total_rows_migrated,
            COUNT(CASE WHEN sync_status = 'SUCCESS' THEN 1 END) as successful_syncs,
            COUNT(CASE WHEN sync_status = 'FAILED' THEN 1 END) as failed_syncs,
            AVG(sync_duration_seconds) as avg_sync_duration,
            AVG(data_consistency_percentage) as avg_consistency_score
        FROM migration_metrics 
        WHERE sync_timestamp >= CURRENT_DATE - INTERVAL '7 days'
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(overall_query))
            row = result.fetchone()
            if row:
                dashboard_data['overall'] = {
                    'total_servers': row[0] or 0,
                    'total_databases': row[1] or 0,
                    'total_tables': row[2] or 0,
                    'total_rows_migrated': row[3] or 0,
                    'successful_syncs': row[4] or 0,
                    'failed_syncs': row[5] or 0,
                    'avg_sync_duration': float(row[6] or 0),
                    'avg_consistency_score': float(row[7] or 0)
                }
        
        # Recent syncs
        recent_syncs_query = """
        SELECT server_name, database_name, table_name, sync_type, sync_status, 
               sync_timestamp, data_consistency_percentage
        FROM migration_metrics 
        ORDER BY sync_timestamp DESC 
        LIMIT 20
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(recent_syncs_query))
            dashboard_data['recent_syncs'] = [
                {
                    'server_name': row[0],
                    'database_name': row[1],
                    'table_name': row[2],
                    'sync_type': row[3],
                    'sync_status': row[4],
                    'sync_timestamp': row[5].isoformat() if row[5] else None,
                    'consistency_percentage': float(row[6] or 0)
                }
                for row in result.fetchall()
            ]
        
        # Data consistency issues
        consistency_query = """
        SELECT server_name, database_name, schema_name, table_name, 
               source_row_count, target_row_count, missing_rows, consistency_percentage
        FROM data_consistency_checks 
        WHERE status = 'INCONSISTENT' 
        ORDER BY check_timestamp DESC 
        LIMIT 10
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(consistency_query))
            dashboard_data['consistency_issues'] = [
                {
                    'server_name': row[0],
                    'database_name': row[1],
                    'schema_name': row[2],
                    'table_name': row[3],
                    'source_count': row[4],
                    'target_count': row[5],
                    'missing_rows': row[6],
                    'consistency_percentage': float(row[7] or 0)
                }
                for row in result.fetchall()
            ]
        
        # Active alerts
        alerts_query = """
        SELECT alert_type, severity, server_name, database_name, table_name, message, alert_timestamp
        FROM alerts 
        WHERE resolved = FALSE 
        ORDER BY alert_timestamp DESC 
        LIMIT 10
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(alerts_query))
            dashboard_data['active_alerts'] = [
                {
                    'alert_type': row[0],
                    'severity': row[1],
                    'server_name': row[2],
                    'database_name': row[3],
                    'table_name': row[4],
                    'message': row[5],
                    'alert_timestamp': row[6].isoformat() if row[6] else None
                }
                for row in result.fetchall()
            ]
        
        # Sync performance trends
        performance_query = """
        SELECT DATE(sync_timestamp) as sync_date,
               COUNT(*) as total_syncs,
               AVG(sync_duration_seconds) as avg_duration,
               AVG(data_consistency_percentage) as avg_consistency
        FROM migration_metrics 
        WHERE sync_timestamp >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY DATE(sync_timestamp)
        ORDER BY sync_date DESC
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(performance_query))
            dashboard_data['performance_trends'] = [
                {
                    'date': row[0].isoformat() if row[0] else None,
                    'total_syncs': row[1],
                    'avg_duration': float(row[2] or 0),
                    'avg_consistency': float(row[3] or 0)
                }
                for row in result.fetchall()
            ]
        
        return dashboard_data
    
    def generate_dashboard_report(self) -> str:
        """Generate HTML dashboard report"""
        data = self.get_dashboard_data()
        
        html_template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Migration Dashboard</title>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; }}
                .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }}
                .metric-card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .metric-value {{ font-size: 2em; font-weight: bold; color: #667eea; }}
                .metric-label {{ color: #666; margin-top: 5px; }}
                .chart-container {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 20px; }}
                .issues-table {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .alert-high {{ color: #dc3545; }}
                .alert-medium {{ color: #ffc107; }}
                .alert-low {{ color: #28a745; }}
                table {{ width: 100%; border-collapse: collapse; }}
                th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f8f9fa; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🚀 SQL Server to PostgreSQL Migration Dashboard</h1>
                    <p>Real-time monitoring and analytics</p>
                </div>
                
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="metric-value">{total_servers}</div>
                        <div class="metric-label">Total Servers</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{total_databases}</div>
                        <div class="metric-label">Total Databases</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{total_tables}</div>
                        <div class="metric-label">Total Tables</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{total_rows_migrated:,}</div>
                        <div class="metric-label">Total Rows Migrated</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{successful_syncs}</div>
                        <div class="metric-label">Successful Syncs</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{failed_syncs}</div>
                        <div class="metric-label">Failed Syncs</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{avg_consistency_score:.1f}%</div>
                        <div class="metric-label">Avg Data Consistency</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{avg_sync_duration:.1f}s</div>
                        <div class="metric-label">Avg Sync Duration</div>
                    </div>
                </div>
                
                <div class="chart-container">
                    <h3>📊 Performance Trends (Last 30 Days)</h3>
                    <canvas id="performanceChart" width="400" height="200"></canvas>
                </div>
                
                <div class="chart-container">
                    <h3>🎯 Data Consistency Issues</h3>
                    <div class="issues-table">
                        <table>
                            <thead>
                                <tr>
                                    <th>Server</th>
                                    <th>Database</th>
                                    <th>Table</th>
                                    <th>Source Count</th>
                                    <th>Target Count</th>
                                    <th>Missing Rows</th>
                                    <th>Consistency %</th>
                                </tr>
                            </thead>
                            <tbody>
                                {consistency_rows}
                            </tbody>
                        </table>
                    </div>
                </div>
                
                <div class="chart-container">
                    <h3>🚨 Active Alerts</h3>
                    <div class="issues-table">
                        <table>
                            <thead>
                                <tr>
                                    <th>Type</th>
                                    <th>Severity</th>
                                    <th>Server</th>
                                    <th>Database</th>
                                    <th>Table</th>
                                    <th>Message</th>
                                    <th>Time</th>
                                </tr>
                            </thead>
                            <tbody>
                                {alert_rows}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
            
            <script>
                // Performance Chart
                const ctx = document.getElementById('performanceChart').getContext('2d');
                new Chart(ctx, {{{{
                    type: 'line',
                    data: {{{{
                        labels: {performance_labels},
                        datasets: [
                            {{{{
                                label: 'Total Syncs',
                                data: {performance_syncs},
                                borderColor: '#667eea',
                                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                                yAxisID: 'y'
                            }}}},
                            {{{{
                                label: 'Avg Consistency %',
                                data: {performance_consistency},
                                borderColor: '#28a745',
                                backgroundColor: 'rgba(40, 167, 69, 0.1)',
                                yAxisID: 'y1'
                            }}}}
                        ]
                    }}}},
                    options: {{{{
                        responsive: true,
                        interaction: {{{{
                            mode: 'index',
                            intersect: false,
                        }}}},
                        scales: {{{{
                            y: {{{{
                                type: 'linear',
                                display: true,
                                position: 'left',
                            }}}},
                            y1: {{{{
                                type: 'linear',
                                display: true,
                                position: 'right',
                                grid: {{{{
                                    drawOnChartArea: false,
                                }}}},
                            }}}},
                        }}}},
                    }}}}
                }}}});
            </script>
        </body>
        </html>
        """
        
        # Prepare data for template
        overall = data.get('overall', {})
        consistency_issues = data.get('consistency_issues', [])
        active_alerts = data.get('active_alerts', [])
        performance_trends = data.get('performance_trends', [])
        
        # Generate table rows
        consistency_rows = ""
        for issue in consistency_issues:
            consistency_rows += f"""
            <tr>
                <td>{issue['server_name']}</td>
                <td>{issue['database_name']}</td>
                <td>{issue['table_name']}</td>
                <td>{issue['source_count']:,}</td>
                <td>{issue['target_count']:,}</td>
                <td>{issue['missing_rows']:,}</td>
                <td>{issue['consistency_percentage']:.1f}%</td>
            </tr>
            """
        
        alert_rows = ""
        for alert in active_alerts:
            severity_class = f"alert-{alert['severity'].lower()}"
            alert_rows += f"""
            <tr>
                <td>{alert['alert_type']}</td>
                <td class="{severity_class}">{alert['severity']}</td>
                <td>{alert['server_name']}</td>
                <td>{alert['database_name']}</td>
                <td>{alert['table_name']}</td>
                <td>{alert['message']}</td>
                <td>{alert['alert_timestamp']}</td>
            </tr>
            """
        
        # Prepare performance chart data
        performance_labels = [trend['date'] for trend in performance_trends]
        performance_syncs = [trend['total_syncs'] for trend in performance_trends]
        performance_consistency = [trend['avg_consistency'] for trend in performance_trends]
        
        return html_template.format(
            total_servers=overall.get('total_servers', 0),
            total_databases=overall.get('total_databases', 0),
            total_tables=overall.get('total_tables', 0),
            total_rows_migrated=overall.get('total_rows_migrated', 0),
            successful_syncs=overall.get('successful_syncs', 0),
            failed_syncs=overall.get('failed_syncs', 0),
            avg_consistency_score=overall.get('avg_consistency_score', 0),
            avg_sync_duration=overall.get('avg_sync_duration', 0),
            consistency_rows=consistency_rows,
            alert_rows=alert_rows,
            performance_labels=json.dumps(performance_labels),
            performance_syncs=json.dumps(performance_syncs),
            performance_consistency=json.dumps(performance_consistency)
        )
    
    def send_alert_email(self, subject: str, message: str, recipients: List[str]):
        """Send alert email (configure SMTP settings in config)"""
        try:
            # This would need SMTP configuration in your config file
            # For now, just log the alert
            logging.info(f"ALERT EMAIL - Subject: {subject}, Message: {message}")
        except Exception as e:
            logging.error(f"Failed to send alert email: {e}")

# Global monitor instance
monitor = MigrationMonitor() 