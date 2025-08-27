import os
import yaml
import pandas as pd
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, MetaData, inspect
from pathlib import Path
import json
from typing import Dict, List, Tuple, Optional
import uuid
import psutil
import platform

# Load configuration
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/db_connections.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

pg_conf = config['postgresql']

class ComprehensiveLogger:
    """Comprehensive logging system for SQL Server to PostgreSQL migration"""
    
    def __init__(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://{pg_conf['username']}:{pg_conf['password']}@{pg_conf['host']}:{pg_conf['port']}/{pg_conf['database']}"
        )
        self.setup_logging_tables()
        
    def setup_logging_tables(self):
        """Create comprehensive logging tables"""
        tables = {
            'migration_runs': """
                CREATE TABLE IF NOT EXISTS migration_runs (
                    run_id VARCHAR(50) PRIMARY KEY,
                    run_type VARCHAR(20), -- 'FULL', 'INCREMENTAL', 'SCHEDULED'
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    status VARCHAR(20), -- 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'
                    total_servers INTEGER,
                    total_databases INTEGER,
                    total_tables INTEGER,
                    total_rows_processed INTEGER,
                    total_rows_inserted INTEGER,
                    successful_syncs INTEGER,
                    failed_syncs INTEGER,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'server_logs': """
                CREATE TABLE IF NOT EXISTS server_logs (
                    log_id SERIAL PRIMARY KEY,
                    run_id VARCHAR(50),
                    server_name VARCHAR(100),
                    database_name VARCHAR(100),
                    log_level VARCHAR(10), -- 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
                    log_message TEXT,
                    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    additional_data JSONB,
                    FOREIGN KEY (run_id) REFERENCES migration_runs(run_id)
                )
            """,
            'table_sync_logs': """
                CREATE TABLE IF NOT EXISTS table_sync_logs (
                    sync_id SERIAL PRIMARY KEY,
                    run_id VARCHAR(50),
                    server_name VARCHAR(100),
                    database_name VARCHAR(100),
                    schema_name VARCHAR(100),
                    table_name VARCHAR(100),
                    sync_type VARCHAR(20), -- 'FULL', 'INCREMENTAL', 'SMART_SYNC'
                    source_row_count INTEGER,
                    target_row_count INTEGER,
                    rows_processed INTEGER,
                    rows_inserted INTEGER,
                    rows_updated INTEGER,
                    rows_skipped INTEGER,
                    sync_duration_seconds DECIMAL(10,2),
                    sync_status VARCHAR(20), -- 'SUCCESS', 'FAILED', 'SKIPPED'
                    error_message TEXT,
                    sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_consistency_status VARCHAR(20),
                    data_consistency_percentage DECIMAL(5,2),
                    FOREIGN KEY (run_id) REFERENCES migration_runs(run_id)
                )
            """,
            'row_count_audit': """
                CREATE TABLE IF NOT EXISTS row_count_audit (
                    audit_id SERIAL PRIMARY KEY,
                    run_id VARCHAR(50),
                    server_name VARCHAR(100),
                    database_name VARCHAR(100),
                    schema_name VARCHAR(100),
                    table_name VARCHAR(100),
                    source_row_count INTEGER,
                    target_row_count INTEGER,
                    missing_rows INTEGER,
                    extra_rows INTEGER,
                    consistency_percentage DECIMAL(5,2),
                    audit_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    audit_status VARCHAR(20), -- 'PASS', 'FAIL', 'WARNING'
                    FOREIGN KEY (run_id) REFERENCES migration_runs(run_id)
                )
            """,
            'system_health_metrics': """
                CREATE TABLE IF NOT EXISTS system_health_metrics (
                    metric_id SERIAL PRIMARY KEY,
                    run_id VARCHAR(50),
                    metric_name VARCHAR(100),
                    metric_value DECIMAL(15,2),
                    metric_unit VARCHAR(20),
                    metric_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    server_info JSONB,
                    FOREIGN KEY (run_id) REFERENCES migration_runs(run_id)
                )
            """,
            'scheduled_jobs': """
                CREATE TABLE IF NOT EXISTS scheduled_jobs (
                    job_id SERIAL PRIMARY KEY,
                    job_name VARCHAR(100),
                    job_type VARCHAR(20), -- 'FULL_SYNC', 'INCREMENTAL_SYNC', 'VALIDATION'
                    schedule_cron VARCHAR(100),
                    is_active BOOLEAN DEFAULT TRUE,
                    last_run_id VARCHAR(50),
                    last_run_time TIMESTAMP,
                    last_run_status VARCHAR(20),
                    next_run_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (last_run_id) REFERENCES migration_runs(run_id)
                )
            """,
            'alerts': """
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id SERIAL PRIMARY KEY,
                    run_id VARCHAR(50),
                    alert_type VARCHAR(50), -- 'SYNC_FAILURE', 'DATA_CONSISTENCY', 'SYSTEM_HEALTH'
                    severity VARCHAR(20), -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
                    server_name VARCHAR(100),
                    database_name VARCHAR(100),
                    schema_name VARCHAR(100),
                    table_name VARCHAR(100),
                    message TEXT,
                    alert_message TEXT,
                    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_resolved BOOLEAN DEFAULT FALSE,
                    resolved_at TIMESTAMP,
                    resolved_by VARCHAR(100),
                    FOREIGN KEY (run_id) REFERENCES migration_runs(run_id)
                )
            """,
            'performance_metrics': """
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    metric_id SERIAL PRIMARY KEY,
                    run_id VARCHAR(50),
                    metric_category VARCHAR(50), -- 'SYNC_SPEED', 'DATA_TRANSFER', 'SYSTEM_RESOURCES'
                    metric_name VARCHAR(100),
                    metric_value DECIMAL(15,2),
                    metric_unit VARCHAR(20),
                    metric_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    context JSONB,
                    FOREIGN KEY (run_id) REFERENCES migration_runs(run_id)
                )
            """
        }
        
        with self.engine.connect() as conn:
            for table_name, create_sql in tables.items():
                conn.execute(text(create_sql))
                conn.commit()
                logging.info(f"Logging table {table_name} created/verified")
    
    def start_migration_run(self, run_type: str = 'MANUAL') -> str:
        """Start a new migration run and return run_id"""
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        query = """
        INSERT INTO migration_runs (run_id, run_type, start_time, status)
        VALUES (:run_id, :run_type, :start_time, 'RUNNING')
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'run_id': run_id,
                'run_type': run_type,
                'start_time': datetime.now()
            })
            conn.commit()
        
        logging.info(f"Started migration run: {run_id}")
        return run_id
    
    def end_migration_run(self, run_id: str, status: str, summary: Dict):
        """End a migration run with summary data"""
        query = """
        UPDATE migration_runs 
        SET end_time = :end_time, status = :status,
            total_servers = :total_servers,
            total_databases = :total_databases,
            total_tables = :total_tables,
            total_rows_processed = :total_rows_processed,
            total_rows_inserted = :total_rows_inserted,
            successful_syncs = :successful_syncs,
            failed_syncs = :failed_syncs,
            error_message = :error_message
        WHERE run_id = :run_id
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'run_id': run_id,
                'end_time': datetime.now(),
                'status': status,
                'total_servers': summary.get('total_servers', 0),
                'total_databases': summary.get('total_databases', 0),
                'total_tables': summary.get('total_tables', 0),
                'total_rows_processed': summary.get('total_rows_processed', 0),
                'total_rows_inserted': summary.get('total_rows_inserted', 0),
                'successful_syncs': summary.get('successful_syncs', 0),
                'failed_syncs': summary.get('failed_syncs', 0),
                'error_message': summary.get('error_message', None)
            })
            conn.commit()
        
        logging.info(f"Ended migration run: {run_id} with status: {status}")
    
    def log_server_event(self, run_id: str, server_name: str, database_name: str, 
                        log_level: str, message: str, additional_data: Dict = None):
        """Log server-level events"""
        query = """
        INSERT INTO server_logs (run_id, server_name, database_name, log_level, log_message, additional_data)
        VALUES (:run_id, :server_name, :database_name, :log_level, :message, :additional_data)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'run_id': run_id,
                'server_name': server_name,
                'database_name': database_name,
                'log_level': log_level,
                'message': message,
                'additional_data': json.dumps(additional_data) if additional_data else None
            })
            conn.commit()
    
    def log_table_sync(self, run_id: str, server_name: str, database_name: str, schema_name: str,
                      table_name: str, sync_type: str, source_count: int, target_count: int,
                      rows_processed: int, rows_inserted: int, duration: float, status: str,
                      error_msg: str = None, consistency_percentage: float = None):
        """Log table sync details"""
        query = """
        INSERT INTO table_sync_logs 
        (run_id, server_name, database_name, schema_name, table_name, sync_type,
         source_row_count, target_row_count, rows_processed, rows_inserted, sync_duration_seconds,
         sync_status, error_message, data_consistency_percentage)
        VALUES (:run_id, :server_name, :database_name, :schema_name, :table_name, :sync_type,
                :source_count, :target_count, :rows_processed, :rows_inserted, :duration,
                :status, :error_msg, :consistency_percentage)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'run_id': run_id,
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
                'consistency_percentage': consistency_percentage
            })
            conn.commit()
    
    def log_row_count_audit(self, run_id: str, server_name: str, database_name: str, schema_name: str,
                           table_name: str, source_count: int, target_count: int):
        """Log row count audit data"""
        missing_rows = max(0, source_count - target_count)
        extra_rows = max(0, target_count - source_count)
        consistency_percentage = (target_count / source_count * 100) if source_count > 0 else 0
        
        audit_status = 'PASS' if source_count == target_count else 'FAIL' if consistency_percentage < 95 else 'WARNING'
        
        query = """
        INSERT INTO row_count_audit 
        (run_id, server_name, database_name, schema_name, table_name,
         source_row_count, target_row_count, missing_rows, extra_rows,
         consistency_percentage, audit_status)
        VALUES (:run_id, :server_name, :database_name, :schema_name, :table_name,
                :source_count, :target_count, :missing_rows, :extra_rows,
                :consistency_percentage, :audit_status)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'run_id': run_id,
                'server_name': server_name,
                'database_name': database_name,
                'schema_name': schema_name,
                'table_name': table_name,
                'source_count': source_count,
                'target_count': target_count,
                'missing_rows': missing_rows,
                'extra_rows': extra_rows,
                'consistency_percentage': consistency_percentage,
                'audit_status': audit_status
            })
            conn.commit()
    
    def log_system_health(self, run_id: str):
        """Log system health metrics"""
        # CPU Usage
        cpu_percent = psutil.cpu_percent(interval=1)
        self._log_metric(run_id, 'system_health_metrics', 'CPU_USAGE', cpu_percent, 'PERCENT')
        
        # Memory Usage
        memory = psutil.virtual_memory()
        self._log_metric(run_id, 'system_health_metrics', 'MEMORY_USAGE', memory.percent, 'PERCENT')
        self._log_metric(run_id, 'system_health_metrics', 'MEMORY_AVAILABLE', memory.available / (1024**3), 'GB')
        
        # Disk Usage
        disk = psutil.disk_usage('/')
        self._log_metric(run_id, 'system_health_metrics', 'DISK_USAGE', disk.percent, 'PERCENT')
        self._log_metric(run_id, 'system_health_metrics', 'DISK_FREE', disk.free / (1024**3), 'GB')
        
        # System Info
        server_info = {
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'processor': platform.processor(),
            'hostname': platform.node()
        }
        
        query = """
        INSERT INTO system_health_metrics (run_id, metric_name, metric_value, metric_unit, server_info)
        VALUES (:run_id, 'SYSTEM_INFO', 0, 'INFO', :server_info)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'run_id': run_id,
                'server_info': json.dumps(server_info)
            })
            conn.commit()
    
    def _log_metric(self, run_id: str, table: str, metric_name: str, value: float, unit: str):
        """Helper method to log metrics"""
        query = f"""
        INSERT INTO {table} (run_id, metric_name, metric_value, metric_unit)
        VALUES (:run_id, :metric_name, :value, :unit)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'run_id': run_id,
                'metric_name': metric_name,
                'value': value,
                'unit': unit
            })
            conn.commit()
    
    def log_alert(self, run_id: str, alert_type: str, severity: str, server_name: str,
                  database_name: str, schema_name: str, table_name: str, message: str):
        """Log alerts"""
        query = """
        INSERT INTO alerts (run_id, alert_type, severity, server_name, database_name, 
                           schema_name, table_name, alert_message)
        VALUES (:run_id, :alert_type, :severity, :server_name, :database_name, 
                :schema_name, :table_name, :message)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'run_id': run_id,
                'alert_type': alert_type,
                'severity': severity,
                'server_name': server_name,
                'database_name': database_name,
                'schema_name': schema_name,
                'table_name': table_name,
                'message': message
            })
            conn.commit()
    
    def log_performance_metric(self, run_id: str, category: str, metric_name: str, 
                              value: float, unit: str, context: Dict = None):
        """Log performance metrics"""
        query = """
        INSERT INTO performance_metrics (run_id, metric_category, metric_name, metric_value, metric_unit, context)
        VALUES (:run_id, :category, :metric_name, :value, :unit, :context)
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(query), {
                'run_id': run_id,
                'category': category,
                'metric_name': metric_name,
                'value': value,
                'unit': unit,
                'context': json.dumps(context) if context else None
            })
            conn.commit()
    
    def get_dashboard_data(self) -> Dict:
        """Get comprehensive dashboard data from all logging tables"""
        data = {}
        
        # Get latest run summary
        latest_run_query = """
        SELECT * FROM migration_runs 
        ORDER BY start_time DESC 
        LIMIT 1
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(latest_run_query))
            latest_run = result.fetchone()
            
            if latest_run:
                data['latest_run'] = {
                    'run_id': latest_run[0],
                    'run_type': latest_run[1],
                    'start_time': latest_run[2].isoformat() if latest_run[2] else None,
                    'end_time': latest_run[3].isoformat() if latest_run[3] else None,
                    'status': latest_run[4],
                    'total_servers': latest_run[5],
                    'total_databases': latest_run[6],
                    'total_tables': latest_run[7],
                    'total_rows_processed': latest_run[8],
                    'total_rows_inserted': latest_run[9],
                    'successful_syncs': latest_run[10],
                    'failed_syncs': latest_run[11]
                }
        
        # Get recent runs (last 10)
        recent_runs_query = """
        SELECT run_id, run_type, start_time, end_time, status, total_rows_inserted, successful_syncs, failed_syncs
        FROM migration_runs 
        ORDER BY start_time DESC 
        LIMIT 10
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(recent_runs_query))
            data['recent_runs'] = [
                {
                    'run_id': row[0],
                    'run_type': row[1],
                    'start_time': row[2].isoformat() if row[2] else None,
                    'end_time': row[3].isoformat() if row[3] else None,
                    'status': row[4],
                    'total_rows_inserted': row[5],
                    'successful_syncs': row[6],
                    'failed_syncs': row[7]
                }
                for row in result.fetchall()
            ]
        
        # Get active alerts
        alerts_query = """
        SELECT alert_type, severity, server_name, database_name, table_name, alert_message, alert_timestamp
        FROM alerts 
        WHERE is_resolved = FALSE 
        ORDER BY alert_timestamp DESC 
        LIMIT 20
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(alerts_query))
            data['active_alerts'] = [
                {
                    'alert_type': row[0],
                    'severity': row[1],
                    'server_name': row[2],
                    'database_name': row[3],
                    'table_name': row[4],
                    'message': row[5],
                    'timestamp': row[6].isoformat() if row[6] else None
                }
                for row in result.fetchall()
            ]
        
        # Get data consistency issues
        consistency_query = """
        SELECT server_name, database_name, table_name, source_row_count, target_row_count, 
               missing_rows, consistency_percentage, audit_status
        FROM row_count_audit 
        WHERE audit_status IN ('FAIL', 'WARNING')
        ORDER BY audit_timestamp DESC 
        LIMIT 20
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(consistency_query))
            data['consistency_issues'] = [
                {
                    'server_name': row[0],
                    'database_name': row[1],
                    'table_name': row[2],
                    'source_count': row[3],
                    'target_count': row[4],
                    'missing_rows': row[5],
                    'consistency_percentage': float(row[6]) if row[6] else 0,
                    'status': row[7]
                }
                for row in result.fetchall()
            ]
        
        # Get system health metrics
        health_query = """
        SELECT metric_name, metric_value, metric_unit, metric_timestamp
        FROM system_health_metrics 
        WHERE run_id = (SELECT run_id FROM migration_runs ORDER BY start_time DESC LIMIT 1)
        ORDER BY metric_timestamp DESC 
        LIMIT 10
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(health_query))
            data['system_health'] = [
                {
                    'metric_name': row[0],
                    'value': float(row[1]) if row[1] else 0,
                    'unit': row[2],
                    'timestamp': row[3].isoformat() if row[3] else None
                }
                for row in result.fetchall()
            ]
        
        return data

# Global logger instance
comprehensive_logger = ComprehensiveLogger() 