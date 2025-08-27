# SQL Server to PostgreSQL Migration Tool

A comprehensive data migration tool with real-time monitoring, logging, and dashboards for migrating data from multiple SQL Server instances to a central PostgreSQL repository.

## 🚀 Features

### Core Migration
- **Full Data Sync**: Complete initial migration of all databases and tables
- **Incremental Sync**: Smart incremental updates based on primary keys, timestamps, or unique identifiers
- **Smart Sync**: For tables without primary keys, uses row hashing to avoid duplicates
- **Multi-Server Support**: Migrate from multiple SQL Server instances simultaneously
- **Data Type Mapping**: Automatic SQL Server to PostgreSQL data type conversion

### Comprehensive Logging & Monitoring
- **PostgreSQL-Based Logging**: All logs stored in PostgreSQL tables for persistence and querying
- **Real-Time Dashboard**: Live web-based monitoring dashboard
- **System Health Monitoring**: CPU, memory, disk usage tracking
- **Data Consistency Auditing**: Row count comparisons and consistency checks
- **Alert System**: Configurable alerts for sync failures, data inconsistencies, and system issues
- **Performance Metrics**: Detailed timing and throughput measurements

### Compliance & Safety
- **Audit Trail**: Complete tracking of all migration activities
- **No Data Loss**: Comprehensive validation and rollback capabilities
- **Pharmaceutical Compliance**: Designed for regulated environments
- **System Table Exclusion**: Automatic filtering of system views and tables

## 📊 Database Schema

The system creates the following PostgreSQL tables for comprehensive logging:

### Core Tables
- `migration_runs` - Overall migration session tracking
- `server_logs` - Detailed server-level event logging
- `table_sync_logs` - Individual table sync operations
- `row_count_audit` - Data consistency validation results

### Monitoring Tables
- `system_health_metrics` - System resource monitoring
- `scheduled_jobs` - Job scheduling and execution tracking
- `alerts` - Alert and notification management
- `performance_metrics` - Performance and throughput data

## 🛠️ Installation

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Database Connections**:
   Edit `config/db_connections.yaml` with your SQL Server and PostgreSQL credentials.

3. **Setup PostgreSQL**:
   Ensure your PostgreSQL database is running and accessible.

## 🚀 Usage

### 1. Run Migration
```bash
python etl/hybrid_sync.py
```

### 2. Start Real-Time Monitor
```bash
python etl/start_monitor.py
```

### 3. Access Dashboard
Open your browser to: `http://localhost:5000`

## 📈 Dashboard Features

### Real-Time Metrics
- **Migration Progress**: Live sync status and progress
- **Data Consistency**: Source vs target row count comparisons
- **System Health**: CPU, memory, and disk usage
- **Performance**: Sync duration and throughput metrics

### Historical Data
- **Migration History**: Complete audit trail of all runs
- **Trend Analysis**: Performance trends over time
- **Issue Tracking**: Historical alerts and resolutions

### Alerts & Notifications
- **Critical Issues**: Immediate notification of sync failures
- **Data Inconsistencies**: Row count mismatches and missing data
- **System Warnings**: Resource usage and performance alerts

## 🔧 Configuration

### Database Connections (`config/db_connections.yaml`)
```yaml
postgresql:
  host: localhost
  port: 5432
  database: migration_repository
  username: your_username
  password: your_password

sqlservers:
  server1:
    server: your_sql_server
    username: your_username
    password: your_password
    skip_databases: []
```

### Sync Configuration
The system supports various sync strategies:
- **Primary Key Based**: Standard incremental sync using primary keys
- **Timestamp Based**: Using timestamp columns for incremental sync
- **Smart Sync**: Row hashing for tables without primary keys
- **Full Replace**: Complete table replacement (configurable)

## 📊 Monitoring Tables

### Migration Runs
Tracks overall migration sessions with start/end times, status, and summary statistics.

### Server Logs
Detailed event logging for each server and database operation.

### Table Sync Logs
Individual table sync operations with timing, row counts, and status.

### Row Count Audit
Data consistency validation with source vs target comparisons.

### System Health Metrics
Real-time system resource monitoring (CPU, memory, disk).

### Alerts
Configurable alert system for various issues and thresholds.

## 🔍 Troubleshooting

### Common Issues

1. **Connection Errors**:
   - Verify database credentials in `db_connections.yaml`
   - Check network connectivity to SQL Server and PostgreSQL

2. **Data Type Issues**:
   - Review automatic data type mapping
   - Check for unsupported SQL Server data types

3. **Performance Issues**:
   - Monitor system resources via dashboard
   - Check for large table sync operations

4. **Consistency Issues**:
   - Review row count audit reports
   - Check for data truncation or encoding issues

### Log Analysis
All logs are stored in PostgreSQL tables for easy querying:
```sql
-- Check recent migration runs
SELECT * FROM migration_runs ORDER BY start_time DESC LIMIT 10;

-- Find failed syncs
SELECT * FROM table_sync_logs WHERE sync_status = 'FAILED';

-- Check data consistency issues
SELECT * FROM row_count_audit WHERE audit_status = 'FAIL';
```

## 🔒 Security & Compliance

### Data Protection
- All credentials stored in encrypted configuration files
- No sensitive data logged to files
- Secure database connections with SSL support

### Audit Trail
- Complete tracking of all migration activities
- Immutable logs for compliance requirements
- Detailed change tracking and validation

### Pharmaceutical Compliance
- Designed for regulated environments
- Comprehensive audit trails
- Data integrity validation
- No data loss guarantees

## 📞 Support

For issues or questions:
1. Check the dashboard for real-time status
2. Review PostgreSQL logs for detailed error information
3. Check system health metrics for resource issues
4. Review data consistency reports for validation issues

## 🔄 Scheduling

The system supports scheduled migrations:
- Configure cron-style schedules in PostgreSQL
- Automatic job execution and monitoring
- Email notifications for job status
- Dashboard integration for scheduled runs

## 📈 Performance Optimization

### Recommendations
- Monitor system resources via dashboard
- Use appropriate batch sizes for large tables
- Schedule migrations during low-usage periods
- Regular maintenance of PostgreSQL tables

### Monitoring
- Real-time performance metrics
- Historical trend analysis
- Resource usage tracking
- Alert-based optimization recommendations

