import os
import yaml
import pyodbc
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
import psycopg2
# Add import for shared loader
from pg_loader import load_csv_to_postgres, validate_row_count
# Add import for comprehensive logging
from comprehensive_logging import comprehensive_logger
# ✅ Import the monitor instance for logging sync metrics and alerts
from monitoring import monitor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hybrid_sync.log'),
        logging.StreamHandler()
    ]
)

# Load DB connection info from YAML
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/db_connections.yaml')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '../data/sqlserver_exports/')

with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

# Configuration for sync strategies
SYNC_CONFIG = {
    'no_pk_strategy': 'smart_sync',  # Options: 'smart_sync', 'timestamp_sync', 'full_replace', 'full_append'
    'enable_audit_trail': True,      # Enable audit trail logging
    'compliance_mode': True          # Enable compliance-friendly operations
}

pg_conf = config['postgresql']

def get_sql_connection(conf, database=None):
    """Get connection to SQL Server with optional database"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={conf['server']},{conf.get('port', 1433)};"
            f"UID={conf['username']};"
            f"PWD={conf['password']};"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
        if database:
            conn_str += f";DATABASE={database}"
        conn = pyodbc.connect(conn_str, timeout=5)
        return conn
    except pyodbc.Error as e:
        logging.error(f"❌ Connection failed: {e}")
        raise

def get_pg_engine():
    """Get PostgreSQL engine"""
    conn_str = (
        f"postgresql+psycopg2://{pg_conf['username']}:{pg_conf['password']}@{pg_conf['host']}:{pg_conf['port']}/{pg_conf['database']}"
    )
    return create_engine(conn_str)
    
def create_schema_if_not_exists(engine, schema):
    with engine.connect() as conn:
        # Create schema if it doesn't exist
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        conn.commit()
        logging.info(f"Schema '{schema}' created/verified successfully")

def get_sql_server_data_types():
    """Get data type mapping from SQL Server to PostgreSQL"""
    return {
        'bigint': 'BIGINT',
        'int': 'INTEGER',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',
        'bit': 'BOOLEAN',
        'decimal': 'NUMERIC',
        'numeric': 'NUMERIC',
        'money': 'NUMERIC(19,4)',
        'smallmoney': 'NUMERIC(10,4)',
        'float': 'DOUBLE PRECISION',
        'real': 'REAL',
        'datetime': 'TIMESTAMP',
        'datetime2': 'TIMESTAMP',
        'smalldatetime': 'TIMESTAMP',
        'date': 'DATE',
        'time': 'TIME',
        'char': 'CHAR',
        'varchar': 'VARCHAR',
        'text': 'TEXT',
        'nchar': 'CHAR',
        'nvarchar': 'VARCHAR',
        'ntext': 'TEXT',
        'binary': 'BYTEA',
        'varbinary': 'BYTEA',
        'image': 'BYTEA',
        'uniqueidentifier': 'UUID'
    }

def infer_data_type(series):
    """Infer PostgreSQL data type from pandas series"""
    if series.dtype == 'int64':
        return 'BIGINT'
    elif series.dtype == 'float64':
        return 'DOUBLE PRECISION'
    elif series.dtype == 'bool':
        return 'BOOLEAN'
    elif series.dtype == 'datetime64[ns]':
        return 'TIMESTAMP'
    else:
        # For text data, check if it's a UUID
        sample_values = series.dropna().head(10)
        if len(sample_values) > 0:
            # Check if it looks like a UUID
            if all(len(str(val)) == 36 and str(val).count('-') == 4 for val in sample_values):
                return 'UUID'
        return 'TEXT'

def create_table_with_proper_types(engine, schema, table_name, df):
    """Create table with proper data types"""
    # Generate column definitions
    columns = []
    for col_name, series in df.items():
        pg_type = infer_data_type(series)
        # Clean column name (remove special characters)
        clean_col_name = ''.join(c for c in col_name if c.isalnum() or c in '_-')
        columns.append(f'"{clean_col_name}" {pg_type}')
    
    # Create table
    columns_def = ', '.join(columns)
    create_table_sql = f'''
    CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
        {columns_def}
    )
    '''
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
        logging.info(f"Created table '{schema}.{table_name}' with proper data types")


def create_sync_tracking_table(engine):
    """Create table to track database sync status"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sync_database_status (
        server_name VARCHAR(100),
        database_name VARCHAR(100),
        last_full_sync TIMESTAMP,
        last_incremental_sync TIMESTAMP,
        sync_status VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (server_name, database_name)
    )
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
        logging.info("Sync tracking table created/verified")

def get_all_databases(conn):
    """Get list of all user databases on the server"""
    cursor = conn.cursor()
    databases = []
    
    query = """
    SELECT name 
    FROM sys.databases 
    WHERE state = 0  -- Only online databases
    AND name NOT IN ('master', 'tempdb', 'model', 'msdb', 'distribution', 'ReportServer', 'ReportServerTempDB')
    ORDER BY name
    """
    
    cursor.execute(query)
    for row in cursor.fetchall():
        databases.append(row[0])
    
    return databases

def should_skip_database(db_name, conf):
    """Check if database should be skipped"""
    skip_databases = conf.get('skip_databases', [])
    
    if not skip_databases:
        return False
    
    if db_name in skip_databases:
        logging.info(f"Skipping database: {db_name} (listed in skip_databases)")
        return True
    
    return False

def should_skip_table(schema, table):
    """Check if table should be skipped"""
    # Skip system views and tables
    skip_tables = [
        'sys.trace_xe_event_map',
        'sys.trace_xe_action_map',
        'sys.trace_xe_event_map',
        'sys.trace_xe_action_map'
    ]
    
    table_full_name = f"{schema}.{table}"
    if table_full_name in skip_tables:
        logging.info(f"Skipping system table: {table_full_name}")
        return True
    
    # Skip tables starting with 'sys.'
    if schema.lower() == 'sys':
        logging.info(f"Skipping system schema table: {table_full_name}")
        return True
    
    return False

def get_sync_status(engine, server_name, database_name):
    """Get sync status for a database"""
    query = """
    SELECT last_full_sync, last_incremental_sync, sync_status
    FROM sync_database_status
    WHERE server_name = :server_name AND database_name = :database_name
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(query),
            {"server_name": server_name, "database_name": database_name}
        )
        row = result.fetchone()
        return row if row else None
def update_sync_status(engine, server_name, database_name, sync_type, sync_status):
    """Update sync status for a database"""
    now = datetime.now()
    if sync_type == 'full':
        query = """
        INSERT INTO sync_database_status (server_name, database_name, last_full_sync, sync_status, updated_at)
        VALUES (:server_name, :database_name, :now, :sync_status, :now)
        ON CONFLICT (server_name, database_name) 
        DO UPDATE SET 
            last_full_sync = EXCLUDED.last_full_sync,
            sync_status = EXCLUDED.sync_status,
            updated_at = EXCLUDED.updated_at
        """
    else:  # incremental
        query = """
        INSERT INTO sync_database_status (server_name, database_name, last_incremental_sync, sync_status, updated_at)
        VALUES (:server_name, :database_name, :now, :sync_status, :now)
        ON CONFLICT (server_name, database_name) 
        DO UPDATE SET 
            last_incremental_sync = EXCLUDED.last_incremental_sync,
            sync_status = EXCLUDED.sync_status,
            updated_at = EXCLUDED.updated_at
        """
    with engine.connect() as conn:
        conn.execute(
            text(query),
            {
                "server_name": server_name,
                "database_name": database_name,
                "now": now,
                "sync_status": sync_status
            }
        )
        conn.commit()
def get_primary_key_info(conn, schema, table):
    """Get primary key information for a table"""
    try:
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        AND CONSTRAINT_NAME LIKE 'PK_%'
        ORDER BY ORDINAL_POSITION
        """
        cursor = conn.cursor()
        cursor.execute(query)
        pk_columns = [row[0] for row in cursor.fetchall()]
        return pk_columns
    except Exception as e:
        logging.warning(f"Could not get PK info for {schema}.{table}: {e}")
        return []

def get_timestamp_column(conn, schema, table):
    """Get timestamp or datetime column for incremental sync"""
    try:
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        AND DATA_TYPE IN ('datetime', 'datetime2', 'smalldatetime', 'timestamp')
        ORDER BY COLUMN_NAME
        """
        cursor = conn.cursor()
        cursor.execute(query)
        timestamp_columns = [row[0] for row in cursor.fetchall()]
        return timestamp_columns[0] if timestamp_columns else None
    except Exception as e:
        logging.warning(f"Could not get timestamp column for {schema}.{table}: {e}")
        return None

def get_unique_identifier_column(conn, schema, table):
    """Get unique identifier column for incremental sync"""
    try:
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table}'
        AND DATA_TYPE IN ('uniqueidentifier', 'int', 'bigint')
        ORDER BY COLUMN_NAME
        """
        cursor = conn.cursor()
        cursor.execute(query)
        id_columns = [row[0] for row in cursor.fetchall()]
        return id_columns[0] if id_columns else None
    except Exception as e:
        logging.warning(f"Could not get unique identifier column for {schema}.{table}: {e}")
        return None

def get_last_synced_pk(engine, server_name, database_name, schema, table):
    """Get last synced primary key value"""
    query = """
    SELECT last_pk_value
    FROM sync_table_status
    WHERE server_name = :server_name AND database_name = :database_name AND schema_name = :schema AND table_name = :table
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(query),
            {"server_name": server_name, "database_name": database_name, "schema": schema, "table": table}
        )
        row = result.fetchone()
        return row[0] if row else None

def get_last_synced_timestamp(engine, server_name, database_name, schema, table):
    """Get last synced timestamp value for tables without primary keys"""
    query = """
    SELECT last_pk_value
    FROM sync_table_status
    WHERE server_name = :server_name AND database_name = :database_name AND schema_name = :schema AND table_name = :table
    """
    with engine.connect() as conn:
        result = conn.execute(
            text(query),
            {"server_name": server_name, "database_name": database_name, "schema": schema, "table": table}
        )
        row = result.fetchone()
        return row[0] if row else None
def update_last_synced_pk(engine, server_name, database_name, schema, table, pk_value):
    """Update last synced primary key value"""
    # Convert numpy types to Python native types for PostgreSQL compatibility
    if hasattr(pk_value, 'item'):
        pk_value = pk_value.item()  # Convert numpy.int64 to Python int
    
    query = """
    INSERT INTO sync_table_status (server_name, database_name, schema_name, table_name, last_pk_value, updated_at)
    VALUES (:server_name, :database_name, :schema, :table, :pk_value, :now)
    ON CONFLICT (server_name, database_name, schema_name, table_name) 
    DO UPDATE SET 
        last_pk_value = EXCLUDED.last_pk_value,
        updated_at = EXCLUDED.updated_at
    """
    with engine.connect() as conn:
        conn.execute(
            text(query),
            {
                "server_name": server_name,
                "database_name": database_name,
                "schema": schema,
                "table": table,
                "pk_value": str(pk_value),  # Convert to string for storage
                "now": datetime.now()
            }
        )
        conn.commit()

def create_table_sync_tracking(engine):
    """Create table to track table-level sync status"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sync_table_status (
        server_name VARCHAR(100),
        database_name VARCHAR(100),
        schema_name VARCHAR(100),
        table_name VARCHAR(100),
        last_pk_value VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (server_name, database_name, schema_name, table_name)
    )
    """
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()

def cleanup_system_tables(engine, schema_name):
    """Remove system tables that might have been created in previous runs"""
    system_tables = [
        'sys_trace_xe_event_map',
        'sys_trace_xe_action_map'
    ]
    
    for table_name in system_tables:
        try:
            with engine.connect() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}"'))
                conn.commit()
                logging.info(f"Cleaned up system table: {schema_name}.{table_name}")
        except Exception as e:
            logging.warning(f"Could not clean up {schema_name}.{table_name}: {e}")

def full_sync_database(conn, db_name, server_conf, server_clean, output_dir, pg_engine):
    """Perform full sync for a database and load into PostgreSQL"""
    sync_start_time = datetime.now()
    logging.info(f"Starting FULL sync for database: {db_name}")
    
    cursor = conn.cursor()
    tables = []
    for row in cursor.tables(tableType='TABLE'):
        tables.append((row.table_schem, row.table_name))
    
    if not tables:
        logging.warning(f"No tables found in {db_name}.")
        return 0, 0, 0, 0  # processed, successful, failed, total_rows
    
    processed_count = 0
    successful_syncs = 0
    failed_syncs = 0
    total_rows_processed = 0
    
    for schema, table in tables:
        table_start_time = datetime.now()
        try:
            # Skip system tables and views
            if should_skip_table(schema, table):
                continue
                
            # Get source row count
            source_count = get_table_row_count(conn, schema, table)
            
            query = f"SELECT * FROM [{schema}].[{table}]"
            df = pd.read_sql(query, conn)
            
            server_dir = os.path.join(output_dir, f"{server_clean}_{db_name}")
            Path(server_dir).mkdir(parents=True, exist_ok=True)
            filename = f"{schema}_{table}.csv"
            filepath = os.path.join(server_dir, filename)
            df.to_csv(filepath, index=False)
            
            # Load into PostgreSQL
            schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
            load_csv_to_postgres(pg_engine, schema_name, filepath, if_exists='replace')
            
            # Get target row count
            target_count = get_postgres_row_count(pg_engine, schema_name, f"{schema}_{table}")
            
            # Calculate duration
            table_duration = (datetime.now() - table_start_time).total_seconds()
            
            # Log metrics
            monitor.log_sync_metric(
                server_name=server_conf['server'],
                database_name=db_name,
                schema_name=schema,
                table_name=table,
                sync_type='FULL',
                source_count=source_count,
                target_count=target_count,
                rows_processed=len(df),
                rows_inserted=len(df),
                duration=table_duration,
                status='SUCCESS'
            )
            
            # Check data consistency
            monitor.check_data_consistency(
                server_name=server_conf['server'],
                database_name=db_name,
                schema_name=schema,
                table_name=table,
                source_count=source_count,
                target_count=target_count
            )
            
            logging.info(f"FULL SYNC: Exported and loaded {schema}.{table} ({len(df)} rows) in {table_duration:.2f}s")
            processed_count += 1
            successful_syncs += 1
            total_rows_processed += len(df)
            
        except Exception as e:
            table_duration = (datetime.now() - table_start_time).total_seconds()
            logging.error(f"Failed to export/load {schema}.{table}: {e}")
            
            # Log failure metrics
            monitor.log_sync_metric(
                server_name=server_conf['server'],
                database_name=db_name,
                schema_name=schema,
                table_name=table,
                sync_type='FULL',
                source_count=0,
                target_count=0,
                rows_processed=0,
                rows_inserted=0,
                duration=table_duration,
                status='FAILED',
                error_msg=str(e)
            )
            
            # Log alert
            monitor.log_alert(
                alert_type='SYNC_FAILURE',
                severity='HIGH',
                server_name=server_conf['server'],
                database_name=db_name,
                schema_name=schema,
                table_name=table,
                message=f"Full sync failed: {e}"
            )
            
            failed_syncs += 1
    
    return processed_count, successful_syncs, failed_syncs, total_rows_processed

def incremental_sync_database(conn, db_name, server_conf, server_clean, output_dir, engine):
    """Perform incremental sync for a database and load into PostgreSQL"""
    logging.info(f"Starting INCREMENTAL sync for database: {db_name}")
    cursor = conn.cursor()
    tables = []
    for row in cursor.tables(tableType='TABLE'):
        tables.append((row.table_schem, row.table_name))
    if not tables:
        logging.warning(f"No tables found in {db_name}.")
        return 0, 0, 0, 0  # processed, successful, failed, total_rows
    processed_count = 0
    successful_syncs = 0
    failed_syncs = 0
    total_rows_processed = 0
    for schema, table in tables:
        try:
            # Skip system tables and views
            if should_skip_table(schema, table):
                continue
                
            # Get current row count for debugging
            current_count = get_table_row_count(conn, schema, table)
            logging.info(f"Processing {schema}.{table} (current row count: {current_count})")
            
            # Try to find a suitable column for incremental sync
            pk_columns = get_primary_key_info(conn, schema, table)
            timestamp_col = get_timestamp_column(conn, schema, table)
            unique_id_col = get_unique_identifier_column(conn, schema, table)
            
            # Determine which column to use for incremental sync
            if pk_columns:
                sync_column = pk_columns[0]
                sync_type = "primary_key"
                logging.info(f"Using primary key column '{sync_column}' for {schema}.{table}")
            elif timestamp_col:
                sync_column = timestamp_col
                sync_type = "timestamp"
                logging.info(f"Using timestamp column '{sync_column}' for {schema}.{table}")
            elif unique_id_col:
                sync_column = unique_id_col
                sync_type = "unique_id"
                logging.info(f"Using unique identifier column '{sync_column}' for {schema}.{table}")
            else:
                logging.warning(f"No suitable column found for incremental sync in {schema}.{table}, performing smart sync")
                # Do smart sync for tables without suitable columns (compliance-friendly)
                query = f"SELECT * FROM [{schema}].[{table}]"
                df = pd.read_sql(query, conn)
                
                if len(df) > 0:
                    server_dir = os.path.join(output_dir, f"{server_clean}_{db_name}")
                    Path(server_dir).mkdir(parents=True, exist_ok=True)
                    filename = f"{schema}_{table}.csv"
                    filepath = os.path.join(server_dir, filename)
                    df.to_csv(filepath, index=False)
                    
                    # Use smart sync (compliance-friendly, no replace/delete)
                    schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
                    inserted_count = smart_sync_table_without_pk(engine, schema_name, f"{schema}_{table}", df)
                    logging.info(f"SMART SYNC (no PK): Exported and smart-synced {inserted_count} new rows from {schema}.{table}")
                    processed_count += 1
                    successful_syncs += 1
                    total_rows_processed += len(df)
                continue
            
            last_pk = get_last_synced_pk(engine, server_conf['server'], db_name, schema, table)
            
            if last_pk is None:
                logging.info(f"No previous sync found for {schema}.{table}, performing full sync")
                query = f"SELECT * FROM [{schema}].[{table}]"
                df = pd.read_sql(query, conn)
                # For full sync within incremental, we need to get the max value to track
                if len(df) > 0 and sync_column in df.columns:
                    max_pk = df[sync_column].max()
                else:
                    max_pk = None
            else:
                # Check if there are actually new rows before querying
                has_new_rows = check_for_new_rows(conn, schema, table, sync_column, last_pk)
                if not has_new_rows:
                    logging.info(f"No new rows found for {schema}.{table} since last sync (last_pk: {last_pk})")
                    continue
                
                logging.info(f"Found new rows for {schema}.{table}, last_pk: {last_pk}")
                query = f"SELECT * FROM [{schema}].[{table}] WHERE [{sync_column}] > ?"
                df = pd.read_sql(query, conn, params=[last_pk])
                max_pk = df[sync_column].max() if len(df) > 0 else last_pk
            
            if len(df) > 0:
                server_dir = os.path.join(output_dir, f"{server_clean}_{db_name}")
                Path(server_dir).mkdir(parents=True, exist_ok=True)
                filename = f"{schema}_{table}.csv"
                filepath = os.path.join(server_dir, filename)
                df.to_csv(filepath, index=False)
                
                # Update last synced PK only if we have a valid max_pk
                if max_pk is not None:
                    update_last_synced_pk(engine, server_conf['server'], db_name, schema, table, max_pk)
                
                # Load into PostgreSQL (use replace for full sync, append for incremental)
                schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
                if last_pk is None:
                    # Full sync - use replace to avoid duplicates
                    load_csv_to_postgres(engine, schema_name, filepath, if_exists='replace')
                else:
                    # Incremental sync - use append for new rows only
                    load_csv_to_postgres(engine, schema_name, filepath, if_exists='append')
                
                # Validate row count
                validate_row_count(engine, schema_name, f"{schema}_{table}", get_postgres_row_count(engine, schema_name, f"{schema}_{table}"))
                logging.info(f"INCREMENTAL SYNC: Exported and loaded {len(df)} new rows from {schema}.{table}")
                processed_count += 1
                successful_syncs += 1
                total_rows_processed += len(df)
            else:
                logging.info(f"No new data for {schema}.{table}")
        except Exception as e:
            logging.error(f"Failed to sync/load {schema}.{table}: {e}")
            failed_syncs += 1
    return processed_count, successful_syncs, failed_syncs, total_rows_processed

def get_postgres_row_count(engine, schema, table):
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"'))
        return result.scalar()

def check_for_new_rows(conn, schema, table, sync_col, last_pk):
    """Check if there are new rows since last sync"""
    if last_pk is None:
        return True  # No previous sync, so we need to check
    
    query = f"SELECT COUNT(*) FROM [{schema}].[{table}] WHERE [{sync_col}] > ?"
    cursor = conn.cursor()
    cursor.execute(query, [last_pk])
    new_count = cursor.fetchone()[0]
    return new_count > 0

def get_table_row_count(conn, schema, table):
    """Get current row count for a table"""
    try:
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]
    except Exception as e:
        logging.warning(f"Could not get row count for {schema}.{table}: {e}")
        return 0

def smart_sync_table_without_pk(engine, schema, table_name, source_df):
    """Smart sync for tables without primary keys - compares rows to avoid duplicates"""
    try:
        # Check if table exists in PostgreSQL
        with engine.connect() as conn:
            try:
                existing_df = pd.read_sql(f'SELECT * FROM "{schema}"."{table_name}"', conn)
                table_exists = True
            except:
                table_exists = False
        
        if not table_exists:
            # Table doesn't exist, create it and insert all data
            create_schema_if_not_exists(engine, schema)
            create_table_with_proper_types(engine, schema, table_name, source_df)
            source_df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
            logging.info(f"Smart sync: Created table and inserted {len(source_df)} rows into {schema}.{table_name}")
            return len(source_df)
        
        if len(existing_df) == 0:
            # No existing data, just insert all
            source_df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
            logging.info(f"Smart sync: Inserted {len(source_df)} new rows into {schema}.{table_name}")
            return len(source_df)
        
        # Compare dataframes to find new/changed rows
        # Create a hash of each row for comparison (excluding any existing hash column)
        source_columns = [col for col in source_df.columns if col != 'row_hash']
        existing_columns = [col for col in existing_df.columns if col != 'row_hash']
        
        # Ensure both dataframes have the same columns for comparison
        common_columns = list(set(source_columns) & set(existing_columns))
        
        if not common_columns:
            logging.warning(f"No common columns found for comparison in {schema}.{table_name}")
            return 0
        
        source_df_clean = source_df[common_columns].fillna('')
        existing_df_clean = existing_df[common_columns].fillna('')
        
        # Create hash of each row for comparison
        source_df_clean['row_hash'] = source_df_clean.apply(lambda x: hash(tuple(x)), axis=1)
        existing_df_clean['row_hash'] = existing_df_clean.apply(lambda x: hash(tuple(x)), axis=1)
        
        # Find new rows (rows in source but not in existing)
        new_rows = source_df_clean[~source_df_clean['row_hash'].isin(existing_df_clean['row_hash'])]
        
        if len(new_rows) > 0:
            # Remove the hash column and get the original data for insertion
            new_rows_original = source_df.iloc[new_rows.index]
            new_rows_original.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
            logging.info(f"Smart sync: Inserted {len(new_rows)} new rows into {schema}.{table_name}")
            return len(new_rows)
        else:
            logging.info(f"Smart sync: No new rows found for {schema}.{table_name}")
            return 0
            
    except Exception as e:
        logging.error(f"Smart sync failed for {schema}.{table_name}: {e}")
        # Fallback to regular insert
        source_df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
        return len(source_df)

def process_sql_server_hybrid(server_name, server_conf, run_id):
    """Process a single SQL Server with hybrid sync"""
    session_start_time = datetime.now()
    session_id = f"{server_name}_{session_start_time.strftime('%Y%m%d_%H%M%S')}"

    try:
        pg_engine = get_pg_engine()
        # Create tracking tables before any sync
        create_sync_tracking_table(pg_engine)
        create_table_sync_tracking(pg_engine)

        master_conn = get_sql_connection(server_conf)
        logging.info(f"Connected to SQL Server: {server_conf['server']}")

        databases = get_all_databases(master_conn)
        master_conn.close()

        if not databases:
            logging.warning(f"No user databases found on {server_conf['server']}.")
            return

        logging.info(f"Found {len(databases)} databases on {server_conf['server']}")

        server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')

        # Session tracking variables
        total_tables = 0
        successful_syncs = 0
        failed_syncs = 0
        skipped_tables = 0
        total_rows_processed = 0
        total_rows_inserted = 0
        full_sync_count = 0
        incremental_sync_count = 0

        for db_name in databases:
            if should_skip_database(db_name, server_conf):
                skipped_tables += 1
                continue

            # Clean up any existing system tables for this database
            schema_name = f"{server_clean}_{db_name}".replace('-', '_').replace(' ', '_')
            cleanup_system_tables(pg_engine, schema_name)

            sync_status = get_sync_status(pg_engine, server_conf['server'], db_name)

            if sync_status is None:
                logging.info(f"New database discovered: {db_name}")
                db_conn = get_sql_connection(server_conf, db_name)
                processed, success, failed, rows = full_sync_database(
                    db_conn, db_name, server_conf, server_clean, OUTPUT_DIR, pg_engine
                )
                db_conn.close()

                update_sync_status(pg_engine, server_conf['server'], db_name, 'full', 'COMPLETED')
                full_sync_count += 1
                total_tables += processed
                successful_syncs += success
                failed_syncs += failed
                total_rows_processed += rows
                total_rows_inserted += rows

            else:
                logging.info(f"Existing database: {db_name}")
                db_conn = get_sql_connection(server_conf, db_name)
                processed, success, failed, rows = incremental_sync_database(
                    db_conn, db_name, server_conf, server_clean, OUTPUT_DIR, pg_engine
                )
                db_conn.close()

                update_sync_status(pg_engine, server_conf['server'], db_name, 'incremental', 'COMPLETED')
                incremental_sync_count += 1
                total_tables += processed
                successful_syncs += success
                failed_syncs += failed
                total_rows_processed += rows
                total_rows_inserted += rows

        # Calculate session duration
        session_end_time = datetime.now()
        session_duration = (session_end_time - session_start_time).total_seconds()

        # Determine overall status
        overall_status = (
            'SUCCESS' if failed_syncs == 0
            else 'PARTIAL' if successful_syncs > 0
            else 'FAILED'
        )

        # Log session summary
        monitor.log_sync_summary(
            session_id=session_id,
            server_name=server_conf['server'],
            database_name='ALL',
            total_tables=total_tables,
            successful_syncs=successful_syncs,
            failed_syncs=failed_syncs,
            skipped_tables=skipped_tables,
            total_rows_processed=total_rows_processed,
            total_rows_inserted=total_rows_inserted,
            start_time=session_start_time,
            end_time=session_end_time,
            overall_status=overall_status
        )

        # Log alerts for failures
        if failed_syncs > 0:
            monitor.log_alert(
                alert_type='SESSION_FAILURES',
                severity='MEDIUM' if successful_syncs > 0 else 'HIGH',
                server_name=server_conf['server'],
                database_name='ALL',
                schema_name='',
                table_name='',
                message=f"Session completed with {failed_syncs} failed syncs out of {total_tables} total tables"
            )

        logging.info(
            f"Completed {server_name}: {full_sync_count} full syncs, "
            f"{incremental_sync_count} incremental syncs"
        )
        logging.info(
            f"Session Summary: {successful_syncs} successful, "
            f"{failed_syncs} failed, {total_rows_inserted} rows inserted in {session_duration:.2f}s"
        )

        return {
            "databases": len(databases),
            "tables": total_tables,
            "successful_syncs": successful_syncs,
            "failed_syncs": failed_syncs,
            "rows_processed": total_rows_processed,
            "rows_inserted": total_rows_inserted
        }

    except Exception as e:
        session_end_time = datetime.now()
        session_duration = (session_end_time - session_start_time).total_seconds()

        logging.error(f"Error processing {server_name}: {e}")

        # Log session failure
        monitor.log_sync_summary(
            session_id=session_id,
            server_name=server_conf['server'],
            database_name='ALL',
            total_tables=0,
            successful_syncs=0,
            failed_syncs=1,
            skipped_tables=0,
            total_rows_processed=0,
            total_rows_inserted=0,
            start_time=session_start_time,
            end_time=session_end_time,
            overall_status='FAILED'
        )

        # Log critical alert
        monitor.log_alert(
            alert_type='SESSION_FAILURE',
            severity='HIGH',
            server_name=server_conf['server'],
            database_name='ALL',
            schema_name='',
            table_name='',
            message=f"Session failed completely: {e}"
        )

        return {
            "databases": 0,
            "tables": 0,
            "successful_syncs": 0,
            "failed_syncs": 1,
            "rows_processed": 0,
            "rows_inserted": 0
        }



def debug_find_new_rows(server_name, server_conf):
    """Debug function to find tables with recent changes"""
    try:
        master_conn = get_sql_connection(server_conf)
        databases = get_all_databases(master_conn)
        master_conn.close()
        
        logging.info(f"=== DEBUG: Checking for recent changes in {server_name} ===")
        
        for db_name in databases:
            if should_skip_database(db_name, server_conf):
                continue
                
            db_conn = get_sql_connection(server_conf, db_name)
            cursor = db_conn.cursor()
            tables = []
            for row in cursor.tables(tableType='TABLE'):
                tables.append((row.table_schem, row.table_name))
            
            for schema, table in tables:
                try:
                    count = get_table_row_count(db_conn, schema, table)
                    if count > 0:  # Only show tables with data
                        logging.info(f"  {db_name}.{schema}.{table}: {count} rows")
                except Exception as e:
                    logging.warning(f"  Error checking {db_name}.{schema}.{table}: {e}")
            
            db_conn.close()
            
    except Exception as e:
        logging.error(f"Debug error for {server_name}: {e}")

def main():
    """Process all SQL servers with hybrid sync"""
    # Start comprehensive logging
    run_id = comprehensive_logger.start_migration_run('HYBRID_SYNC')
    comprehensive_logger.log_system_health(run_id)
    
    sqlservers = config.get('sqlservers', {})
    
    if not sqlservers:
        logging.error("No SQL servers configured in db_connections.yaml")
        comprehensive_logger.log_alert(run_id, 'CONFIG_ERROR', 'HIGH', '', '', '', '', 'No SQL servers configured')
        comprehensive_logger.end_migration_run(run_id, 'FAILED', {'error_message': 'No SQL servers configured'})
        return
    
    logging.info(f"Starting hybrid sync for {len(sqlservers)} SQL servers")
    comprehensive_logger.log_server_event(run_id, 'SYSTEM', 'ALL', 'INFO', f"Starting hybrid sync for {len(sqlservers)} SQL servers")
    
    # Initialize totals
    total_servers = len(sqlservers)
    total_databases = 0
    total_tables = 0
    total_rows_processed = 0
    total_rows_inserted = 0
    successful_syncs = 0
    failed_syncs = 0
    
    try:
        for server_name, server_conf in sqlservers.items():
            logging.info(f"Processing SQL Server: {server_name}")
            comprehensive_logger.log_server_event(run_id, server_name, 'ALL', 'INFO', f"Processing SQL Server: {server_name}")
            
            # Process the server and get summary
            summary = process_sql_server_hybrid(server_name, server_conf, run_id)
            
            # Aggregate
            total_databases += summary.get("databases", 0)
            total_tables += summary.get("tables", 0)
            total_rows_processed += summary.get("rows_processed", 0)
            total_rows_inserted += summary.get("rows_inserted", 0)
            successful_syncs += summary.get("successful_syncs", 0)
            failed_syncs += summary.get("failed_syncs", 0)
        
        # Final summary
        summary = {
            'total_servers': total_servers,
            'total_databases': total_databases,
            'total_tables': total_tables,
            'total_rows_processed': total_rows_processed,
            'total_rows_inserted': total_rows_inserted,
            'successful_syncs': successful_syncs,
            'failed_syncs': failed_syncs
        }
        
        comprehensive_logger.end_migration_run(run_id, 'COMPLETED', summary)
        comprehensive_logger.log_server_event(run_id, 'SYSTEM', 'ALL', 'INFO', 'Hybrid sync completed successfully')
        logging.info(f"Hybrid sync complete for {total_servers} servers. Summary: {summary}")
        
    except Exception as e:
        logging.error(f"Error in main sync process: {e}")
        comprehensive_logger.log_alert(run_id, 'SYNC_ERROR', 'CRITICAL', 'SYSTEM', 'ALL', '', '', f"Main sync process failed: {e}")
        comprehensive_logger.end_migration_run(run_id, 'FAILED', {'error_message': str(e)})


if __name__ == "__main__":
    main()