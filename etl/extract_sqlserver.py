import os
import yaml
import pyodbc
import pandas as pd
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extract_sqlserver.log'),
        logging.StreamHandler()
    ]
)

# Load DB connection info from YAML
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/db_connections.yaml')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '../data/sqlserver_exports/')

with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

# Ensure output directory exists
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

def get_connection(conf, database=None):
    """Get connection to SQL Server, optionally to a specific database"""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={conf['server']};"
        f"UID={conf['username']};"
        f"PWD={conf['password']}"
    )
    if database:
        conn_str += f";DATABASE={database}"
    return pyodbc.connect(conn_str)

def get_all_databases(conn):
    """Get list of all user databases on the server"""
    cursor = conn.cursor()
    databases = []
    
    # Query to get only user databases (exclude system databases)
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

def get_table_row_count(conn, schema, table):
    """Get row count for a table"""
    try:
        query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]
    except Exception as e:
        logging.warning(f"Could not get row count for {schema}.{table}: {e}")
        return 0

def list_tables(conn):
    cursor = conn.cursor()
    tables = []
    for row in cursor.tables(tableType='TABLE'):
        tables.append((row.table_schem, row.table_name))
    return tables

def should_skip_database(db_name, conf):
    """Check if database should be skipped"""
    skip_databases = conf.get('skip_databases', [])
    
    # If skip_databases is empty or not present, migrate ALL databases
    if not skip_databases:
        return False
    
    # Only skip databases explicitly listed in config
    if db_name in skip_databases:
        logging.info(f"Skipping database: {db_name} (listed in skip_databases)")
        return True
    
    return False

def export_table_to_csv(conn, schema, table, output_dir, server_name, db_name):
    query = f"SELECT * FROM [{schema}].[{table}]"
    df = pd.read_sql(query, conn)
    
    # Create subdirectory for this server/database
    server_dir = os.path.join(output_dir, f"{server_name}_{db_name}")
    Path(server_dir).mkdir(parents=True, exist_ok=True)
    
    filename = f"{schema}_{table}.csv"
    filepath = os.path.join(server_dir, filename)
    df.to_csv(filepath, index=False)
    logging.info(f"Exported {schema}.{table} to {filepath} ({len(df)} rows)")

def process_database(conn, db_name, server_conf, server_clean, output_dir):
    """Process a single database"""
    logging.info(f"Processing database: {db_name}")
    
    tables = list_tables(conn)
    if not tables:
        logging.warning(f"No tables found in {db_name}.")
        return 0, 0
    
    processed_count = 0
    skipped_count = 0
    
    for schema, table in tables:
        try:
            export_table_to_csv(conn, schema, table, output_dir, server_clean, db_name)
            processed_count += 1
            
        except Exception as e:
            logging.error(f"Failed to export {schema}.{table}: {e}")
    
    logging.info(f"Database {db_name}: {processed_count} tables processed, {skipped_count} tables skipped")
    return processed_count, skipped_count

def process_sql_server(server_name, server_conf):
    """Process a single SQL Server instance - all databases"""
    try:
        # Connect to master database to get list of all databases
        master_conn = get_connection(server_conf)
        logging.info(f"Connected to SQL Server: {server_conf['server']}")
        
        # Get all databases
        databases = get_all_databases(master_conn)
        master_conn.close()
        
        if not databases:
            logging.warning(f"No user databases found on {server_conf['server']}.")
            return
        
        logging.info(f"Found {len(databases)} databases on {server_conf['server']}")
        
        # Clean server name for file paths
        server_clean = ''.join(c for c in server_conf['server'] if c.isalnum() or c in '_-')
        
        total_processed = 0
        total_skipped = 0
        processed_dbs = 0
        skipped_dbs = 0
        
        for db_name in databases:
            # Check if database should be skipped
            if should_skip_database(db_name, server_conf):
                skipped_dbs += 1
                continue
            
            try:
                # Connect to specific database
                db_conn = get_connection(server_conf, db_name)
                processed, skipped = process_database(db_conn, db_name, server_conf, server_clean, OUTPUT_DIR)
                db_conn.close()
                
                total_processed += processed
                total_skipped += skipped
                processed_dbs += 1
                
            except Exception as e:
                logging.error(f"Error processing database {db_name}: {e}")
        
        logging.info(f"Completed {server_name}: {processed_dbs} databases processed, {skipped_dbs} databases skipped")
        logging.info(f"Total: {total_processed} tables processed, {total_skipped} tables skipped")
        
    except Exception as e:
        logging.error(f"Error processing {server_name}: {e}")

def main():
    """Process all SQL servers in configuration"""
    sqlservers = config.get('sqlservers', {})
    
    if not sqlservers:
        logging.error("No SQL servers configured in db_connections.yaml")
        return
    
    logging.info(f"Starting extraction for {len(sqlservers)} SQL servers")
    
    for server_name, server_conf in sqlservers.items():
        logging.info(f"Processing SQL Server: {server_name}")
        process_sql_server(server_name, server_conf)
    
    logging.info("Extraction complete for all servers.")

if __name__ == "__main__":
    main()