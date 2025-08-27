import os
import yaml
import pandas as pd
import logging
from sqlalchemy import create_engine, text, MetaData, inspect
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('load_postgres.log'),
        logging.StreamHandler()
    ]
)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/db_connections.yaml')
EXPORT_DIR = os.path.join(os.path.dirname(__file__), '../data/sqlserver_exports/')

with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

pg_conf = config['postgresql']

def get_pg_engine(conf):
    conn_str = (
        f"postgresql+psycopg2://{conf['username']}:{conf['password']}@{conf['host']}:{conf['port']}/{conf['database']}"
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

def load_csv_to_postgres(engine, schema, csv_path):
    table_name = os.path.splitext(os.path.basename(csv_path))[0]
    # Clean table name (remove special characters)
    table_name = ''.join(c for c in table_name if c.isalnum() or c in '_-')
    
    # Read CSV
    df = pd.read_csv(csv_path)
    
    # Create table with proper data types
    create_table_with_proper_types(engine, schema, table_name, df)
    
    # Load data
    df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
    logging.info(f"Loaded {csv_path} into {schema}.{table_name} ({len(df)} rows)")

def process_server_directory(engine, server_dir, schema_name):
    """Process all CSV files in a server directory"""
    csv_files = [f for f in os.listdir(server_dir) if f.endswith('.csv')]
    
    if not csv_files:
        logging.warning(f"No CSV files found in {server_dir}")
        return
    
    logging.info(f"Processing {len(csv_files)} CSV files for schema '{schema_name}'")
    
    for csv_file in csv_files:
        csv_path = os.path.join(server_dir, csv_file)
        try:
            load_csv_to_postgres(engine, schema_name, csv_path)
        except Exception as e:
            logging.error(f"Failed to load {csv_file}: {e}")

def main():
    try:
        engine = get_pg_engine(pg_conf)
        
        # Check if export directory exists
        if not os.path.exists(EXPORT_DIR):
            logging.error(f"Export directory not found: {EXPORT_DIR}")
            return
        
        # Get all server directories
        server_dirs = [d for d in os.listdir(EXPORT_DIR) 
                      if os.path.isdir(os.path.join(EXPORT_DIR, d))]
        
        if not server_dirs:
            logging.warning("No server directories found to load.")
            return
        
        logging.info(f"Found {len(server_dirs)} server directories to process")
        
        for server_dir_name in server_dirs:
            server_dir_path = os.path.join(EXPORT_DIR, server_dir_name)
            
            # Create schema name from directory name
            schema_name = server_dir_name.replace('-', '_').replace(' ', '_')
            
            try:
                # Create schema
                create_schema_if_not_exists(engine, schema_name)
                logging.info(f"Processing server directory: {server_dir_name}")
                
                # Process all CSV files in this directory
                process_server_directory(engine, server_dir_path, schema_name)
                
                logging.info(f"Completed processing {server_dir_name}")
                
            except Exception as e:
                logging.error(f"Error processing {server_dir_name}: {e}")
        
        logging.info("All server directories loaded into PostgreSQL successfully.")
        
    except Exception as e:
        logging.error(f"Error in loading: {e}")

if __name__ == "__main__":
    main()