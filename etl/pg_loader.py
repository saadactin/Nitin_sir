import os
import pandas as pd
import logging
from sqlalchemy import text
from pathlib import Path

def create_schema_if_not_exists(engine, schema):
    with engine.connect() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        conn.commit()
        logging.info(f"Schema '{schema}' created/verified successfully")

def infer_data_type(series):
    if series.dtype == 'int64':
        return 'BIGINT'
    elif series.dtype == 'float64':
        return 'DOUBLE PRECISION'
    elif series.dtype == 'bool':
        return 'BOOLEAN'
    elif series.dtype == 'datetime64[ns]':
        return 'TIMESTAMP'
    else:
        sample_values = series.dropna().head(10)
        if len(sample_values) > 0:
            if all(len(str(val)) == 36 and str(val).count('-') == 4 for val in sample_values):
                return 'UUID'
        return 'TEXT'

def create_table_with_proper_types(engine, schema, table_name, df):
    columns = []
    for col_name, series in df.items():
        pg_type = infer_data_type(series)
        clean_col_name = ''.join(c for c in col_name if c.isalnum() or c in '_-')
        columns.append(f'"{clean_col_name}" {pg_type}')
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

def load_csv_to_postgres(engine, schema, csv_path, if_exists='append'):
    table_name = os.path.splitext(os.path.basename(csv_path))[0]
    table_name = ''.join(c for c in table_name if c.isalnum() or c in '_-')
    df = pd.read_csv(csv_path)
    create_schema_if_not_exists(engine, schema)
    create_table_with_proper_types(engine, schema, table_name, df)
    df.to_sql(table_name, engine, schema=schema, if_exists=if_exists, index=False)
    logging.info(f"Loaded {csv_path} into {schema}.{table_name} ({len(df)} rows)")

def validate_row_count(engine, schema, table_name, expected_count):
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'))
        actual_count = result.scalar()
    if actual_count != expected_count:
        logging.warning(f"Row count mismatch for {schema}.{table_name}: expected {expected_count}, got {actual_count}")
        return False
    logging.info(f"Row count validated for {schema}.{table_name}: {actual_count} rows")
    return True 