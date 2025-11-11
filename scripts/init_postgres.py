#!/usr/bin/env python3
"""
Initialize PostgreSQL database with data from CSV files in the data folder.
Creates tables in the olist_data schema.
"""
import os
import time
import psycopg2
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
SCHEMA_NAME = "olist_data"

# Read database connection details from environment variables
DB_CONFIG = {
    'host': os.environ.get('POSTGRES_HOST', 'localhost'),
    'port': os.environ.get('POSTGRES_PORT', '5432'),
    'database': os.environ.get('POSTGRES_DB', 'dbt_workshop'),
    'user': os.environ.get('POSTGRES_USER', 'dbt_user'),
    'password': os.environ.get('POSTGRES_PASSWORD', 'dbt_password')
}

def wait_for_postgres(max_retries=30):
    """Wait for PostgreSQL to be ready."""
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("PostgreSQL is ready!")
            return True
        except psycopg2.OperationalError:
            print(f"Waiting for PostgreSQL... ({i+1}/{max_retries})")
            time.sleep(1)
    return False

def get_csv_files():
    """Get all CSV files from the data directory."""
    return sorted(DATA_DIR.glob("*.csv"))

def create_table_name(csv_path):
    """Convert CSV filename to table name (remove .csv extension)."""
    return csv_path.stem

def load_csv_to_postgres(csv_path, conn, schema_name):
    """Load a CSV file into PostgreSQL database."""
    table_name = create_table_name(csv_path)
    full_table_name = f"{schema_name}.{table_name}"

    print(f"Loading {csv_path.name} into table {full_table_name}...")

    # Read CSV into pandas DataFrame
    df = pd.read_csv(csv_path, low_memory=False)

    # Write to PostgreSQL
    from sqlalchemy import create_engine
    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    df.to_sql(
        table_name,
        engine,
        schema=schema_name,
        if_exists='replace',
        index=False,
        method='multi',
        chunksize=1000
    )

    row_count = len(df)
    print(f"  ✓ Loaded {row_count} rows into {full_table_name}")

    return full_table_name, row_count

def main():
    print("=" * 60)
    print("PostgreSQL Database Initialization")
    print("=" * 60)

    # Wait for PostgreSQL to be ready
    if not wait_for_postgres():
        print("ERROR: PostgreSQL is not ready after waiting")
        return 1

    csv_files = get_csv_files()

    if not csv_files:
        print(f"No CSV files found in {DATA_DIR}")
        return 1

    print(f"\nFound {len(csv_files)} CSV files to load")
    print(f"Database: {DB_CONFIG['database']} @ {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"Schema: {SCHEMA_NAME}")
    print("-" * 60)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
        conn.commit()
        print(f"✓ Schema {SCHEMA_NAME} created/verified")
        print("-" * 60)

        total_rows = 0
        loaded_tables = []

        for csv_file in csv_files:
            table_name, row_count = load_csv_to_postgres(csv_file, conn, SCHEMA_NAME)
            loaded_tables.append(table_name)
            total_rows += row_count

        conn.commit()

        print("-" * 60)
        print(f"✓ Successfully loaded {len(loaded_tables)} tables with {total_rows:,} total rows")
        print("\nTables created:")
        for table in loaded_tables:
            print(f"  - {table}")
        print("=" * 60)

        cursor.close()
        conn.close()
        return 0

    except Exception as e:
        print(f"ERROR: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
