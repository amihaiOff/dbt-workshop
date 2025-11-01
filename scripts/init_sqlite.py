#!/usr/bin/env python3
"""
Initialize SQLite database with data from CSV files in the data folder.
Creates tables in the olist_data schema.
"""
import sqlite3
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
DB_FILE = DATA_DIR / "workshop.db"
SCHEMA_NAME = "olist_data"

def get_csv_files():
    """Get all CSV files from the data directory."""
    return sorted(DATA_DIR.glob("*.csv"))

def create_table_name(csv_path):
    """Convert CSV filename to table name (remove .csv extension)."""
    return csv_path.stem

def load_csv_to_sqlite(csv_path, conn, schema_name):
    """Load a CSV file into SQLite database with schema prefix."""
    table_name = create_table_name(csv_path)
    full_table_name = f"{schema_name}__{table_name}"
    
    print(f"Loading {csv_path.name} into table {full_table_name}...")
    
    df = pd.read_csv(csv_path)
    df.to_sql(full_table_name, conn, if_exists='replace', index=False)
    
    row_count = len(df)
    print(f"  ✓ Loaded {row_count} rows into {full_table_name}")
    
    return full_table_name, row_count

def main():
    csv_files = get_csv_files()
    
    if not csv_files:
        print(f"No CSV files found in {DATA_DIR}")
        return
    
    print(f"Found {len(csv_files)} CSV files to load")
    print(f"Database: {DB_FILE}")
    print(f"Schema: {SCHEMA_NAME}")
    print("-" * 60)
    
    conn = sqlite3.connect(DB_FILE)
    
    try:
        total_rows = 0
        loaded_tables = []
        
        for csv_file in csv_files:
            table_name, row_count = load_csv_to_sqlite(csv_file, conn, SCHEMA_NAME)
            loaded_tables.append(table_name)
            total_rows += row_count
        
        conn.commit()
        
        print("-" * 60)
        print(f"✓ Successfully loaded {len(loaded_tables)} tables with {total_rows} total rows")
        print("\nTables created:")
        for table in loaded_tables:
            print(f"  - {table}")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()

