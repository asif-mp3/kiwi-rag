import duckdb
import os
from pathlib import Path
from data_sources.gsheet.connector import fetch_sheets

DB_PATH = "data_sources/snapshots/latest.duckdb"


def quote_identifier(name: str) -> str:
    """Quote SQL identifiers that contain spaces or special characters"""
    if ' ' in name or any(char in name for char in ['-', '.', '(', ')']):
        return f'"{name}"'
    return name


def drop_all_tables(conn):
    """Drop all tables in DuckDB database"""
    try:
        # Get list of all tables
        tables_result = conn.execute("SHOW TABLES").fetchall()
        tables = [row[0] for row in tables_result]
        
        # Drop each table
        for table in tables:
            quoted_table = quote_identifier(table)
            conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            print(f"   Dropped table: {table}")
        
        return len(tables)
    except Exception as e:
        print(f"⚠️  Error dropping tables: {e}")
        return 0


def reset_duckdb_snapshot():
    """Delete and recreate DuckDB snapshot file for clean state"""
    try:
        if Path(DB_PATH).exists():
            os.remove(DB_PATH)
            print(f"   Deleted old DuckDB file: {DB_PATH}")
        
        # Create new empty database
        conn = duckdb.connect(DB_PATH)
        conn.close()
        print(f"   Created fresh DuckDB file: {DB_PATH}")
        
    except Exception as e:
        print(f"⚠️  Error resetting DuckDB: {e}")


def load_snapshot(sheets=None, full_reset=False):
    """
    Load Google Sheets data into DuckDB.
    Automatically detects and transforms wide format sheets to long format.
    
    Args:
        sheets: Pre-fetched sheets dict. If None, will fetch from Google Sheets.
        full_reset: If True, perform full reset (drop all tables, recreate DB).
                   If False, use incremental refresh (drop and recreate per table).
    """
    # Use pre-fetched sheets if provided, otherwise fetch
    if sheets is None:
        sheets = fetch_sheets()
    
    if full_reset:
        print("🔄 Performing FULL RESET...")
        
        # Drop all tables and recreate DB file
        reset_duckdb_snapshot()
        
        # Connect to fresh database
        conn = duckdb.connect(DB_PATH)
        
        # Load all sheets (with wide format transformation)
        from data_sources.gsheet.wide_format_transformer import detect_wide_format, unpivot_wide_format
        
        for table_name, df in sheets.items():
            quoted_table = quote_identifier(table_name)
            
            # Load original table
            conn.execute(f"CREATE TABLE {quoted_table} AS SELECT * FROM df")
            print(f"   Created table: {table_name}")
            
            # Check if wide format and create long format version
            is_wide, date_columns = detect_wide_format(df)
            if is_wide:
                long_df = unpivot_wide_format(df, table_name)
                if long_df is not None:
                    long_table_name = f"{table_name}_long"
                    quoted_long_table = quote_identifier(long_table_name)
                    conn.execute(f"CREATE TABLE {quoted_long_table} AS SELECT * FROM long_df")
                    print(f"   Created long format table: {long_table_name}")
        
        conn.close()
        print("✓ Full reset complete")
        
    else:
        # Incremental refresh
        from data_sources.gsheet.wide_format_transformer import detect_wide_format, unpivot_wide_format
        
        conn = duckdb.connect(DB_PATH)
        
        for table_name, df in sheets.items():
            quoted_table = quote_identifier(table_name)
            
            # Drop and recreate original table
            conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            conn.execute(f"CREATE TABLE {quoted_table} AS SELECT * FROM df")
            
            # Check if wide format and create/update long format version
            is_wide, date_columns = detect_wide_format(df)
            if is_wide:
                long_df = unpivot_wide_format(df, table_name)
                if long_df is not None:
                    long_table_name = f"{table_name}_long"
                    quoted_long_table = quote_identifier(long_table_name)
                    conn.execute(f"DROP TABLE IF EXISTS {quoted_long_table}")
                    conn.execute(f"CREATE TABLE {quoted_long_table} AS SELECT * FROM long_df")
        
        conn.close()
    
    # Log table statistics
    print("\n📊 Table Statistics:")
    conn = duckdb.connect(DB_PATH)
    for table_name in sheets.keys():
        quoted_table = quote_identifier(table_name)
        row_count = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
        col_info = conn.execute(f"DESCRIBE {quoted_table}").fetchdf()
        
        # Count column types
        type_counts = col_info['column_type'].value_counts().to_dict()
        type_summary = ", ".join([f"{count} {dtype}" for dtype, count in type_counts.items()])
        
        print(f"   {table_name}: {row_count:,} rows, {len(col_info)} cols ({type_summary})")
    conn.close()
    
    # Mark as synced after successful load
    # Pass the sheets we just loaded to avoid re-fetching
    from data_sources.gsheet.change_detector import mark_synced
    mark_synced(sheets)
