import duckdb
import os
from pathlib import Path
from data_sources.gsheet.connector import fetch_sheets_with_tables

DB_PATH = "data_sources/snapshots/latest.duckdb"


def quote_identifier(name: str) -> str:
    """Quote SQL identifiers that contain spaces or special characters"""
    if ' ' in name or any(char in name for char in ['-', '.', '(', ')']):
        return f'"{name}"'
    return name


def sanitize_table_name(name: str) -> str:
    """
    Sanitize string to be a valid, clean DuckDB table name.
    1. Replace non-alphanumeric chars (space, -, etc.) with underscore
    2. Remove multiple underscores
    3. Strip leading/trailing underscores
    """
    import re
    # Replace non-alphanumeric with _
    clean = re.sub(r'[^a-zA-Z0-9]', '_', str(name))
    # Collapse multiple _
    clean = re.sub(r'_+', '_', clean)
    # Strip
    return clean.strip('_')


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


def load_snapshot(sheets_with_tables=None, full_reset=False):
    """
    Load Google Sheets data into DuckDB with multi-table detection.
    Each detected table within a sheet gets its own DuckDB table.
    
    Args:
        sheets_with_tables: Pre-fetched sheets with detected tables. 
                           Dict[sheet_name, List[table_info]].
                           If None, will fetch from Google Sheets.
        full_reset: If True, perform full reset (drop all tables, recreate DB).
                   If False, use incremental refresh (drop and recreate per table).
    """
    # Use pre-fetched sheets if provided, otherwise fetch
    if sheets_with_tables is None:
        sheets_with_tables = fetch_sheets_with_tables()
    
    if full_reset:
        print("🔄 Performing FULL RESET...")
        
        # Drop all tables and recreate DB file
        reset_duckdb_snapshot()
        
        # Connect to fresh database
        conn = duckdb.connect(DB_PATH)
        
        # Track used names to ensure uniqueness per snapshot load
        # Map: base_name -> count
        name_counts = {}
        
        # Load all detected tables, iterating deterministically
        for sheet_name in sorted(sheets_with_tables.keys()):
            tables = sheets_with_tables[sheet_name]
            
            for idx, table_info in enumerate(tables, 1):
                # Determine base name
                if 'title' in table_info and table_info['title']:
                    # Use semantic title
                    base_name = sanitize_table_name(table_info['title'])
                else:
                    # Fallback to SheetName
                    base_name = sanitize_table_name(sheet_name)
                    # If falling back to sheet name, ensure we append TableN unless handled by collision logic
                    # actually, sticking to original fallback logic for no-title tables is safer:
                    # But let's verify if collision logic handles it.
                    # If I use 'sales' as base name, collision logic gives 'sales_1', 'sales_2'.
                    # This is cleaner than 'sales_Table1' mixed with 'Freshggies...'.
                    base_name = f"{sanitize_table_name(sheet_name)}_Table{idx}"

                # Calculate unique final name
                if base_name in name_counts:
                    name_counts[base_name] += 1
                    final_name = f"{base_name}_{name_counts[base_name]}"
                else:
                    name_counts[base_name] = 1
                    # Special case: if base_name came from a title, use it directly for the first occurrence
                    final_name = base_name

                quoted_table = quote_identifier(final_name)
                
                # Get the dataframe for this table
                df = table_info['dataframe']
                
                # Create table in DuckDB
                conn.execute(f"CREATE TABLE {quoted_table} AS SELECT * FROM df")
                print(f"   Created table: {final_name} ({len(df)} rows, {len(df.columns)} cols)")
                
                # Store the final table name in table_info for later use (e.g. logging)
                table_info['duckdb_table_name'] = final_name
        
        conn.close()
        print("✓ Full reset complete")
        
        
    else:
        # Incremental refresh
        conn = duckdb.connect(DB_PATH)
        
        for sheet_name, tables in sheets_with_tables.items():
            for idx, table_info in enumerate(tables, 1):
                # Use pre-assigned name
                final_name = table_info['duckdb_table_name']
                quoted_table = quote_identifier(final_name)
                
                # Get the dataframe for this table
                df = table_info['dataframe']
                
                # Drop and recreate table
                # (Ideally we would merge, but for now simple replace is safer for tables)
                conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")
                conn.execute(f"CREATE TABLE {quoted_table} AS SELECT * FROM df")
        
        conn.close()
    
    # Log table statistics
    print("\n📊 Table Statistics:")
    conn = duckdb.connect(DB_PATH)
    
    for sheet_name in sorted(sheets_with_tables.keys()):
        tables = sheets_with_tables[sheet_name]
        for idx, table_info in enumerate(tables, 1):
            final_name = table_info['duckdb_table_name']
            quoted_table = quote_identifier(final_name)
            
            try:
                row_count = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
                col_info = conn.execute(f"DESCRIBE {quoted_table}").fetchdf()
                
                # Count column types
                type_counts = col_info['column_type'].value_counts().to_dict()
                type_summary = ", ".join([f"{count} {dtype}" for dtype, count in type_counts.items()])
                
                # Show table lineage
                row_range = table_info.get('row_range', (0, 0))
                # Provide 1-based index for user friendliness
                r_start = row_range[0] + 1
                r_end = row_range[1]
                print(f"   {final_name}: {row_count:,} rows, {len(col_info)} cols ({type_summary})")
                print(f"      Source: {sheet_name} rows {r_start}-{r_end}")
            
            except Exception as e:
                print(f"      ⚠️  Error reading stats for {final_name}: {e}")
    
    conn.close()
    
    # Mark as synced after successful load
    from data_sources.gsheet.change_detector import mark_synced
    mark_synced(sheets_with_tables)
