import duckdb
import os
import json
from pathlib import Path
from typing import Dict, List, Any
from data_sources.gsheet.connector import fetch_sheets_with_tables

DB_PATH = "data_sources/snapshots/latest.duckdb"
TABLE_METADATA_FILE = "data_sources/snapshots/table_metadata.json"


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


def load_table_metadata() -> Dict[str, Dict[str, Any]]:
    """
    Load table metadata from disk.
    
    Returns:
        Dict mapping table_name to metadata:
        {
            "Sales_Table1": {
                "source_id": "1mRcD...#Sales",
                "sheet_name": "Sales",
                "table_index": 1,
                "row_count": 150,
                "created_at": "2025-12-30T12:50:09+05:30"
            },
            ...
        }
    """
    if not Path(TABLE_METADATA_FILE).exists():
        return {}
    
    try:
        with open(TABLE_METADATA_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  Could not load table metadata: {e}")
        return {}


def save_table_metadata(metadata: Dict[str, Dict[str, Any]]):
    """Persist table metadata to disk"""
    try:
        Path(TABLE_METADATA_FILE).parent.mkdir(parents=True, exist_ok=True)
        with open(TABLE_METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)
    except Exception as e:
        print(f"⚠️  Could not save table metadata: {e}")


def delete_tables_by_source_id(source_id: str, conn=None):
    """
    Delete all DuckDB tables associated with a given source_id.
    
    This is used for atomic sheet-level rebuilds: when a sheet changes,
    ALL tables derived from that sheet are deleted before rebuilding.
    
    Args:
        source_id: Source identifier (spreadsheet_id#sheet_name)
        conn: Optional DuckDB connection (creates new one if None)
    
    Returns:
        Number of tables deleted
    """
    close_conn = False
    if conn is None:
        conn = duckdb.connect(DB_PATH)
        close_conn = True
    
    try:
        # Load table metadata to find tables with this source_id
        metadata = load_table_metadata()
        
        tables_to_delete = []
        for table_name, table_meta in metadata.items():
            if table_meta.get('source_id') == source_id:
                tables_to_delete.append(table_name)
        
        # Delete each table
        for table_name in tables_to_delete:
            try:
                quoted_table = quote_identifier(table_name)
                conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")
                print(f"   Deleted table: {table_name}")
                
                # Remove from metadata
                del metadata[table_name]
            except Exception as e:
                print(f"   ⚠️  Error deleting table {table_name}: {e}")
        
        # Save updated metadata
        if tables_to_delete:
            save_table_metadata(metadata)
        
        return len(tables_to_delete)
        
    finally:
        if close_conn:
            conn.close()


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
        
        # Clear table metadata
        save_table_metadata({})
        
    except Exception as e:
        print(f"⚠️  Error resetting DuckDB: {e}")


def load_snapshot(sheets_with_tables=None, full_reset=False, changed_sheets=None):
    """
    Load Google Sheets data into DuckDB with multi-table detection.
    
    SHEET-LEVEL REBUILD LOGIC:
    - If full_reset=True: Delete all tables and rebuild everything
    - If changed_sheets provided: Delete only tables from changed sheets, rebuild those sheets
    - Otherwise: Incremental refresh (drop and recreate all tables)
    
    Args:
        sheets_with_tables: Pre-fetched sheets with detected tables. 
                           Dict[sheet_name, List[table_info]].
                           If None, will fetch from Google Sheets.
        full_reset: If True, perform full reset (drop all tables, recreate DB).
        changed_sheets: List of sheet names that changed (for incremental rebuild).
                       If provided, only these sheets will be rebuilt.
    """
    from datetime import datetime
    
    # Use pre-fetched sheets if provided, otherwise fetch
    if sheets_with_tables is None:
        sheets_with_tables = fetch_sheets_with_tables()
    
    # Load existing table metadata
    table_metadata = load_table_metadata()
    
    if full_reset:
        print("🔄 Performing FULL RESET...")
        
        # Drop all tables and recreate DB file
        reset_duckdb_snapshot()
        table_metadata = {}
        
        # Connect to fresh database
        conn = duckdb.connect(DB_PATH)
        
        # Rebuild all sheets
        sheets_to_rebuild = sorted(sheets_with_tables.keys())
        
    elif changed_sheets:
        print(f"🔄 Performing INCREMENTAL REBUILD for {len(changed_sheets)} sheet(s)...")
        
        # Connect to existing database
        conn = duckdb.connect(DB_PATH)
        
        # Delete tables from changed sheets
        for sheet_name in changed_sheets:
            # Get source_id for this sheet
            if sheet_name in sheets_with_tables and sheets_with_tables[sheet_name]:
                source_id = sheets_with_tables[sheet_name][0].get('source_id')
                if source_id:
                    print(f"   Deleting tables from sheet '{sheet_name}' (source_id: {source_id})...")
                    deleted_count = delete_tables_by_source_id(source_id, conn)
                    print(f"   Deleted {deleted_count} table(s)")
        
        # Rebuild only changed sheets
        sheets_to_rebuild = changed_sheets
        
    else:
        # Legacy incremental refresh (rebuild all)
        print("🔄 Performing LEGACY INCREMENTAL REFRESH...")
        conn = duckdb.connect(DB_PATH)
        sheets_to_rebuild = sorted(sheets_with_tables.keys())
    
    # Track used names to ensure uniqueness per snapshot load
    # Map: base_name -> count
    name_counts = {}
    
    # Load tables from sheets to rebuild
    for sheet_name in sheets_to_rebuild:
        if sheet_name not in sheets_with_tables:
            continue
        
        tables = sheets_with_tables[sheet_name]
        
        for idx, table_info in enumerate(tables, 1):
            # Determine base name
            if 'title' in table_info and table_info['title']:
                # Use semantic title
                base_name = sanitize_table_name(table_info['title'])
            else:
                # Fallback to SheetName_TableN
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
            
            # Drop table if it exists (for incremental refresh)
            conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            
            # Create table in DuckDB
            conn.execute(f"CREATE TABLE {quoted_table} AS SELECT * FROM df")
            print(f"   Created table: {final_name} ({len(df)} rows, {len(df.columns)} cols)")
            
            # Store the final table name in table_info for later use
            table_info['duckdb_table_name'] = final_name
            
            # Update table metadata
            table_metadata[final_name] = {
                "source_id": table_info.get('source_id'),
                "sheet_name": table_info.get('sheet_name'),
                "table_index": idx,
                "row_count": len(df),
                "created_at": datetime.now().isoformat()
            }
    
    conn.close()
    
    # Save updated table metadata
    save_table_metadata(table_metadata)
    
    if full_reset:
        print("✓ Full reset complete")
    elif changed_sheets:
        print(f"✓ Incremental rebuild complete ({len(changed_sheets)} sheet(s) rebuilt)")
    else:
        print("✓ Legacy incremental refresh complete")
    
    # Log table statistics
    print("\n📊 Table Statistics:")
    conn = duckdb.connect(DB_PATH)
    
    for sheet_name in sorted(sheets_to_rebuild):
        if sheet_name not in sheets_with_tables:
            continue
        
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
