"""
Reload data with the fixed combine_date_time_columns function
"""
from data_sources.gsheet.connector import fetch_sheets_with_tables
from data_sources.gsheet.snapshot_loader import load_snapshot
import duckdb

# Load fresh data with the fixed code
print("Loading data from Google Sheets with FIXED code...")
sheets_with_tables = fetch_sheets_with_tables()

# Load into DuckDB
print("\nLoading into DuckDB...")
load_snapshot(sheets_with_tables, full_reset=True)

# Verify the fix
print("\n" + "=" * 80)
print("VERIFICATION:")
print("=" * 80)

conn = duckdb.connect('data_sources/snapshots/latest.duckdb')

print("\nSample data (should show 2017 dates now):")
result = conn.execute('SELECT Date, Time FROM worksheet1 LIMIT 5').fetchdf()
print(result)

print("\nTest timestamp filter for 02/01/2017 (January 2nd):")
result = conn.execute("""
    SELECT Date, Time, "EARLWOOD TEMP 1h average [°C]" 
    FROM worksheet1 
    WHERE Time >= TIMESTAMP '2017-01-02 00:00:00' 
      AND Time <= TIMESTAMP '2017-01-02 23:59:59' 
    LIMIT 5
""").fetchdf()
print(f"Found {len(result)} rows")
print(result)

conn.close()

if len(result) > 0:
    print("\n✅ FIX SUCCESSFUL! Timestamps now have correct dates.")
else:
    print("\n❌ FIX FAILED! Still no results.")
