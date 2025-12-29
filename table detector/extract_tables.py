"""
Extract Tables from Google Sheets
==================================

This script loads data from your Google Sheet and extracts detected tables
without normalization - just clean table detection and storage.
"""

from sheet_ingestion import ingest_google_sheet
from config import SPREADSHEET_ID, WORKSHEET_NAME, SERVICE_ACCOUNT_PATH
import json
import pandas as pd


def main():
    print("=" * 80)
    print("GOOGLE SHEETS TABLE EXTRACTION")
    print("=" * 80)
    
    # Load Google Sheet data
    print("\n📥 Loading Google Sheet...")
    result = ingest_google_sheet(
        spreadsheet_id=SPREADSHEET_ID,
        worksheet_name=WORKSHEET_NAME,
        service_account_path=SERVICE_ACCOUNT_PATH,
        use_custom_detector=True,
        keep_titles=True
    )
    
    print(f"✅ Loaded {len(result['tables'])} table(s) from sheet")
    
    # Extract and display each table
    all_tables = []
    
    for i, table in enumerate(result['tables'], 1):
        print("\n" + "=" * 80)
        print(f"TABLE {i}/{len(result['tables'])}")
        print("=" * 80)
        
        # Get DataFrame
        if 'dataframe' in table:
            df = table['dataframe']
        elif 'duckdb_data' in table:
            table_id = table['table_id']
            table_name = table_id.replace('-', '_')
            df = result['duckdb_tables'].get(table_name)
            if df is None:
                print(f"⚠️  Skipping table {table_id} - no data found")
                continue
        else:
            print(f"⚠️  Skipping table - unknown structure")
            continue
        
        table_id = table['table_id']
        table_title = table.get('title', 'Untitled')
        
        # Restore original column names
        if 'headers' in table and len(table['headers']) == len(df.columns):
            df.columns = [str(h) for h in table['headers']]
        
        print(f"\n📊 Table: {table_title}")
        print(f"   ID: {table_id}")
        print(f"   Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"   Columns: {', '.join(str(c) for c in df.columns[:5])}{'...' if len(df.columns) > 5 else ''}")
        
        print(f"\n   Data Preview (first 5 rows):")
        print("-" * 80)
        print(df.head(5).to_string(index=False))
        print("-" * 80)
        
        # Store table info
        all_tables.append({
            'table_id': table_id,
            'title': table_title,
            'shape': {
                'rows': df.shape[0],
                'columns': df.shape[1]
            },
            'columns': [str(c) for c in df.columns],
            'data': df.to_dict(orient='records')
        })
    
    # Export to JSON
    print("\n" + "=" * 80)
    print("EXPORTING RESULTS")
    print("=" * 80)
    
    export_data = {
        'spreadsheet_id': SPREADSHEET_ID,
        'worksheet_name': WORKSHEET_NAME,
        'total_tables': len(all_tables),
        'tables': all_tables
    }
    
    output_file = 'extracted_tables.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, default=str)
    
    print(f"\n✅ Results exported to: {output_file}")
    
    # Print summary
    print("\n" + "=" * 80)
    print("📋 ALL EXTRACTED TABLES")
    print("=" * 80)
    
    for i, table_info in enumerate(all_tables, 1):
        print(f"\n{'=' * 80}")
        print(f"TABLE {i}: {table_info['title']}")
        print(f"{'=' * 80}")
        print(f"Rows: {table_info['shape']['rows']}")
        print(f"Columns: {table_info['shape']['columns']}")
        print(f"\nColumn Names:")
        for col in table_info['columns']:
            print(f"  - {col}")
        
        # Recreate DataFrame for display
        df = pd.DataFrame(table_info['data'])
        print(f"\nData (showing up to 10 rows):")
        print("-" * 80)
        print(df.head(10).to_string(index=False))
        if len(df) > 10:
            print(f"... ({len(df) - 10} more rows)")
        print("-" * 80)
    
    print("\n" + "=" * 80)
    print(f"✅ EXTRACTION COMPLETE! {len(all_tables)} tables extracted.")
    print("=" * 80)


if __name__ == "__main__":
    main()
