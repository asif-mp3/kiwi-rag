"""
Google Sheets Table Detection and Ingestion Pipeline
====================================================

This module provides a pipeline for detecting and cleaning tables from Google Sheets.

Usage:
------
from sheet_ingestion import ingest_google_sheet

# Ingest a Google Sheet
result = ingest_google_sheet(
    spreadsheet_id="YOUR_SPREADSHEET_ID",
    worksheet_name="Sheet1",
    service_account_path="service_account.json"
)

# Access the detected tables
tables = result['tables']
metadata = result['metadata']
"""

from gsheet_loader import load_google_sheet
from table_detector import detect_tables_from_dataframe
from table_cleaner import clean_detected_tables
from typing import Dict, List, Any
import json


def ingest_google_sheet(
    spreadsheet_id: str,
    worksheet_name: str,
    service_account_path: str,
    use_custom_detector: bool = True,
    keep_titles: bool = True
) -> Dict[str, Any]:
    """
    Complete ingestion pipeline for Google Sheets.
    
    Args:
        spreadsheet_id: Google Sheets spreadsheet ID
        worksheet_name: Name of the worksheet to process
        service_account_path: Path to service account JSON file
        use_custom_detector: Use custom detection (recommended)
        keep_titles: Preserve table titles for identification
    
    Returns:
        Dictionary containing:
        - tables: List of detected and cleaned tables
        - metadata: Additional metadata about the ingestion
        - duckdb_tables: Dict mapping table_id to DataFrame
    """
    # Step 1: Load Google Sheet
    df = load_google_sheet(spreadsheet_id, worksheet_name, service_account_path)
    
    # Step 2: Detect tables
    tables = detect_tables_from_dataframe(df, use_custom=use_custom_detector)
    
    # Step 3: Clean tables
    tables = clean_detected_tables(tables, keep_title=keep_titles)
    
    # Step 4: Create DuckDB-style tables (generic column names)
    duckdb_tables = {}
    for table in tables:
        table_id = table['table_id']
        table_name = table_id.replace('-', '_')
        duckdb_tables[table_name] = table.get('dataframe')
    
    # Return complete result
    return {
        'tables': tables,
        'duckdb_tables': duckdb_tables,
        'metadata': {
            'spreadsheet_id': spreadsheet_id,
            'worksheet_name': worksheet_name,
            'sheet_dimensions': df.shape,
            'num_tables': len(tables),
            'table_ids': [t['table_id'] for t in tables]
        }
    }


def get_table_summary(result: Dict[str, Any]) -> str:
    """
    Generate a human-readable summary of the ingestion result.
    
    Args:
        result: Output from ingest_google_sheet()
    
    Returns:
        Formatted summary string
    """
    metadata = result['metadata']
    tables = result['tables']
    
    summary = []
    summary.append(f"Ingestion Summary")
    summary.append(f"=" * 60)
    summary.append(f"Spreadsheet: {metadata['spreadsheet_id']}")
    summary.append(f"Worksheet: {metadata['worksheet_name']}")
    summary.append(f"Dimensions: {metadata['sheet_dimensions'][0]} rows × {metadata['sheet_dimensions'][1]} columns")
    summary.append(f"Tables detected: {metadata['num_tables']}")
    summary.append("")
    
    for table in tables:
        summary.append(f"Table: {table['table_id']}")
        if table.get('title'):
            summary.append(f"  Title: {table['title']}")
        summary.append(f"  Shape: {table['shape']}")
        summary.append(f"  Columns: {len(table['headers'])}")
        summary.append("")
    
    return "\n".join(summary)


__all__ = [
    'ingest_google_sheet',
    'get_table_summary',
    'load_google_sheet',
    'detect_tables_from_dataframe',
    'clean_detected_tables'
]

