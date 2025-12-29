"""
Table Detection Integration Module
====================================

This module bridges the table detector layer with the main ingestion pipeline.
It provides a clean interface for detecting multiple tables within a single sheet.
"""

import sys
import os
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any

# Add table detector directory to path
TABLE_DETECTOR_PATH = Path(__file__).parent.parent.parent / "table detector"
sys.path.insert(0, str(TABLE_DETECTOR_PATH))

# Import table detector functions
from custom_detector import detect_tables_custom
from table_cleaner import clean_detected_tables


def detect_and_clean_tables(df: pd.DataFrame, sheet_name: str) -> List[Dict[str, Any]]:
    """
    Detect and clean multiple tables from a single sheet DataFrame.
    
    Args:
        df: Raw DataFrame from Google Sheets (with all data, no headers assumed)
        sheet_name: Name of the source sheet (for context)
    
    Returns:
        List of detected tables, each containing:
        - table_id: Unique identifier
        - row_range: (start_row, end_row)
        - col_range: (start_col, end_col)
        - dataframe: Cleaned table data with proper headers
        - title: Optional table title if detected
        - sheet_name: Source sheet name
    """
    try:
        # Step 1: Detect tables using custom detector
        detected_tables = detect_tables_custom(df)
        
        if not detected_tables:
            # Fallback: treat entire sheet as one table
            print(f"      ⚠️  No tables detected in '{sheet_name}', using entire sheet")
            return [{
                'table_id': 'table_0',
                'row_range': (0, len(df)),
                'col_range': (0, len(df.columns)),
                'dataframe': df,
                'sheet_name': sheet_name
            }]
        
        # Step 2: Clean detected tables (remove title rows, set headers, etc.)
        cleaned_tables = clean_detected_tables(detected_tables, keep_title=True)
        
        # Step 3: Add sheet context to each table
        for table in cleaned_tables:
            table['sheet_name'] = sheet_name
        
        return cleaned_tables
        
    except Exception as e:
        print(f"      ⚠️  Error detecting tables in '{sheet_name}': {e}")
        print(f"      → Falling back to treating entire sheet as one table")
        
        # Fallback: treat entire sheet as one table
        return [{
            'table_id': 'table_0',
            'row_range': (0, len(df)),
            'col_range': (0, len(df.columns)),
            'dataframe': df,
            'sheet_name': sheet_name
        }]


def get_table_name(sheet_name: str, table_index: int) -> str:
    """
    Generate a standardized table name.
    
    Args:
        sheet_name: Source sheet name
        table_index: 1-indexed table number
    
    Returns:
        Formatted table name: {SheetName}_Table{N}
    """
    return f"{sheet_name}_Table{table_index}"


def extract_table_title(table_info: Dict[str, Any]) -> str:
    """
    Extract a human-readable title for the table.
    
    Args:
        table_info: Table information dict
    
    Returns:
        Title string (either detected title or generated from table_id)
    """
    if 'title' in table_info and table_info['title']:
        return table_info['title']
    
    # Generate title from table_id
    table_id = table_info.get('table_id', 'Unknown')
    sheet_name = table_info.get('sheet_name', 'Unknown')
    return f"Table from {sheet_name} ({table_id})"
