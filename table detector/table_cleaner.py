import pandas as pd
import re


def clean_detected_tables(tables, keep_title=True):
    """
    Post-process detected tables to:
    1. Keep or remove title rows based on keep_title parameter
    2. Ensure proper header detection
    3. Clean up empty rows
    
    Note: Skips cleaning for wide tables that already have proper headers.
    """
    cleaned_tables = []
    
    for table in tables:
        # Skip cleaning for wide tables - they already have proper multi-level headers
        if table.get('is_wide_table', False) or 'header_rows' in table:
            cleaned_tables.append(table)
            continue
        
        df = table['dataframe'].copy()
        r0, r1 = table['row_range']
        c0, c1 = table['col_range']
        
        title_row = None
        
        if len(df) < 2:
            # Too small to have title + header + data
            cleaned_tables.append({
                "table_id": table['table_id'],
                "row_range": (r0, r1),
                "col_range": (c0, c1),
                "dataframe": df
            })
            continue
        
        # Check if first row is a title
        first_row = df.iloc[0]
        second_row = df.iloc[1] if len(df) > 1 else None
        
        # Count non-empty cells in first and second rows
        non_empty_first = first_row.astype(str).str.strip().replace('', pd.NA).notna().sum()
        non_empty_second = second_row.astype(str).str.strip().replace('', pd.NA).notna().sum() if second_row is not None else 0
        
        # Title detection criteria:
        # 1. First row has significantly fewer non-empty cells than second row (likely title vs header)
        # 2. OR first row has only 1-2 non-empty cells
        is_title = False
        
        if non_empty_first <= 2:
            # Definitely a title (1-2 cells)
            is_title = True
        elif non_empty_second > 0 and non_empty_first < (non_empty_second * 0.6):
            # First row has less than 60% of second row's cells - likely a title
            is_title = True
        
        if is_title:
            if keep_title:
                # Save the title - get the first non-empty cell
                for cell in first_row:
                    if pd.notna(cell) and str(cell).strip():
                        title_row = str(cell).strip()
                        break
            
            # Remove the title row from dataframe
            df = df.iloc[1:].reset_index(drop=True)
            r0 += 1
        
        # Check if we have a proper header row (should have multiple non-empty cells)
        if len(df) > 0:
            header_row = df.iloc[0]
            non_empty_in_header = header_row.astype(str).str.strip().replace('', pd.NA).notna().sum()
            
            # If header row has good coverage, use it as column names
            if non_empty_in_header >= max(2, len(df.columns) * 0.4):
                # Set first row as header
                df.columns = [str(col).strip() if pd.notna(col) and str(col).strip() else f"Column_{i}" 
                             for i, col in enumerate(df.iloc[0])]
                df = df.iloc[1:].reset_index(drop=True)
                r0 += 1
        
        # Remove trailing empty rows
        while len(df) > 0 and df.iloc[-1].astype(str).str.strip().replace('', pd.NA).notna().sum() == 0:
            df = df.iloc[:-1]
            r1 -= 1
        
        # Only keep tables with actual data (at least 1 data row after header)
        if len(df) > 0:
            table_info = {
                "table_id": table['table_id'],
                "row_range": (r0, r1),
                "col_range": (c0, c1),
                "dataframe": df
            }
            
            # Add title if we found one and keeping titles
            if title_row:
                table_info["title"] = title_row
            
            cleaned_tables.append(table_info)
    
    return cleaned_tables


def merge_related_tables(tables):
    """
    Merge tables that are logically related but split due to column gaps.
    Tables are merged if they:
    1. Have overlapping or adjacent row ranges
    2. Are in the same general area (within 5 rows)
    """
    if len(tables) <= 1:
        return tables
    
    merged = []
    used = set()
    
    for i, table1 in enumerate(tables):
        if i in used:
            continue
        
        r1_start, r1_end = table1['row_range']
        c1_start, c1_end = table1['col_range']
        
        # Look for tables to merge with this one
        tables_to_merge = [table1]
        
        for j, table2 in enumerate(tables[i+1:], start=i+1):
            if j in used:
                continue
            
            r2_start, r2_end = table2['row_range']
            c2_start, c2_end = table2['col_range']
            
            # Check if rows overlap or are very close
            row_overlap = (
                (r1_start <= r2_start <= r1_end + 5) or
                (r2_start <= r1_start <= r2_end + 5)
            )
            
            # Check if columns are adjacent or close (within 3 columns)
            col_adjacent = abs(c1_end - c2_start) <= 3 or abs(c2_end - c1_start) <= 3
            
            if row_overlap and col_adjacent:
                tables_to_merge.append(table2)
                used.add(j)
        
        # If we found tables to merge, combine them
        if len(tables_to_merge) > 1:
            merged_table = combine_tables(tables_to_merge)
            merged.append(merged_table)
        else:
            merged.append(table1)
        
        used.add(i)
    
    return merged


def combine_tables(tables):
    """
    Combine multiple tables into one by finding their bounding box
    and reconstructing the dataframe.
    """
    # Find bounding box
    min_row = min(t['row_range'][0] for t in tables)
    max_row = max(t['row_range'][1] for t in tables)
    min_col = min(t['col_range'][0] for t in tables)
    max_col = max(t['col_range'][1] for t in tables)
    
    # Create empty dataframe with the full size
    num_rows = max_row - min_row
    num_cols = max_col - min_col
    
    # Use the first table's dataframe as base to get the original data
    # We'll need to reconstruct from the original sheet
    # For now, just return the largest table
    largest = max(tables, key=lambda t: (t['row_range'][1] - t['row_range'][0]) * (t['col_range'][1] - t['col_range'][0]))
    
    return {
        "table_id": tables[0]['table_id'],
        "row_range": (min_row, max_row),
        "col_range": (min_col, max_col),
        "dataframe": largest['dataframe'],
        "merged_from": [t['table_id'] for t in tables]
    }
