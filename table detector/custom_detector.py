import pandas as pd
import numpy as np


def detect_tables_custom(df: pd.DataFrame):
    """
    Custom table detection algorithm for Google Sheets.
    Detects tables by finding contiguous rectangular blocks of non-empty cells.
    """
    # Convert to numpy array for easier processing
    grid = df.fillna("").astype(str).values
    rows, cols = grid.shape
    
    # Create a binary mask of non-empty cells
    non_empty = (grid != "").astype(int)
    
    # Track which cells have been assigned to a table
    assigned = np.zeros((rows, cols), dtype=bool)
    
    tables = []
    table_id = 0
    
    # Scan for table starting points
    for r in range(rows):
        for c in range(cols):
            if non_empty[r, c] and not assigned[r, c]:
                # Found a potential table start
                # Expand to find the full table bounds
                r0, r1, c0, c1 = expand_table_region(non_empty, assigned, r, c, rows, cols)
                
                if (r1 - r0) >= 2 and (c1 - c0) >= 2:  # Minimum table size: 2x2
                    # Check if there's a title row above this table
                    if r0 > 0:
                        # Look at the row above
                        row_above = non_empty[r0 - 1, c0:c1]
                        non_empty_above = np.sum(row_above)
                        
                        # Look at the current first row
                        current_first = non_empty[r0, c0:c1]
                        non_empty_current = np.sum(current_first)
                        
                        # If row above has data and significantly fewer cells than current row,
                        # it's likely a title row
                        if non_empty_above > 0 and non_empty_above < (non_empty_current * 0.6):
                            # Include the title row
                            r0 -= 1
                    
                    # Mark cells as assigned
                    assigned[r0:r1, c0:c1] = True
                    
                    # Extract the table
                    table_df = df.iloc[r0:r1, c0:c1].reset_index(drop=True)
                    
                    tables.append({
                        "table_id": f"table_{table_id}",
                        "row_range": (r0, r1),
                        "col_range": (c0, c1),
                        "dataframe": table_df
                    })
                    table_id += 1
                else:
                    # Mark as assigned anyway to skip small fragments
                    assigned[r0:r1, c0:c1] = True
    
    return tables


def expand_table_region(non_empty, assigned, start_r, start_c, rows, cols):
    """
    Expand from a starting cell to find the full table bounds.
    Expands both horizontally and vertically to find rectangular table.
    """
    # First, find the extent of the first row to determine column range
    c0 = start_c
    c1 = start_c + 1
    
    # Expand right to find contiguous columns in the first row
    while c1 < cols and non_empty[start_r, c1]:
        c1 += 1
    
    # Expand left if needed
    while c0 > 0 and non_empty[start_r, c0 - 1]:
        c0 -= 1
    
    # Now expand downward, checking if rows have similar column coverage
    r0 = start_r
    r1 = start_r + 1
    
    max_empty_rows = 2  # Allow up to 2 consecutive empty rows
    empty_row_count = 0
    
    while r1 < rows and empty_row_count < max_empty_rows:
        # Check how many cells in this row (within our column range) are non-empty
        row_cells = non_empty[r1, c0:c1]
        non_empty_count = np.sum(row_cells)
        
        # If at least 30% of cells are non-empty, consider it part of the table
        if non_empty_count >= max(1, (c1 - c0) * 0.3):
            r1 += 1
            empty_row_count = 0
            
            # Adjust column range if this row extends further
            # Check left extension
            temp_c0 = c0
            while temp_c0 > 0 and non_empty[r1 - 1, temp_c0 - 1]:
                temp_c0 -= 1
            
            # Check right extension
            temp_c1 = c1
            while temp_c1 < cols and non_empty[r1 - 1, temp_c1]:
                temp_c1 += 1
            
            # Only extend if the extension is reasonable (not too far)
            if temp_c0 >= c0 - 2:
                c0 = temp_c0
            if temp_c1 <= c1 + 2:
                c1 = temp_c1
        else:
            empty_row_count += 1
            r1 += 1
    
    # Remove trailing empty rows
    r1 -= empty_row_count
    
    return r0, r1, c0, c1
