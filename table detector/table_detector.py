import pandas as pd
from gridgulp import GridGulp
import tempfile
import os


def detect_tables_gridgulp(df: pd.DataFrame) -> list:
    """
    Detect multiple logical tables from a DataFrame using GridGulp.
    
    Args:
        df: Input DataFrame
        
    Returns:
        List of detected tables with metadata
    """
    # Create a temporary Excel file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xlsx', delete=False) as tmp:
        temp_path = tmp.name
    
    try:
        # Save DataFrame to temporary Excel file
        df.to_excel(temp_path, index=False, header=False)
        
        # Initialize GridGulp with custom config for better detection
        from gridgulp.config import Config
        
        config = Config(
            confidence_threshold=0.4,  # Lower threshold to detect more tables
            min_table_size=(1, 1),     # Allow smaller tables
            min_column_overlap_for_merge=0.2,  # More aggressive column merging
            detect_merged_cells=True,
            use_border_detection=True,
            # Island detection parameters to prevent merging
            island_min_cells=4,        # Lower minimum cells (default: 20)
            island_density_threshold=0.5,  # Lower density requirement (default: 0.8)
            prefer_large_tables=False,  # Don't prefer merging into large tables
            adaptive_thresholds=False,  # Disable adaptive threshold adjustments
            column_gap_prevents_merge=True,  # Gaps should prevent merging
            empty_row_tolerance=0,     # Don't tolerate empty rows in tables
        )
        
        gg = GridGulp(config=config)
        result = gg.detect_tables_sync(temp_path)
        
        extracted_tables = []
        
        # GridGulp returns sheets, each containing tables
        for sheet in result.sheets:
            for idx, table in enumerate(sheet.tables):
                r0 = table.range.start_row
                r1 = table.range.end_row
                c0 = table.range.start_col
                c1 = table.range.end_col
                
                table_df = (
                    df.iloc[r0:r1, c0:c1]
                    .reset_index(drop=True)
                )
                
                extracted_tables.append({
                    "table_id": f"table_{idx}",
                    "row_range": (r0, r1),
                    "col_range": (c0, c1),
                    "dataframe": table_df
                })
        
        return extracted_tables
    
    finally:
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def detect_tables_from_dataframe(df: pd.DataFrame, use_custom: bool = False) -> list:
    """
    Detect tables from a DataFrame using the appropriate detection method.
    
    Args:
        df: Input DataFrame from Google Sheets
        use_custom: If True, use custom detection algorithm
        
    Returns:
        List of detected tables with metadata
    """
    # Use standard detection (no multi-header detection)
    if use_custom:
        from custom_detector import detect_tables_custom
        return detect_tables_custom(df)
    else:
        return detect_tables_gridgulp(df)

