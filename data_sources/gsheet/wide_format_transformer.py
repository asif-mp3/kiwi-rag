import re
import pandas as pd
from datetime import datetime


def is_date_column(column_name: str) -> bool:
    """
    Check if a column name matches common date patterns.
    
    Patterns supported:
    - DD-MMM-YYYY (e.g., "11-Dec-2025")
    - DD-MM-YYYY (e.g., "11-12-2025")
    - YYYY-MM-DD (e.g., "2025-12-11")
    - D-MMM-YYYY (e.g., "1-Dec-2025")
    """
    # Pattern: DD-MMM-YYYY or D-MMM-YYYY
    pattern1 = r'^\d{1,2}-[A-Za-z]{3}-\d{4}$'
    # Pattern: DD-MM-YYYY or D-M-YYYY
    pattern2 = r'^\d{1,2}-\d{1,2}-\d{4}$'
    # Pattern: YYYY-MM-DD
    pattern3 = r'^\d{4}-\d{1,2}-\d{1,2}$'
    
    return bool(
        re.match(pattern1, str(column_name)) or
        re.match(pattern2, str(column_name)) or
        re.match(pattern3, str(column_name))
    )


def parse_date_column(column_name: str) -> datetime:
    """Parse date from column name, trying multiple formats."""
    formats = [
        "%d-%b-%Y",  # 11-Dec-2025
        "%d-%m-%Y",  # 11-12-2025
        "%Y-%m-%d",  # 2025-12-11
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(column_name), fmt)
        except ValueError:
            continue
    
    return None


def detect_wide_format(df: pd.DataFrame, min_date_columns: int = 5) -> tuple[bool, list]:
    """
    Detect if a DataFrame is in wide format (dates as columns).
    
    Args:
        df: DataFrame to check
        min_date_columns: Minimum number of date columns to consider it wide format
    
    Returns:
        (is_wide_format, date_columns_list)
    """
    date_columns = [col for col in df.columns if is_date_column(col)]
    
    is_wide = len(date_columns) >= min_date_columns
    
    return is_wide, date_columns


def unpivot_wide_format(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Transform wide format DataFrame to long format.
    
    Wide format:
        Employee Name | 11-Dec-2025 | 12-Dec-2025 | ...
        John          | 8.5         | NaN         | ...
    
    Long format:
        Employee Name | Date       | Hours  | Status
        John          | 2025-12-11 | 8.5    | P
        John          | 2025-12-12 | 0.0    | A
    
    Args:
        df: Wide format DataFrame
        table_name: Name of the table (for logging)
    
    Returns:
        Long format DataFrame or None if not suitable for unpivoting
    """
    # Detect date columns
    is_wide, date_columns = detect_wide_format(df)
    
    if not is_wide:
        print(f"   ⚠️  {table_name}: Not in wide format, skipping unpivot")
        return None
    
    # Check if date columns contain numeric data (hours/attendance)
    # Skip sheets where date columns contain strings (like "In"/"Out" timestamps)
    sample_values = []
    for date_col in date_columns[:3]:  # Check first 3 date columns
        non_null_values = df[date_col].dropna()
        if len(non_null_values) > 0:
            sample_values.extend(non_null_values.head(5).tolist())
    
    # If most values are strings, skip unpivoting
    string_count = sum(1 for v in sample_values if isinstance(v, str))
    if string_count > len(sample_values) * 0.5:
        print(f"   ⚠️  {table_name}: Date columns contain strings, skipping unpivot")
        return None
    
    # Identify metadata columns (non-date columns)
    metadata_columns = [col for col in df.columns if col not in date_columns]
    
    # Create long format
    records = []
    
    for _, row in df.iterrows():
        for date_col in date_columns:
            # Parse date
            date_value = parse_date_column(date_col)
            if not date_value:
                continue
            
            # Get hours/value
            hours = row[date_col]
            
            # Determine status and hours value
            try:
                if pd.isna(hours):
                    status = 'A'  # Absent
                    hours_value = 0.0
                elif isinstance(hours, str):
                    # Skip string values (like "WO", "AB", etc.)
                    continue
                elif float(hours) == 0:
                    status = 'A'  # Absent (0 hours)
                    hours_value = 0.0
                else:
                    status = 'P'  # Present
                    hours_value = float(hours)
            except (ValueError, TypeError):
                # Skip values that can't be converted to float
                continue
            
            # Build record
            record = {col: row[col] for col in metadata_columns}
            record['Date'] = date_value.date()
            record['Hours'] = hours_value
            record['Status'] = status
            
            records.append(record)
    
    if not records:
        print(f"   ⚠️  {table_name}: No valid records after unpivoting, skipping")
        return None
    
    long_df = pd.DataFrame(records)
    
    print(f"   ✓ Unpivoted {table_name}: {len(df)} rows → {len(long_df)} rows")
    print(f"     Metadata columns: {metadata_columns}")
    print(f"     Date range: {min(date_columns)} to {max(date_columns)}")
    
    return long_df
