import gspread
import pandas as pd
import numpy as np
import yaml
from google.oauth2.service_account import Credentials
from typing import Dict, List, Any


def _load_config():
    with open("config/settings.yaml") as f:
        return yaml.safe_load(f)


def infer_and_convert_types(df):
    """
    Intelligently infer and convert data types from string data.
    Converts numeric strings to INT/FLOAT, dates to datetime, booleans to bool.
    """
    for col in df.columns:
        try:
            # Skip if already numeric (check dtype, not boolean evaluation)
            if pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            # Skip if all values are NA
            if df[col].isna().all():
                continue
            
            # Get non-null values for analysis
            non_null = df[col].dropna()
            if len(non_null) == 0:
                continue
            
            # Try boolean conversion first (True/False, Yes/No, 1/0)
            if non_null.isin(['True', 'False', 'true', 'false', 'TRUE', 'FALSE', 
                              'Yes', 'No', 'yes', 'no', 'YES', 'NO',
                              '1', '0', 1, 0]).all():
                try:
                    df[col] = df[col].map({
                        'True': True, 'true': True, 'TRUE': True, 'Yes': True, 'yes': True, 'YES': True, '1': True, 1: True,
                        'False': False, 'false': False, 'FALSE': False, 'No': False, 'no': False, 'NO': False, '0': False, 0: False
                    })
                    continue
                except:
                    pass
            
            # Try numeric conversion (int or float)
            try:
                # Remove common formatting (commas, currency symbols, whitespace)
                cleaned = non_null.astype(str).str.strip()
                cleaned = cleaned.str.replace(',', '')
                cleaned = cleaned.str.replace('$', '')
                cleaned = cleaned.str.replace('₹', '')
                cleaned = cleaned.str.replace('%', '')
                
                # Try converting to numeric
                numeric_values = pd.to_numeric(cleaned, errors='coerce')
                
                # If >80% of non-null values are numeric, convert the column
                if numeric_values.notna().sum() / len(non_null) > 0.8:
                    # Check if all numeric values are integers
                    if numeric_values.dropna().apply(lambda x: x == int(x)).all():
                        df[col] = pd.to_numeric(df[col].astype(str).str.strip().str.replace(',', '').str.replace('$', '').str.replace('₹', '').str.replace('%', ''), errors='coerce').astype('Int64')
                    else:
                        df[col] = pd.to_numeric(df[col].astype(str).str.strip().str.replace(',', '').str.replace('$', '').str.replace('₹', '').str.replace('%', ''), errors='coerce')
                    continue
            except:
                pass
            
            # Try date/datetime conversion
            try:
                # Common date formats - suppress FutureWarning about deprecated parameter
                import warnings
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=FutureWarning)
                    date_values = pd.to_datetime(non_null, errors='coerce', infer_datetime_format=True)
                
                # If >80% of non-null values are valid dates, convert
                if date_values.notna().sum() / len(non_null) > 0.8:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    continue
            except:
                pass
        except Exception as e:
            # If any error occurs for this column, skip it and continue with next column
            print(f"      ⚠️  Warning: Could not infer type for column '{col}': {e}")
            continue
    
    return df


def detect_date_format(date_series):
    """
    Detect whether dates are in DD/MM/YYYY or MM/DD/YYYY format.
    
    Strategy:
    1. Look for dates where day > 12 (unambiguous)
    2. If found, determine format based on position
    3. Default to DD/MM/YYYY if ambiguous
    
    Returns:
        str: 'DD/MM/YYYY' or 'MM/DD/YYYY'
    """
    # Get non-null string values
    non_null = date_series.dropna().astype(str)
    
    if len(non_null) == 0:
        return 'DD/MM/YYYY'  # Default
    
    # Look for unambiguous dates (where one part is > 12)
    for date_str in non_null.head(100):  # Check first 100 dates
        parts = date_str.split('/')
        if len(parts) != 3:
            continue
        
        try:
            first = int(parts[0])
            second = int(parts[1])
            
            # If first part > 12, it must be day (DD/MM/YYYY)
            if first > 12:
                return 'DD/MM/YYYY'
            
            # If second part > 12, it must be day (MM/DD/YYYY)
            if second > 12:
                return 'MM/DD/YYYY'
        except ValueError:
            continue
    
    # Default to DD/MM/YYYY (international standard)
    return 'DD/MM/YYYY'


def combine_date_time_columns(df):
    """
    Combine separate Date and Time columns into a single proper timestamp.
    This fixes time range queries by ensuring Time has the correct date component.
    
    If both 'Date' and 'Time' columns exist:
    - Detect date format (DD/MM/YYYY vs MM/DD/YYYY)
    - Parse dates with correct format
    - Normalize Date column to ISO format (YYYY-MM-DD)
    - Combine them into Time as a proper timestamp (timezone-naive)
    """
    # Check if both Date and Time columns exist
    if 'Date' not in df.columns or 'Time' not in df.columns:
        return df
    
    try:
        # Detect date format
        date_format = detect_date_format(df['Date'])
        dayfirst = (date_format == 'DD/MM/YYYY')
        
        # Parse dates with correct format
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=FutureWarning)
            parsed_dates = pd.to_datetime(
                df['Date'],
                errors='coerce',
                dayfirst=dayfirst
            )
        
        # Parse time column to extract time components
        # The Time column may contain just time strings like "01:00", "14:30", etc.
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=FutureWarning)
            parsed_times = pd.to_datetime(
                df['Time'].astype(str),
                errors='coerce',
                format='mixed'
            )
        
        # Combine the date from parsed_dates with the time from parsed_times
        # This ensures we use the correct date from the Date column, not today's date
        combined = pd.to_datetime(
            parsed_dates.dt.strftime('%Y-%m-%d') + ' ' + 
            parsed_times.dt.strftime('%H:%M:%S'),
            errors='coerce'
        )
        
        # Remove timezone info if present
        if combined.dt.tz is not None:
            combined = combined.dt.tz_localize(None)
        
        # Only update if combination was successful for most rows
        if combined.notna().sum() / len(df) > 0.5:
            # Update Time column with proper timestamp
            df['Time'] = combined
            
            # Normalize Date column to ISO format (YYYY-MM-DD) for consistency
            df['Date'] = parsed_dates.dt.strftime('%d/%m/%Y')
            
            print(f"      → Combined Date + Time into timestamp column (detected {date_format} format)")
    except Exception as e:
        # If combination fails, keep original columns
        print(f"      ⚠️  Warning: Could not combine Date + Time columns: {e}")
        pass
    
    return df


def fetch_sheets():
    """
    Fetches all tabs from the Google Sheet as Pandas DataFrames.
    Handles duplicate and empty column headers by making them unique.
    Read-only. No mutation allowed.
    """
    config = _load_config()
    gs_config = config["google_sheets"]

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = Credentials.from_service_account_file(
        gs_config["credentials_path"],
        scopes=scopes
    )

    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(gs_config["spreadsheet_id"])

    sheets_data = {}
    total_sheets = len(spreadsheet.worksheets())
    
    print(f"📊 Loading {total_sheets} sheets from Google Sheets...")

    for idx, worksheet in enumerate(spreadsheet.worksheets(), 1):
        try:
            sheet_name = worksheet.title
            print(f"   [{idx}/{total_sheets}] Loading '{sheet_name}'...", end=" ")
            
            # Get all values including headers
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) < 2:
                # Skip empty sheets or sheets with only headers
                print("⊘ Empty, skipped")
                continue
            
            # Extract headers and data
            headers = all_values[0]
            data_rows = all_values[1:]
            
            # Make headers unique by appending numbers to duplicates
            unique_headers = []
            header_counts = {}
            
            for header in headers:
                # Handle empty headers
                if not header or header.strip() == '':
                    header = 'Unnamed'
                else:
                    # Strip leading/trailing whitespace from column names
                    header = header.strip()
                
                # Make duplicates unique
                if header in header_counts:
                    header_counts[header] += 1
                    unique_header = f"{header}_{header_counts[header]}"
                else:
                    header_counts[header] = 0
                    unique_header = header
                
                unique_headers.append(unique_header)
            
            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=unique_headers)
            
            # Remove completely empty rows
            df = df.replace('', pd.NA).dropna(how='all')
            
            if df.empty:
                print("⊘ No data, skipped")
                continue
            
            # Apply intelligent type inference
            df = infer_and_convert_types(df)
            
            # Combine Date + Time columns if both exist
            df = combine_date_time_columns(df)
            
            sheets_data[worksheet.title] = df
            print(f"✓ {len(df):,} rows, {len(df.columns)} cols")
            
        except Exception as e:
            print(f"⚠️  Error: {e}")
            continue

    print(f"\n✓ Loaded {len(sheets_data)} sheets successfully")

    if not sheets_data:
        raise RuntimeError("No data found in Google Sheets")

    return sheets_data


def fetch_sheets_with_tables() -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetches all tabs from Google Sheet and detects multiple tables within each sheet.
    
    CRITICAL: This function fetches RAW data WITHOUT type inference first,
    then detects tables, then applies type inference to each detected table.
    This prevents Int64 errors during table detection.
    
    Returns:
        Dict mapping sheet_name to list of detected tables.
        Each table contains:
        - table_id: Unique identifier
        - row_range: (start_row, end_row)
        - col_range: (start_col, end_col)
        - dataframe: Table data with proper headers and types
        - title: Optional table title
        - sheet_name: Source sheet name
    """
    from data_sources.gsheet.table_detection import detect_and_clean_tables
    
    config = _load_config()
    gs_config = config["google_sheets"]

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = Credentials.from_service_account_file(
        gs_config["credentials_path"],
        scopes=scopes
    )

    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(gs_config["spreadsheet_id"])

    sheets_with_tables = {}
    total_sheets = len(spreadsheet.worksheets())
    
    print(f"📊 Loading {total_sheets} sheets from Google Sheets...")

    for idx, worksheet in enumerate(spreadsheet.worksheets(), 1):
        try:
            sheet_name = worksheet.title
            print(f"   [{idx}/{total_sheets}] Loading '{sheet_name}'...", end=" ")
            
            # Get all values including headers
            all_values = worksheet.get_all_values()
            
            if not all_values or len(all_values) < 2:
                # Skip empty sheets or sheets with only headers
                print("⊘ Empty, skipped")
                continue
            
            # Create DataFrame with RAW string data (no type inference yet)
            # IMPORTANT: Do not extract headers here! 
            # The custom detector processing pipeline (detect_and_clean_tables -> clean_detected_tables)
            # expects the headers to be in the first row of the DataFrame body.
            # If we extract headers here, clean_detected_tables will treat the first DATA row as headers.
            raw_df = pd.DataFrame(all_values)
            
            # DO NOT remove empty rows here! The custom detector NEEDS them to separate tables.
            # Empty rows act as separators between tables in the sheet.
            # The table_cleaner will handle empty row removal AFTER detection.
            
            if raw_df.empty:
                print("⊘ No data, skipped")
                continue
            
            # Detect tables in this sheet using RAW data
            print(f"✓ {len(raw_df):,} rows, detecting tables...", end=" ")
            detected_tables = detect_and_clean_tables(raw_df, sheet_name)
            
            # NOW apply type inference and date/time combination to each detected table
            for table in detected_tables:
                table_df = table['dataframe']
                
                # Apply intelligent type inference
                table_df = infer_and_convert_types(table_df)
                
                # Combine Date + Time columns if both exist
                table_df = combine_date_time_columns(table_df)
                
                # Update the dataframe in the table info
                table['dataframe'] = table_df
            
            sheets_with_tables[worksheet.title] = detected_tables
            print(f"✓ Found {len(detected_tables)} table(s)")
            
        except Exception as e:
            import traceback
            print(f"⚠️  Error processing '{sheet_name}': {e}")
            print(f"    Traceback:")
            traceback.print_exc()
            print(f"    → Skipping this sheet")
            continue

    print(f"\n✓ Loaded {len(sheets_with_tables)} sheets successfully")
    print(f"✓ Detected {sum(len(tables) for tables in sheets_with_tables.values())} total tables across {len(sheets_with_tables)} sheets\n")

    if not sheets_with_tables:
        raise RuntimeError("No data found in Google Sheets")

    return sheets_with_tables
