import gspread
import pandas as pd
import numpy as np
import yaml
from google.oauth2.service_account import Credentials


def _load_config():
    with open("config/settings.yaml") as f:
        return yaml.safe_load(f)


def infer_and_convert_types(df):
    """
    Intelligently infer and convert data types from string data.
    Converts numeric strings to INT/FLOAT, dates to datetime, booleans to bool.
    """
    for col in df.columns:
        # Skip if already numeric
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
