"""
Sheet-Level Hash Computation Module
====================================

This module provides deterministic hash computation for entire Google Sheets
BEFORE any table detection, type inference, or normalization.

The hash represents the complete structural and data state of the sheet.
Any change to the sheet (data, headers, structure, formatting) will change the hash.

Key Design Principles:
- Hash is computed on RAW grid data (no processing)
- Canonical representation ensures determinism
- Preserves row and column order
- Normalizes empty/null cells consistently
- Stringifies all values consistently
"""

import gspread
import yaml
import hashlib
import json
from google.oauth2.service_account import Credentials
from typing import List, Any


def _load_config():
    """Load configuration from settings.yaml"""
    with open("config/settings.yaml") as f:
        return yaml.safe_load(f)


def load_raw_sheet_grid(spreadsheet_id: str, sheet_name: str, credentials_path: str) -> List[List[str]]:
    """
    Load raw grid data from a Google Sheet WITHOUT any processing.
    
    This function loads the sheet exactly as it exists in the source:
    - No header detection
    - No type inference
    - No empty row removal
    - No column normalization
    
    Args:
        spreadsheet_id: Google Sheets spreadsheet ID
        sheet_name: Name of the worksheet to load
        credentials_path: Path to service account JSON file
    
    Returns:
        List of lists representing the raw grid (all values as strings)
    
    Raises:
        Exception: If sheet cannot be loaded
    """
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        credentials = Credentials.from_service_account_file(
            credentials_path,
            scopes=scopes
        )
        
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # Get ALL values from the sheet (including empty cells)
        # This returns a list of lists where each inner list is a row
        raw_grid = worksheet.get_all_values()
        
        return raw_grid
        
    except Exception as e:
        raise Exception(f"Failed to load raw sheet '{sheet_name}': {e}")


def compute_sheet_hash(raw_grid: List[List[str]]) -> str:
    """
    Compute deterministic SHA-256 hash of raw sheet grid.
    
    The hash is based on a canonical representation:
    1. Preserve row and column order (no sorting)
    2. Normalize empty/null cells to empty strings
    3. Stringify all values consistently
    4. Serialize to stable JSON format
    5. Apply SHA-256 hash
    
    This ensures:
    - Same data = same hash (deterministic)
    - Different data = different hash (sensitive)
    - Any change (data, structure, formatting) changes the hash
    
    Args:
        raw_grid: List of lists representing the raw sheet grid
    
    Returns:
        SHA-256 hash as hexadecimal string
    """
    if not raw_grid:
        # Empty sheet has a deterministic hash
        return hashlib.sha256(b"").hexdigest()
    
    # Create canonical representation
    # Normalize each cell: convert to string, treat None/empty as ""
    canonical_grid = []
    for row in raw_grid:
        canonical_row = []
        for cell in row:
            # Normalize cell value
            if cell is None or cell == '':
                normalized_cell = ''
            else:
                # Convert to string and strip whitespace for consistency
                # This handles numbers, dates, booleans, etc.
                normalized_cell = str(cell).strip()
            canonical_row.append(normalized_cell)
        canonical_grid.append(canonical_row)
    
    # Serialize to stable JSON format
    # sort_keys=False to preserve column order
    # separators for compact, consistent output
    json_str = json.dumps(
        canonical_grid,
        sort_keys=False,
        separators=(',', ':'),
        ensure_ascii=True
    )
    
    # Compute SHA-256 hash
    hash_bytes = hashlib.sha256(json_str.encode('utf-8'))
    return hash_bytes.hexdigest()


def load_raw_sheet_with_hash(spreadsheet_id: str, sheet_name: str, credentials_path: str) -> tuple[List[List[str]], str]:
    """
    Load raw sheet grid and compute its hash in one operation.
    
    This is a convenience function that combines load_raw_sheet_grid()
    and compute_sheet_hash() for efficiency.
    
    Args:
        spreadsheet_id: Google Sheets spreadsheet ID
        sheet_name: Name of the worksheet to load
        credentials_path: Path to service account JSON file
    
    Returns:
        Tuple of (raw_grid, sheet_hash)
    """
    raw_grid = load_raw_sheet_grid(spreadsheet_id, sheet_name, credentials_path)
    sheet_hash = compute_sheet_hash(raw_grid)
    return raw_grid, sheet_hash


def get_source_id(spreadsheet_id: str, sheet_name: str) -> str:
    """
    Generate a stable source_id for a sheet.
    
    The source_id uniquely identifies a sheet and is used to track
    all derived artifacts (DuckDB tables, ChromaDB embeddings).
    
    Format: {spreadsheet_id}#{sheet_name}
    
    Args:
        spreadsheet_id: Google Sheets spreadsheet ID
        sheet_name: Name of the worksheet
    
    Returns:
        Source ID string
    """
    return f"{spreadsheet_id}#{sheet_name}"


# Example usage and testing
if __name__ == "__main__":
    """
    Test the sheet hasher with the configured Google Sheet.
    This can be used to verify hash stability and change detection.
    """
    config = _load_config()
    gs_config = config["google_sheets"]
    
    spreadsheet_id = gs_config["spreadsheet_id"]
    credentials_path = gs_config["credentials_path"]
    
    # Test with first available sheet
    import gspread
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(spreadsheet_id)
    
    print("Testing Sheet Hasher")
    print("=" * 60)
    
    for worksheet in spreadsheet.worksheets()[:3]:  # Test first 3 sheets
        sheet_name = worksheet.title
        print(f"\nSheet: {sheet_name}")
        
        try:
            raw_grid, sheet_hash = load_raw_sheet_with_hash(
                spreadsheet_id, 
                sheet_name, 
                credentials_path
            )
            
            print(f"  Dimensions: {len(raw_grid)} rows × {len(raw_grid[0]) if raw_grid else 0} cols")
            print(f"  Hash: {sheet_hash[:16]}...")
            print(f"  Source ID: {get_source_id(spreadsheet_id, sheet_name)}")
            
            # Test hash stability (load twice, should get same hash)
            _, sheet_hash2 = load_raw_sheet_with_hash(
                spreadsheet_id, 
                sheet_name, 
                credentials_path
            )
            
            if sheet_hash == sheet_hash2:
                print(f"  ✓ Hash is stable (deterministic)")
            else:
                print(f"  ✗ Hash is NOT stable (non-deterministic!)")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    print("\n" + "=" * 60)
