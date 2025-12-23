import gspread
import yaml
import json
import hashlib
import pandas as pd
from google.oauth2.service_account import Credentials
from pathlib import Path
from data_sources.gsheet.connector import fetch_sheets

SHEET_STATE_FILE = "data_sources/snapshots/sheet_state.json"
# Legacy fingerprint file for backward compatibility
FINGERPRINT_FILE = "data_sources/snapshots/fingerprints.json"


def _load_config():
    with open("config/settings.yaml") as f:
        return yaml.safe_load(f)


def compute_table_fingerprint(df: pd.DataFrame) -> str:
    """
    Compute deterministic SHA-256 hash of DataFrame content.
    Detects ANY cell-level change (edits, additions, deletions).
    
    Normalization ensures:
    - Same data = same hash (deterministic)
    - Different data = different hash (sensitive)
    - Row/column order doesn't affect hash
    """
    if df.empty:
        return hashlib.sha256(b"").hexdigest()
    
    # Create a copy to avoid modifying original
    df_copy = df.copy()
    
    # Convert Int64 (nullable int) to regular int64 to avoid dtype errors
    for col in df_copy.columns:
        if df_copy[col].dtype.name == 'Int64':
            df_copy[col] = df_copy[col].astype('float64')  # Use float to preserve NaN
    
    # Normalize DataFrame for stable hashing
    normalized = (
        df_copy.fillna("")  # Consistent null handling
          .astype(str)  # Convert all to strings
          .sort_index(axis=0)  # Sort rows by index
          .sort_index(axis=1)  # Sort columns alphabetically
    )
    
    # Serialize to string with delimiters
    content = "|".join(
        normalized.apply(lambda row: "§".join(row), axis=1)
    )
    
    # Hash with SHA-256
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def load_sheet_state() -> dict:
    """
    Load stored sheet state (spreadsheet_id + sheet list + fingerprints) from disk.
    Returns dict with 'spreadsheet_id', 'sheets' list and 'fingerprints' dict.
    Falls back to legacy fingerprints.json if sheet_state.json doesn't exist.
    """
    # Try new format first
    if Path(SHEET_STATE_FILE).exists():
        try:
            with open(SHEET_STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Could not load sheet state: {e}")
    
    # Fall back to legacy format
    if Path(FINGERPRINT_FILE).exists():
        try:
            with open(FINGERPRINT_FILE, 'r') as f:
                fingerprints = json.load(f)
                # Convert to new format (no spreadsheet_id in legacy)
                return {
                    "spreadsheet_id": None,
                    "sheets": list(fingerprints.keys()),
                    "fingerprints": fingerprints
                }
        except Exception as e:
            print(f"⚠️  Could not load legacy fingerprints: {e}")
    
    return {"spreadsheet_id": None, "sheets": [], "fingerprints": {}}


def save_sheet_state(sheets: list, fingerprints: dict):
    """Persist sheet state (spreadsheet_id + sheet list + fingerprints) to disk"""
    try:
        Path(SHEET_STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
        
        # Get current spreadsheet ID from config
        config = _load_config()
        spreadsheet_id = config["google_sheets"]["spreadsheet_id"]
        
        state = {
            "spreadsheet_id": spreadsheet_id,
            "sheets": sorted(sheets),  # Sort for consistent comparison
            "fingerprints": fingerprints
        }
        
        with open(SHEET_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
            
    except Exception as e:
        print(f"⚠️  Could not save sheet state: {e}")


def detect_sheet_structure_change(old_state: dict, current_sheets: dict) -> tuple[bool, list]:
    """
    Detect if sheet structure has changed (add/delete/rename).
    Returns (structure_changed, changes_list).
    
    Changes detected:
    - New sheet added
    - Existing sheet deleted
    - Sheet renamed (detected as delete + add)
    """
    old_sheets = set(old_state.get("sheets", []))
    current_sheet_names = set(current_sheets.keys())
    
    if old_sheets == current_sheet_names:
        return False, []
    
    changes = []
    
    # Detect additions
    added = current_sheet_names - old_sheets
    for sheet in added:
        changes.append(f"Added: '{sheet}'")
    
    # Detect deletions
    deleted = old_sheets - current_sheet_names
    for sheet in deleted:
        changes.append(f"Deleted: '{sheet}'")
    
    return True, changes


def compute_current_fingerprints(sheets: dict) -> dict:
    """Compute fingerprints for given sheets"""
    fingerprints = {}
    for sheet_name, df in sheets.items():
        fingerprints[sheet_name] = compute_table_fingerprint(df)
    return fingerprints


def get_sheet_names() -> list:
    """
    Get list of sheet names (lightweight, no data download).
    Much faster than downloading all sheet data.
    """
    try:
        config = _load_config()
        gs_config = config["google_sheets"]
        
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        credentials = Credentials.from_service_account_file(
            gs_config["credentials_path"],
            scopes=scopes
        )
        
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(gs_config["spreadsheet_id"])
        
        # Get just the sheet names (fast!)
        sheet_names = sorted([ws.title for ws in spreadsheet.worksheets()])
        
        return sheet_names
    except Exception as e:
        print(f"⚠️  Could not fetch sheet names: {e}")
        return None



def needs_refresh() -> tuple[bool, bool, dict]:
    """
    Check if sheets need refresh and if full reset is required.
    Returns (needs_refresh, full_reset_required, current_sheets) tuple.
    
    HASH-BASED DETECTION: Always fetches data and compares fingerprints.
    This detects ANY changes (content or structure) automatically.
    """
    try:
        # Load stored state
        old_state = load_sheet_state()
        
        # Get current spreadsheet ID from config
        config = _load_config()
        current_spreadsheet_id = config["google_sheets"]["spreadsheet_id"]
        old_spreadsheet_id = old_state.get("spreadsheet_id")
        
        # Check if spreadsheet ID changed (user switched to different sheet)
        if old_spreadsheet_id and old_spreadsheet_id != current_spreadsheet_id:
            print("🔄 Spreadsheet ID changed - FULL RESET REQUIRED")
            print(f"   Old: {old_spreadsheet_id}")
            print(f"   New: {current_spreadsheet_id}")
            # Fetch new sheets and trigger full reset
            sheets = fetch_sheets()
            return True, True, sheets
        
        # ALWAYS fetch data and compute fingerprints to detect ANY changes
        print("🔍 Checking for changes (comparing content hashes)...")
        sheets = fetch_sheets()
        current_fingerprints = compute_current_fingerprints(sheets)
        
        # First run - no previous state
        if not old_state.get("sheets"):
            print("🔍 No previous state found (first run)")
            return True, True, sheets
        
        # Check for sheet structure changes
        structure_changed, changes = detect_sheet_structure_change(old_state, sheets)
        
        if structure_changed:
            print("🔄 Sheet structure changed - FULL RESET REQUIRED")
            for change in changes:
                print(f"   {change}")
            return True, True, sheets
        
        # No structure change - check content fingerprints
        old_fingerprints = old_state.get("fingerprints", {})
        
        for sheet_name, current_fp in current_fingerprints.items():
            old_fp = old_fingerprints.get(sheet_name)
            
            if old_fp != current_fp:
                print(f"🔄 Content changed in sheet '{sheet_name}'")
                print(f"   Old: {old_fp[:16] if old_fp else 'None'}...")
                print(f"   New: {current_fp[:16]}...")
                return True, False, sheets  # Content change only, no full reset
        
        print("✓ No content changes detected (fingerprints match)")
        return False, False, sheets
        
    except Exception as e:
        # Safe default: full reset on error
        print(f"⚠️  Could not check for changes: {e}")
        try:
            sheets = fetch_sheets()
            return True, True, sheets
        except:
            return True, True, {}


def mark_synced(sheets: dict):
    """
    Store sheet state (sheet list + fingerprints) for the given sheets.
    IMPORTANT: Uses the same sheets data that was already fetched.
    """
    try:
        fingerprints = compute_current_fingerprints(sheets)
        sheet_names = list(sheets.keys())
        save_sheet_state(sheet_names, fingerprints)
        print(f"✓ Sheet state saved for {len(sheet_names)} sheet(s)")
        
    except Exception as e:
        print(f"⚠️  Could not save sheet state: {e}")
