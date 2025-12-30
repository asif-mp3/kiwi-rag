"""
Sheet-Level Change Detection Module
====================================

This module implements hash-based change detection at the SHEET level.
Each sheet is treated as an atomic unit of truth.

Key Design Principles:
- Hash entire raw sheet BEFORE table detection
- Any change to sheet triggers rebuild of ALL tables from that sheet
- Track sheet-level hashes in registry (not table-level fingerprints)
- Use source_id (spreadsheet_id#sheet_name) as atomic unit

Change Detection Logic:
1. Load sheet registry (previous state)
2. Fetch current sheets and compute raw hashes
3. Compare hashes to detect changes
4. Return list of changed sheets for targeted rebuilds
"""

import yaml
import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Any
from data_sources.gsheet.sheet_hasher import (
    load_raw_sheet_with_hash,
    get_source_id,
    compute_sheet_hash
)

SHEET_REGISTRY_FILE = "data_sources/snapshots/sheet_state.json"


def _load_config():
    """Load configuration from settings.yaml"""
    with open("config/settings.yaml") as f:
        return yaml.safe_load(f)


def load_sheet_registry() -> Dict[str, Any]:
    """
    Load sheet-level hash registry from disk.
    
    Returns:
        Dict with structure:
        {
            "spreadsheet_id": "1mRcD...",
            "sheets": {
                "Sales": {
                    "hash": "a1b2c3d4...",
                    "last_synced": "2025-12-30T12:50:09+05:30",
                    "table_count": 16,
                    "source_id": "1mRcD...#Sales"
                },
                ...
            }
        }
    """
    if not Path(SHEET_REGISTRY_FILE).exists():
        return {
            "spreadsheet_id": None,
            "sheets": {}
        }
    
    try:
        with open(SHEET_REGISTRY_FILE, 'r') as f:
            registry = json.load(f)
            
        # Migrate old format if needed
        if "fingerprints" in registry:
            print("   Migrating old table-level fingerprints to sheet-level hashes...")
            registry = _migrate_old_format(registry)
            
        return registry
        
    except Exception as e:
        print(f"⚠️  Could not load sheet registry: {e}")
        return {
            "spreadsheet_id": None,
            "sheets": {}
        }


def _migrate_old_format(old_registry: dict) -> dict:
    """
    Migrate old table-level fingerprint format to new sheet-level hash format.
    
    Old format:
    {
        "spreadsheet_id": "...",
        "sheets": ["Sales", "Month"],
        "fingerprints": {"sales_Table1": "hash1", ...}
    }
    
    New format:
    {
        "spreadsheet_id": "...",
        "sheets": {
            "Sales": {"hash": "...", "last_synced": "...", "table_count": 16},
            ...
        }
    }
    
    NOTE: We cannot preserve old hashes since they were table-level.
    This will trigger a full rebuild on first run after migration.
    """
    spreadsheet_id = old_registry.get("spreadsheet_id")
    old_sheets = old_registry.get("sheets", [])
    
    new_registry = {
        "spreadsheet_id": spreadsheet_id,
        "sheets": {}
    }
    
    # Extract unique sheet names from old fingerprints
    # Old format: "sales_Table1" -> "sales"
    fingerprints = old_registry.get("fingerprints", {})
    for table_name in fingerprints.keys():
        # Extract sheet name (everything before _Table)
        if "_Table" in table_name:
            sheet_name = table_name.split("_Table")[0]
            if sheet_name not in new_registry["sheets"]:
                new_registry["sheets"][sheet_name] = {
                    "hash": None,  # Will trigger rebuild
                    "last_synced": None,
                    "table_count": 0,
                    "source_id": get_source_id(spreadsheet_id, sheet_name) if spreadsheet_id else None
                }
    
    # Also include sheets from old "sheets" list
    for sheet_name in old_sheets:
        if sheet_name not in new_registry["sheets"]:
            new_registry["sheets"][sheet_name] = {
                "hash": None,
                "last_synced": None,
                "table_count": 0,
                "source_id": get_source_id(spreadsheet_id, sheet_name) if spreadsheet_id else None
            }
    
    print(f"   Migrated {len(new_registry['sheets'])} sheets from old format")
    return new_registry


def save_sheet_registry(spreadsheet_id: str, sheet_hashes: Dict[str, Dict[str, Any]]):
    """
    Persist sheet-level hash registry to disk.
    
    Args:
        spreadsheet_id: Google Sheets spreadsheet ID
        sheet_hashes: Dict mapping sheet_name to hash metadata
            {
                "Sales": {
                    "hash": "a1b2c3d4...",
                    "table_count": 16,
                    "source_id": "1mRcD...#Sales"
                },
                ...
            }
    """
    try:
        Path(SHEET_REGISTRY_FILE).parent.mkdir(parents=True, exist_ok=True)
        
        # Add timestamp to each sheet
        current_time = datetime.now().isoformat()
        for sheet_name, metadata in sheet_hashes.items():
            metadata["last_synced"] = current_time
        
        registry = {
            "spreadsheet_id": spreadsheet_id,
            "sheets": sheet_hashes
        }
        
        with open(SHEET_REGISTRY_FILE, 'w') as f:
            json.dump(registry, f, indent=2)
            
        print(f"✓ Sheet registry saved for {len(sheet_hashes)} sheet(s)")
        
    except Exception as e:
        print(f"⚠️  Could not save sheet registry: {e}")


def compute_current_sheet_hashes(sheets_with_tables: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    """
    Extract sheet hashes from sheets_with_tables data.
    
    The sheets_with_tables structure already contains computed hashes
    (added by fetch_sheets_with_tables in connector.py).
    
    Args:
        sheets_with_tables: Dict mapping sheet_name to list of table dicts
            Each sheet should have a 'sheet_hash' key in metadata
    
    Returns:
        Dict mapping sheet_name to hash metadata
    """
    config = _load_config()
    spreadsheet_id = config["google_sheets"]["spreadsheet_id"]
    
    sheet_hashes = {}
    
    for sheet_name, tables in sheets_with_tables.items():
        # Extract hash from first table's metadata (all tables from same sheet have same hash)
        if tables and len(tables) > 0:
            sheet_hash = tables[0].get('sheet_hash')
            
            if not sheet_hash:
                # Fallback: compute hash from raw data if not present
                print(f"   ⚠️  No sheet_hash found for '{sheet_name}', this shouldn't happen")
                sheet_hash = "UNKNOWN"
            
            sheet_hashes[sheet_name] = {
                "hash": sheet_hash,
                "table_count": len(tables),
                "source_id": get_source_id(spreadsheet_id, sheet_name)
            }
    
    return sheet_hashes


def get_changed_sheets(old_registry: dict, current_sheet_hashes: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Identify which sheets have changed based on hash comparison.
    
    A sheet is considered changed if:
    - Hash is different from stored hash
    - Sheet is new (not in old registry)
    - Hash is missing (None) in old registry
    
    Args:
        old_registry: Previously stored registry
        current_sheet_hashes: Current sheet hashes
    
    Returns:
        List of sheet names that have changed
    """
    changed_sheets = []
    old_sheets = old_registry.get("sheets", {})
    
    for sheet_name, current_metadata in current_sheet_hashes.items():
        current_hash = current_metadata["hash"]
        
        if sheet_name not in old_sheets:
            # New sheet
            changed_sheets.append(sheet_name)
            print(f"   🆕 New sheet detected: '{sheet_name}'")
            continue
        
        old_metadata = old_sheets[sheet_name]
        old_hash = old_metadata.get("hash")
        
        if old_hash is None:
            # No previous hash (first run or migration)
            changed_sheets.append(sheet_name)
            print(f"   🔄 No previous hash for '{sheet_name}' (first run)")
            continue
        
        if current_hash != old_hash:
            # Hash changed
            changed_sheets.append(sheet_name)
            print(f"   🔄 Content changed in sheet '{sheet_name}'")
            print(f"      Old: {old_hash[:16]}...")
            print(f"      New: {current_hash[:16]}...")
    
    # Check for deleted sheets
    for sheet_name in old_sheets.keys():
        if sheet_name not in current_sheet_hashes:
            print(f"   🗑️  Sheet deleted: '{sheet_name}'")
            # Note: Deleted sheets don't need rebuild, but we should clean up their tables
            # This will be handled by the cleanup logic
    
    return changed_sheets


def needs_refresh(sheets_with_tables: Dict[str, List[Dict[str, Any]]]) -> Tuple[bool, bool, List[str]]:
    """
    Check if sheets need refresh and determine rebuild strategy.
    
    Args:
        sheets_with_tables: Pre-fetched sheets with detected tables and computed hashes
    
    Returns:
        Tuple of (needs_refresh, full_reset_required, changed_sheets)
        - needs_refresh: True if any sheets changed
        - full_reset_required: True if spreadsheet ID changed or first run
        - changed_sheets: List of sheet names that changed (empty if full reset)
    """
    try:
        # Load stored registry
        old_registry = load_sheet_registry()
        
        # Get current spreadsheet ID from config
        config = _load_config()
        current_spreadsheet_id = config["google_sheets"]["spreadsheet_id"]
        old_spreadsheet_id = old_registry.get("spreadsheet_id")
        
        # Check if spreadsheet ID changed (user switched to different spreadsheet)
        if old_spreadsheet_id and old_spreadsheet_id != current_spreadsheet_id:
            print("🔄 Spreadsheet ID changed - FULL RESET REQUIRED")
            print(f"   Old: {old_spreadsheet_id}")
            print(f"   New: {current_spreadsheet_id}")
            return True, True, []
        
        # First run - no previous state
        if not old_registry.get("sheets"):
            print("🔍 No previous state found (first run) - FULL RESET REQUIRED")
            return True, True, []
        
        # Compute current sheet hashes
        current_sheet_hashes = compute_current_sheet_hashes(sheets_with_tables)
        
        # Identify changed sheets
        changed_sheets = get_changed_sheets(old_registry, current_sheet_hashes)
        
        if not changed_sheets:
            print("✓ No sheet changes detected (all hashes match)")
            return False, False, []
        
        # Changes detected - incremental rebuild
        print(f"🔄 {len(changed_sheets)} sheet(s) changed - INCREMENTAL REBUILD")
        return True, False, changed_sheets
        
    except Exception as e:
        # Safe default: full reset on error
        print(f"⚠️  Could not check for changes: {e}")
        import traceback
        traceback.print_exc()
        return True, True, []


def mark_synced(sheets_with_tables: Dict[str, List[Dict[str, Any]]]):
    """
    Mark sheets as synced by storing their current hashes.
    
    This should be called AFTER successful DuckDB and ChromaDB updates.
    
    Args:
        sheets_with_tables: Dict mapping sheet_name to list of table dicts
    """
    try:
        config = _load_config()
        spreadsheet_id = config["google_sheets"]["spreadsheet_id"]
        
        # Compute current sheet hashes
        sheet_hashes = compute_current_sheet_hashes(sheets_with_tables)
        
        # Save to registry
        save_sheet_registry(spreadsheet_id, sheet_hashes)
        
        # Count total tables
        total_tables = sum(len(tables) for tables in sheets_with_tables.values())
        print(f"✓ Marked {len(sheet_hashes)} sheet(s) as synced ({total_tables} total tables)")
        
    except Exception as e:
        print(f"⚠️  Could not mark sheets as synced: {e}")


# Backward compatibility: Keep old function names as aliases
def load_sheet_state():
    """Backward compatibility alias for load_sheet_registry"""
    return load_sheet_registry()


def save_sheet_state(sheets: list, fingerprints: dict):
    """
    Backward compatibility function (deprecated).
    
    This function is kept for compatibility but should not be used.
    Use save_sheet_registry() instead.
    """
    print("⚠️  save_sheet_state() is deprecated, use save_sheet_registry() instead")
