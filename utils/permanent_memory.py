"""
Permanent Memory Module

Manages persistent JSON-based memory for user preferences and bot identity.
This module is used by the prompt layer to inject behavioral constraints.

CRITICAL RULES:
- Load memory on every query
- Never infer or hallucinate memory
- Only write when user explicitly instructs
- Atomic file operations
- No auto-learning
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Memory file path
MEMORY_FILE = "data_sources/persistent_memory.json"


def load_memory() -> Dict[str, Any]:
    """
    Load permanent memory from disk.
    
    Returns:
        Dict containing memory structure:
        {
            "user_preferences": {...},
            "bot_identity": {...},
            "meta": {...}
        }
        
        Returns empty structure if file doesn't exist.
    """
    memory_path = Path(MEMORY_FILE)
    
    if not memory_path.exists():
        # Return empty memory structure (do not create file)
        return {
            "user_preferences": {},
            "bot_identity": {},
            "meta": {}
        }
    
    try:
        with open(memory_path, 'r', encoding='utf-8') as f:
            memory = json.load(f)
            
        # Validate structure
        if not isinstance(memory, dict):
            print(f"Warning: Invalid memory structure, using empty memory")
            return {
                "user_preferences": {},
                "bot_identity": {},
                "meta": {}
            }
        
        # Ensure required keys exist
        if "user_preferences" not in memory:
            memory["user_preferences"] = {}
        if "bot_identity" not in memory:
            memory["bot_identity"] = {}
        if "meta" not in memory:
            memory["meta"] = {}
            
        return memory
        
    except json.JSONDecodeError as e:
        print(f"Warning: Failed to parse memory JSON: {e}")
        return {
            "user_preferences": {},
            "bot_identity": {},
            "meta": {}
        }
    except Exception as e:
        print(f"Warning: Failed to load memory: {e}")
        return {
            "user_preferences": {},
            "bot_identity": {},
            "meta": {}
        }


def save_memory(memory: Dict[str, Any]) -> bool:
    """
    Save memory to disk atomically.
    
    Args:
        memory: Memory dictionary to save
        
    Returns:
        True if successful, False otherwise
    """
    memory_path = Path(MEMORY_FILE)
    
    # Ensure directory exists
    memory_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Update metadata
    if "meta" not in memory:
        memory["meta"] = {}
    
    if "created_at" not in memory["meta"]:
        memory["meta"]["created_at"] = datetime.now().isoformat()
    
    memory["meta"]["last_updated"] = datetime.now().isoformat()
    
    try:
        # Atomic write: write to temp file, then rename
        temp_path = memory_path.with_suffix('.tmp')
        
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(memory, f, indent=2, ensure_ascii=False)
        
        # Atomic rename
        temp_path.replace(memory_path)
        
        return True
        
    except Exception as e:
        print(f"Error: Failed to save memory: {e}")
        return False


def update_memory(category: str, key: str, value: Any) -> bool:
    """
    Update a specific memory field.
    
    Args:
        category: "user_preferences" or "bot_identity"
        key: Field name (e.g., "address_as", "name")
        value: Value to store
        
    Returns:
        True if successful, False otherwise
    """
    if category not in ["user_preferences", "bot_identity"]:
        print(f"Error: Invalid category '{category}'")
        return False
    
    # Load existing memory
    memory = load_memory()
    
    # Update field
    memory[category][key] = value
    
    # Save
    return save_memory(memory)


def format_memory_for_prompt() -> str:
    """
    Format memory as behavioral constraints for prompt injection.
    
    Returns:
        String to inject into system prompt, or empty string if no memory.
    """
    memory = load_memory()
    
    constraints = []
    
    # User preferences
    user_prefs = memory.get("user_preferences", {})
    if user_prefs.get("address_as"):
        constraints.append(f"- Address the user as \"{user_prefs['address_as']}\"")
    
    # Bot identity
    bot_identity = memory.get("bot_identity", {})
    if bot_identity.get("name"):
        constraints.append(f"- Your name is \"{bot_identity['name']}\"")
    
    if not constraints:
        return ""
    
    # Format as prompt section
    prompt_section = "\n\nIMPORTANT BEHAVIORAL CONSTRAINTS FROM USER MEMORY:\n"
    prompt_section += "\n".join(constraints)
    prompt_section += "\n"
    
    return prompt_section


def get_memory_summary() -> str:
    """
    Get human-readable summary of current memory.
    
    Returns:
        Summary string for debugging/display
    """
    memory = load_memory()
    
    summary_parts = []
    
    user_prefs = memory.get("user_preferences", {})
    if user_prefs:
        summary_parts.append(f"User preferences: {user_prefs}")
    
    bot_identity = memory.get("bot_identity", {})
    if bot_identity:
        summary_parts.append(f"Bot identity: {bot_identity}")
    
    if not summary_parts:
        return "No memory stored"
    
    return " | ".join(summary_parts)


def clear_memory() -> bool:
    """
    Clear all memory (for testing/reset).
    
    Returns:
        True if successful
    """
    empty_memory = {
        "user_preferences": {},
        "bot_identity": {},
        "meta": {
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat()
        }
    }
    
    return save_memory(empty_memory)
