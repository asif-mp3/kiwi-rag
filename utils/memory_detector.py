"""
Memory Intent Detector

Uses Gemini AI to detect when user wants to store permanent memory.
Language-agnostic: works in English, Tamil, Hindi, and any language.

CRITICAL RULES:
- Only detect EXPLICIT memory instructions
- Never trigger on casual mentions or questions
- Extract and normalize instructions
- Semantic detection, not keyword-based
"""

import os
import google.generativeai as genai
from dotenv import load_dotenv
from typing import Optional, Dict, Any
import json

load_dotenv()

# Detection prompt
MEMORY_DETECTION_PROMPT = """You are a memory intent detector for a conversational AI system.

Your ONLY job is to detect if the user is explicitly instructing the system to remember something permanently.

## Detection Rules

TRIGGER memory detection when user:
- Explicitly says "remember" or equivalent in any language
- Uses phrases like "from now on", "always", "call me", "address me as", "your name is"
- Gives permanent instructions about preferences or identity

Examples that MUST trigger:
- "Call me madam, remember"
- "Your name is Kiwi"
- "From now on, address me as sir"
- "Inime enna madam nu dhan koopduva, nyabagam vechiko" (Tamil)
- "Yaad rakhna, mujhe sir bulana" (Hindi)

DO NOT trigger on:
- Questions: "What is madam?"
- Casual mentions: "Tell me about Kiwi fruit"
- Temporary context: "For this query, use..."
- Polite requests without permanence: "Can you help me?"

## Output Format

You must output ONLY valid JSON:

If memory intent detected:
{
  "has_memory_intent": true,
  "category": "user_preferences" | "bot_identity",
  "key": "address_as" | "name",
  "value": "extracted_value",
  "confidence": 0.0-1.0
}

If NO memory intent:
{
  "has_memory_intent": false
}

## Normalization Rules

- "Call me madam" → category: "user_preferences", key: "address_as", value: "madam"
- "Your name is Kiwi" → category: "bot_identity", key: "name", value: "Kiwi"
- "Address me as sir" → category: "user_preferences", key: "address_as", value: "sir"

Extract ONLY the core value, not the full sentence.

Output ONLY JSON. No explanations."""


def detect_memory_intent(question: str) -> Optional[Dict[str, Any]]:
    """
    Detect if user question contains memory storage intent.
    
    Args:
        question: User's question/statement
        
    Returns:
        Dict with detection result, or None if detection fails
        {
            "has_memory_intent": bool,
            "category": str,  # if has_memory_intent
            "key": str,       # if has_memory_intent
            "value": str,     # if has_memory_intent
            "confidence": float  # if has_memory_intent
        }
    """
    try:
        # Get API key
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            print("Warning: GEMINI_API_KEY not found, memory detection disabled")
            return {"has_memory_intent": False}
        
        # Configure Gemini
        genai.configure(api_key=api_key)
        
        # Create model with JSON output
        model = genai.GenerativeModel(
            model_name="gemini-2.0-flash-exp",  # Fast model for detection
            generation_config={
                "temperature": 0.0,
                "response_mime_type": "application/json"
            },
            system_instruction=MEMORY_DETECTION_PROMPT
        )
        
        # Build prompt
        user_prompt = f"User input: {question}\n\nDetect memory intent and output JSON:"
        
        # Call API
        response = model.generate_content(user_prompt)
        
        # Parse JSON response
        result = json.loads(response.text)
        
        # Validate structure
        if not isinstance(result, dict):
            return {"has_memory_intent": False}
        
        if not result.get("has_memory_intent", False):
            return {"has_memory_intent": False}
        
        # Validate required fields for positive detection
        required_fields = ["category", "key", "value"]
        if not all(field in result for field in required_fields):
            print(f"Warning: Incomplete memory detection result: {result}")
            return {"has_memory_intent": False}
        
        # Validate category
        if result["category"] not in ["user_preferences", "bot_identity"]:
            print(f"Warning: Invalid category: {result['category']}")
            return {"has_memory_intent": False}
        
        return result
        
    except json.JSONDecodeError as e:
        print(f"Warning: Failed to parse memory detection JSON: {e}")
        return {"has_memory_intent": False}
    except Exception as e:
        print(f"Warning: Memory detection failed: {e}")
        return {"has_memory_intent": False}


def extract_memory_instruction(question: str) -> Optional[Dict[str, str]]:
    """
    Extract and normalize memory instruction from user question.
    
    This is a convenience wrapper around detect_memory_intent.
    
    Args:
        question: User's question/statement
        
    Returns:
        Dict with normalized instruction, or None if no memory intent
        {
            "category": "user_preferences" | "bot_identity",
            "key": "address_as" | "name",
            "value": "extracted_value"
        }
    """
    result = detect_memory_intent(question)
    
    if not result or not result.get("has_memory_intent"):
        return None
    
    return {
        "category": result["category"],
        "key": result["key"],
        "value": result["value"]
    }
