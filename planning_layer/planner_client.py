import os
import json
import yaml
import google.generativeai as genai
from pathlib import Path
from planning_layer.planner_prompt import PLANNER_SYSTEM_PROMPT
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def load_config():
    """Load LLM configuration from settings.yaml"""
    config_path = Path("config/settings.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return config.get("llm", {})


def initialize_gemini_client(config):
    """Initialize Gemini API client with configuration"""
    api_key_env = config.get("api_key_env", "GEMINI_API_KEY")
    api_key = os.getenv(api_key_env)
    
    if not api_key:
        raise ValueError(
            f"Gemini API key not found. Please set the {api_key_env} environment variable."
        )
    
    genai.configure(api_key=api_key)
    
    model_name = config.get("model", "gemini-2.0-flash-exp")
    temperature = config.get("temperature", 0.0)
    
    generation_config = {
        "temperature": temperature,
        "response_mime_type": "application/json",
    }
    
    model = genai.GenerativeModel(
        model_name=model_name,
        generation_config=generation_config,
        system_instruction=PLANNER_SYSTEM_PROMPT
    )
    
    return model


def format_schema_context(schema_context: list) -> str:
    """Format schema context for LLM prompt"""
    if not schema_context:
        return "No schema context available."
    
    formatted = "Available Schema:\n\n"
    
    for item in schema_context:
        formatted += f"- {item.get('text', '')}\n"
    
    return formatted


def parse_json_response(response_text: str) -> dict:
    """Parse JSON from LLM response, handling potential formatting issues"""
    # Remove markdown code blocks if present
    text = response_text.strip()
    if text.startswith("```"):
        # Extract content between code blocks
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
        if text.startswith("json"):
            text = text[4:].strip()
    
    # Parse JSON
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON from LLM response: {e}\nResponse: {text}")


def generate_plan(question: str, schema_context: list, max_retries: int = None) -> dict:
    """
    Generate query plan using Gemini LLM.
    
    Args:
        question: User's natural language question
        schema_context: List of schema documents from ChromaDB retrieval
        max_retries: Maximum number of retry attempts (defaults to config value)
    
    Returns:
        dict: Query plan matching plan_schema.json
    
    Raises:
        ValueError: If API key is missing, JSON parsing fails, or max retries exceeded
    
    CRITICAL: This function ONLY proposes intent. It does NOT:
    - Execute queries
    - Validate plans (done by plan_validator.py)
    - Generate SQL (done by sql_compiler.py)
    - Access data (done by executor.py)
    """
    
    # Load configuration
    config = load_config()
    if max_retries is None:
        max_retries = config.get("max_retries", 3)
    
    # Initialize Gemini client
    model = initialize_gemini_client(config)
    
    # Format schema context
    schema_text = format_schema_context(schema_context)
    
    # Build user prompt
    user_prompt = f"""{schema_text}

User Question: {question}

Output the query plan as JSON:"""
    
    # Retry logic for API failures
    last_error = None
    for attempt in range(max_retries):
        try:
            # Call Gemini API
            response = model.generate_content(user_prompt)
            
            # Extract text from response
            response_text = response.text
            
            # Parse JSON
            plan = parse_json_response(response_text)
            
            # Basic structure check (detailed validation happens in validator)
            if not isinstance(plan, dict):
                raise ValueError(f"LLM response is not a JSON object: {type(plan)}")
            
            if "query_type" not in plan:
                raise ValueError("LLM response missing required field: query_type")
            
            if "table" not in plan:
                raise ValueError("LLM response missing required field: table")
            
            return plan
            
        except json.JSONDecodeError as e:
            last_error = e
            if attempt < max_retries - 1:
                continue  # Retry
            else:
                raise ValueError(f"Failed to parse valid JSON after {max_retries} attempts: {e}")
        
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                continue  # Retry
            else:
                raise ValueError(f"Failed to generate plan after {max_retries} attempts: {e}")
    
    # Should never reach here, but just in case
    raise ValueError(f"Failed to generate plan: {last_error}")
