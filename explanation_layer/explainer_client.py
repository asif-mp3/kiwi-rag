"""
Explanation Layer Client

Generates natural language explanations from verified query results using Gemini API.
Implements strict hallucination prevention and validation.
"""

import os
import re
import yaml
import pandas as pd
import google.generativeai as genai
from pathlib import Path
from dotenv import load_dotenv
from explanation_layer.explanation_prompt import EXPLANATION_SYSTEM_PROMPT

# Load environment variables
load_dotenv()


def load_config():
    """Load LLM configuration from settings.yaml"""
    config_path = Path("config/settings.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return config.get("llm", {})


def initialize_gemini_client(config):
    """Initialize Gemini API client for explanation generation"""
    api_key_env = config.get("api_key_env", "GEMINI_API_KEY")
    api_key = os.getenv(api_key_env)
    
    if not api_key:
        raise ValueError(
            f"Gemini API key not found. Please set the {api_key_env} environment variable."
        )
    
    genai.configure(api_key=api_key)
    
    model_name = config.get("model", "gemini-2.0-flash-exp")
    
    # CRITICAL: Force temperature to 0 for explanation to prevent hallucinations
    generation_config = {
        "temperature": 0.0,
        "top_p": 0.1,  # Low top_p for more deterministic outputs
        "response_mime_type": "text/plain",
    }
    
    model = genai.GenerativeModel(
        model_name=model_name,
        generation_config=generation_config,
        system_instruction=EXPLANATION_SYSTEM_PROMPT
    )
    
    return model


def _format_dataframe_for_prompt(df: pd.DataFrame) -> str:
    """
    Convert DataFrame to human-readable text representation.
    Preserves row order (critical for ranking questions).
    """
    if df.empty:
        return "EMPTY RESULT (no rows returned)"
    
    # For single value results (aggregations)
    if len(df) == 1 and len(df.columns) == 1:
        col_name = df.columns[0]
        value = df[col_name].iloc[0]
        return f"{col_name}: {value}"
    
    # For single row with multiple columns
    if len(df) == 1:
        parts = []
        for col in df.columns:
            value = df[col].iloc[0]
            parts.append(f"{col}: {value}")
        return "\n".join(parts)
    
    # For multiple rows, format as a table
    result_text = "Result rows (in order):\n\n"
    
    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        result_text += f"Row {idx}:\n"
        for col in df.columns:
            value = row[col]
            result_text += f"  {col}: {value}\n"
        result_text += "\n"
    
    return result_text.strip()


def _format_schema_context(schema_context: list) -> str:
    """Format schema context as bullet list"""
    if not schema_context:
        return "No schema context available."
    
    formatted = "Schema context (for understanding only, NOT authoritative):\n\n"
    for item in schema_context:
        # Handle both string items and dict items with 'text' key
        if isinstance(item, dict):
            text = item.get('text', str(item))
        else:
            text = str(item)
        formatted += f"- {text}\n"
    
    return formatted.strip()


def _build_explanation_prompt(question: str, schema_text: str, result_text: str) -> str:
    """
    Construct the complete user prompt following the mandatory format.
    This is the exact structure required to prevent hallucinations.
    """
    prompt = f"""USER QUESTION:
{question}

{schema_text}

VERIFIED QUERY RESULT (AUTHORITATIVE):
{result_text}

INSTRUCTIONS:
- Answer the user question using ONLY the verified query result
- Do NOT add or infer any information
- Do NOT compute rankings or values
- If multiple rows are present, explain their order clearly
- If the question asks for a specific rank (e.g., "second least"), select the correct row based ONLY on the provided order
- Keep the answer concise and factual
- Do NOT mention SQL, tables, databases, or any internal system details

Provide a natural language answer:"""
    
    return prompt


def _validate_explanation(explanation: str, execution_df: pd.DataFrame) -> None:
    """
    Validate that the explanation doesn't contain hallucinations.
    Raises ValueError if hallucinations are detected.
    """
    if execution_df.empty:
        # For empty results, just check that the explanation acknowledges this
        if "no" not in explanation.lower() or "found" not in explanation.lower():
            raise ValueError(
                "Hallucination detected: Empty result but explanation doesn't indicate no data found"
            )
        return
    
    # Extract all numbers from the explanation (with decimal support)
    explanation_numbers = set(re.findall(r'\d+\.?\d*', explanation))
    
    # Extract all numbers from the DataFrame
    df_numbers = set()
    for col in execution_df.columns:
        for value in execution_df[col]:
            # Convert to string and extract numbers
            value_str = str(value)
            numbers_in_value = re.findall(r'\d+\.?\d*', value_str)
            df_numbers.update(numbers_in_value)
    
    # Check if explanation contains numbers not in the DataFrame
    hallucinated_numbers = explanation_numbers - df_numbers
    
    # Filter out common numbers that might appear in natural language (like "second", "2")
    # but be strict about decimal numbers
    suspicious_numbers = {
        num for num in hallucinated_numbers 
        if '.' in num or (num not in ['1', '2', '3', '4', '5'])
    }
    
    if suspicious_numbers:
        raise ValueError(
            f"Hallucination detected: Explanation contains numbers not in the result: {suspicious_numbers}\n"
            f"Explanation: {explanation}\n"
            f"Valid numbers from DataFrame: {df_numbers}"
        )
    
    # Extract potential names/identifiers (words with capital letters, excluding common words)
    explanation_words = set(re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', explanation))
    
    # Get all string values from DataFrame (full values, not split)
    df_full_strings = set()
    df_individual_words = set()
    
    for col in execution_df.columns:
        for value in execution_df[col]:
            if isinstance(value, str):
                df_full_strings.add(value)
                # Also track individual words
                df_individual_words.update(value.split())
    
    # Common words and phrases that are okay to use in natural language
    # This includes temporal phrases, connectors, and descriptive language
    common_words = {
        'The', 'A', 'An', 'In', 'On', 'At', 'To', 'For', 'With', 'By', 'From',
        'Chennai', 'Campus', 'Student', 'Students', 'Data', 'Result', 'Results',
        'Total', 'Average', 'Count', 'Maximum', 'Minimum', 'Sum', 'CGPA',
        'Name', 'No', 'Found', 'Is', 'Are', 'Has', 'Have', 'Was', 'Were',
        'Based', 'Top', 'Of', 'And', 'Or', 'But', 'Not', 'All', 'Each',
        'Order', 'Lowest', 'Highest', 'First', 'Second', 'Third', 'Fourth', 'Fifth',
        # Temporal/date phrases (natural language)
        'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
        'September', 'October', 'November', 'December',
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday',
        'On January', 'On February', 'On March', 'On April', 'On May', 'On June',
        'On July', 'On August', 'On September', 'On October', 'On November', 'On December',
        'At', 'Between', 'During', 'Following', 'As Follows',
        # Descriptive phrases
        'Temperature', 'Temperatures', 'Earlwood', 'Hour', 'Hours'
    }
    
    # Check for names/identifiers not in the DataFrame
    hallucinated_names = set()
    
    for word in explanation_words:
        # Skip common words
        if word in common_words:
            continue
        
        # Skip if it's a month name or day name (natural language for dates)
        month_names = ['January', 'February', 'March', 'April', 'May', 'June', 
                       'July', 'August', 'September', 'October', 'November', 'December']
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        if any(month in word for month in month_names) or any(day in word for day in day_names):
            continue
        
        # Check if it's an exact match in full strings or individual words
        if word in df_full_strings or word in df_individual_words:
            continue
        
        # Check if it's a valid substring of any full string (for partial names)
        # e.g., "Hemanth Kumar" should match "Hemanth Kumar balineni"
        is_valid_substring = False
        for full_string in df_full_strings:
            if word in full_string:
                # Additional check: make sure it's a proper substring (word boundaries)
                if word == full_string or full_string.startswith(word + ' ') or ' ' + word in full_string:
                    is_valid_substring = True
                    break
        
        if is_valid_substring:
            continue
        
        # Only flag if we find proper nouns (multiple capital letters) that look like entity names
        # Skip short words and single capital letter words (likely natural language)
        if len(word) > 5 and sum(1 for c in word if c.isupper()) >= 2:
            # Additional filter: must not be a common phrase pattern
            # e.g., "On January" should not be flagged
            if not any(common in word for common in ['On ', 'At ', 'In ', 'The ', 'For ']):
                hallucinated_names.add(word)
    
    if hallucinated_names:
        raise ValueError(
            f"Hallucination detected: Explanation contains entity names not in the result: {hallucinated_names}\n"
            f"Explanation: {explanation}\n"
            f"Valid strings from DataFrame: {df_full_strings}"
        )


def generate_explanation(
    question: str,
    schema_context: list,
    execution_df: pd.DataFrame
) -> str:
    """
    Generate natural language explanation from verified query results.
    
    Args:
        question: Original user question
        schema_context: List of schema context items (for understanding only)
        execution_df: Verified execution result (authoritative ground truth)
    
    Returns:
        str: Natural language explanation
    
    Raises:
        ValueError: If hallucinations are detected in the explanation
    
    CRITICAL: This function MUST NOT:
    - Compute or derive any values
    - Add information not in execution_df
    - Contradict the execution result
    - Mention internal system details
    """
    
    # Load configuration and initialize client
    config = load_config()
    model = initialize_gemini_client(config)
    
    # Format inputs
    schema_text = _format_schema_context(schema_context)
    result_text = _format_dataframe_for_prompt(execution_df)
    
    # Build prompt
    prompt = _build_explanation_prompt(question, schema_text, result_text)
    
    # Call Gemini API
    try:
        response = model.generate_content(prompt)
        explanation = response.text.strip()
    except Exception as e:
        raise ValueError(f"Failed to generate explanation: {e}")
    
    # Validate for hallucinations
    try:
        _validate_explanation(explanation, execution_df)
    except ValueError as e:
        # If validation fails, return a safe fallback
        raise ValueError(f"Hallucination detected in LLM output. {e}")
    
    return explanation


def explain_results(result_df: pd.DataFrame) -> str:
    """
    Backward compatibility wrapper for the old interface.
    
    This function is deprecated. Use generate_explanation() instead.
    """
    # Provide minimal context for backward compatibility
    return generate_explanation(
        question="Explain these results",
        schema_context=[],
        execution_df=result_df
    )
