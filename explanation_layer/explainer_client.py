import os
import json
import yaml
import google.generativeai as genai
from pathlib import Path
from explanation_layer.explanation_prompt import EXPLANATION_SYSTEM_PROMPT
from dotenv import load_dotenv

# Load environment variables
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
    }
    
    model = genai.GenerativeModel(
        model_name=model_name,
        generation_config=generation_config,
        system_instruction=EXPLANATION_SYSTEM_PROMPT
    )
    
    return model


def explain_results(result_df, query_plan=None, original_question=None):
    """
    Generate a natural language explanation of query results using LLM.
    
    Args:
        result_df: DataFrame containing query results
        query_plan: Optional dict containing the query plan (for context)
        original_question: Optional string containing the user's original question
    
    Returns:    
        str: Natural language explanation of the results
    """
    # Detect the language of the original question first
    question_language = "English"  # Default
    if original_question:
        try:
            from langdetect import detect
            import re
            # Check for Tamil characters
            tamil_pattern = re.compile(r'[\u0B80-\u0BFF]')
            if tamil_pattern.search(original_question):
                question_language = "Tamil"
            else:
                detected = detect(original_question)
                if detected == 'ta':
                    question_language = "Tamil"
        except:
            pass
    
    if result_df.empty:
        # Provide helpful message in the detected language
        if question_language == "Tamil":
            return "கோரப்பட்ட தகவல் கிடைக்கவில்லை. பெயர் சரியாக உள்ளதா என்று சரிபார்க்கவும்."
        else:
            return "No data found for the requested criteria. Please check if the name or value is spelled correctly."
    
    # Build context for the LLM
    context = {
        "row_count": len(result_df),
        "columns": list(result_df.columns),
        "data_sample": result_df.head(10).to_dict('records')  # Show up to 10 rows
    }
    
    # Add query plan context if available
    if query_plan:
        context["query_type"] = query_plan.get("query_type")
        context["table"] = query_plan.get("table")  # Add table/sheet name
        context["aggregation_function"] = query_plan.get("aggregation_function")
        context["aggregation_column"] = query_plan.get("aggregation_column")
        context["filters"] = query_plan.get("filters", [])
        context["subset_filters"] = query_plan.get("subset_filters", [])
    
    # Add aggregation metadata if present (from executor)
    if hasattr(result_df, 'attrs'):
        if 'aggregation_function' in result_df.attrs:
            context["aggregation_function"] = result_df.attrs['aggregation_function']
        if 'aggregation_column' in result_df.attrs:
            context["aggregation_column"] = result_df.attrs['aggregation_column']
    
    # Build the prompt for the LLM
    prompt = f"""Given the following query results, generate a concise, natural language explanation.

Context:
{json.dumps(context, indent=2, default=str)}
"""
    
    if original_question:
        prompt += f"\nOriginal Question: {original_question}\n"
        prompt += f"Question Language: {question_language}\n"
    
    prompt += f"""
Instructions:
1. **IMPORTANT: Respond in {question_language} language**
2. **For Tamil responses:**
   - **ALWAYS write numbers in Tamil words, NEVER use digits**
     - Examples: 15000 → "பதினைந்தாயிரம்", 24.2 → "இருபத்தி நான்கு புள்ளி இரண்டு"
   - **MANDATORY**: After the Tamil response, add the separator `|||ENGLISH_TRANSLATION|||`
   - Follow with the **English translation** of the response.
   - **In the English translation, MUST use numeric digits** (e.g., "24.2", "15,000").
3. If this is an aggregation query (AVG, SUM, MIN, MAX, COUNT), state the result clearly using the correct aggregation type
4. For MIN/MAX queries, also mention which row(s) had that value
5. For multiple rows, provide a brief summary and list key data points
6. Be concise and direct - no unnecessary elaboration
7. Use the exact aggregation function name from the context (e.g., "minimum" for MIN, "average" for AVG)
8. For English responses (primary language queries): Format numbers with commas (e.g., "15,000")
9. **CRITICAL: For Tamil, convert ALL numbers (dates, times, values) to Tamil words**
10. **Output Format for Tamil Queries:**
    [Tamil Response with Words for Numbers]
    |||ENGLISH_TRANSLATION|||
    [English Translation with Digits for Numbers]

Generate the explanation:"""
    
    try:
        # Initialize LLM
        config = load_config()
        model = initialize_gemini_client(config)
        
        # Generate explanation
        response = model.generate_content(prompt)
        explanation = response.text.strip()
        
        return explanation
        
    except Exception as e:
        # Fallback to simple explanation if LLM fails
        print(f"Warning: LLM explanation failed ({e}), using fallback")
        return _fallback_explanation(result_df, context)


def _fallback_explanation(result_df, context):
    """
    Simple fallback explanation when LLM is unavailable.
    This is much simpler and just presents the data.
    """
    if result_df.empty:
        return "No data is available for the requested criteria."
    
    # Check for aggregation
    agg_func = context.get("aggregation_function")
    agg_col = context.get("aggregation_column")
    
    if agg_func and agg_col and agg_col in result_df.columns:
        values = result_df[agg_col].dropna()
        
        if len(values) == 0:
            return f"No valid values found in '{agg_col}' column"
        
        # Calculate based on aggregation function
        if agg_func == "AVG":
            result = values.mean()
            func_name = "average"
        elif agg_func == "SUM":
            result = values.sum()
            func_name = "total"
        elif agg_func == "COUNT":
            result = len(values)
            func_name = "count"
        elif agg_func == "MAX":
            result = values.max()
            func_name = "maximum"
        elif agg_func == "MIN":
            result = values.min()
            func_name = "minimum"
        else:
            result = values.mean()
            func_name = "result"
        
        # Format result
        if isinstance(result, float):
            formatted_result = f"{result:.2f}"
        else:
            formatted_result = str(result)
        
        explanation = f"The {func_name} is {formatted_result}"
        
        # Add breakdown for MIN/MAX
        if agg_func in ["MIN", "MAX"]:
            matching_rows = result_df[result_df[agg_col] == result]
            if len(matching_rows) > 0:
                explanation += f"\n\nThis value appears in {len(matching_rows)} row(s)"
        
        return explanation
    
    # For non-aggregation queries
    row_count = len(result_df)
    
    if row_count == 1:
        # Single row result
        parts = []
        for col in result_df.columns:
            value = result_df[col].iloc[0]
            parts.append(f"{col} = {value}")
        return ", ".join(parts)
    else:
        # Multiple rows
        return f"Found {row_count} results with columns: {', '.join(result_df.columns)}"
