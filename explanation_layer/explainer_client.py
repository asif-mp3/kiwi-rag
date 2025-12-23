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
    if result_df.empty:
        return "No data is available for the requested criteria."
    
    # Build context for the LLM
    context = {
        "row_count": len(result_df),
        "columns": list(result_df.columns),
        "data_sample": result_df.head(10).to_dict('records')  # Show up to 10 rows
    }
    
    # Add query plan context if available
    if query_plan:
        context["query_type"] = query_plan.get("query_type")
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
    
    prompt += """
Instructions:
1. If this is an aggregation query (AVG, SUM, MIN, MAX, COUNT), state the result clearly using the correct aggregation type
2. For MIN/MAX queries, also mention which row(s) had that value
3. For multiple rows, provide a brief summary and list key data points
4. Be concise and direct - no unnecessary elaboration
5. Use the exact aggregation function name from the context (e.g., "minimum" for MIN, "maximum" for MAX, "average" for AVG)
6. Format numbers to 2 decimal places when appropriate

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
