def explain_results(result_df):
    """
    Generate a natural language explanation of query results.
    Handles both single and multiple row results appropriately.
    """
    if result_df.empty:
        return "No data is available for the requested criteria."
    
    # Special handling for aggregation_on_subset queries
    if hasattr(result_df, 'attrs') and 'aggregation_function' in result_df.attrs:
        aggregation_function = result_df.attrs['aggregation_function']
        aggregation_column = result_df.attrs['aggregation_column']
        subset_limit = result_df.attrs.get('subset_limit', len(result_df))
        
        # Calculate the aggregation
        if aggregation_column not in result_df.columns:
            return f"Error: Column '{aggregation_column}' not found in results"
        
        values = result_df[aggregation_column].dropna()
        
        if len(values) == 0:
            return f"No valid values found in '{aggregation_column}' column"
        
        if aggregation_function == "AVG":
            agg_result = values.mean()
            agg_name = "average"
        elif aggregation_function == "SUM":
            agg_result = values.sum()
            agg_name = "total"
        elif aggregation_function == "COUNT":
            agg_result = len(values)
            agg_name = "count"
        elif aggregation_function == "MAX":
            agg_result = values.max()
            agg_name = "maximum"
        elif aggregation_function == "MIN":
            agg_result = values.min()
            agg_name = "minimum"
        else:
            agg_result = values.mean()
            agg_name = "result"
        
        # Format the aggregation result
        if isinstance(agg_result, float):
            formatted_result = f"{agg_result:.2f}"
        else:
            formatted_result = str(agg_result)
        
        # Build the explanation with breakdown
        explanation = f"The {agg_name} is {formatted_result}\n\n"
        actual_count = len(result_df)
        explanation += f"data points ({actual_count} total):\n"
        
        # Show each item with relevant columns
        for i, (idx, row) in enumerate(result_df.iterrows()):
            item_parts = []
            
            # Try to find a name/identifier column
            name_columns = ['Lineitem name', 'Name', 'name', 'item', 'Item', 'product', 'Product']
            name_value = None
            for col in name_columns:
                if col in result_df.columns:
                    name_value = row[col]
                    break
            
            if name_value:
                item_parts.append(f"{name_value}")
            
            # Add the aggregation column value
            item_parts.append(f"{aggregation_column} = {row[aggregation_column]}")
            
            # Add date/time if available
            date_columns = ['Created at', 'Date', 'date', 'created_at', 'timestamp']
            for col in date_columns:
                if col in result_df.columns and col != aggregation_column:
                    item_parts.append(f"{col} = {row[col]}")
                    break
            
            explanation += f"  {i+1}. {', '.join(item_parts)}\n"
        
        return explanation.strip()
    
    # Special handling for aggregation results (single value with column name like 'result', 'avg_cgpa', etc.)
    if len(result_df) == 1 and len(result_df.columns) == 1:
        col_name = result_df.columns[0]
        value = result_df[col_name].iloc[0]
        
        # Format the value nicely
        if isinstance(value, float):
            formatted_value = f"{value:.2f}"
        else:
            formatted_value = str(value)
        
        # Create a user-friendly explanation based on column name
        if col_name == "result":
            return f"The result is {formatted_value}"
        elif "avg" in col_name.lower() or "average" in col_name.lower():
            return f"The average is {formatted_value}"
        elif "sum" in col_name.lower() or "total" in col_name.lower():
            return f"The total is {formatted_value}"
        elif "count" in col_name.lower():
            return f"The count is {formatted_value}"
        elif "max" in col_name.lower() or "maximum" in col_name.lower():
            return f"The maximum is {formatted_value}"
        elif "min" in col_name.lower() or "minimum" in col_name.lower():
            return f"The minimum is {formatted_value}"
        else:
            return f"{col_name} = {formatted_value}"
    
    # For single row results with multiple columns
    if len(result_df) == 1:
        parts = []
        for col in result_df.columns:
            value = result_df[col].iloc[0]
            parts.append(f"{col} = {value}")
        return ", ".join(parts)
    
    # For multiple row results
    row_count = len(result_df)
    
    # If it's a simple single-column result, list all values
    if len(result_df.columns) == 1:
        col_name = result_df.columns[0]
        values = result_df[col_name].tolist()
        items_list = "\n".join([f"  {i+1}. {val}" for i, val in enumerate(values)])
        return f"Found {row_count} results for {col_name}:\n{items_list}"
    
    # For multi-column results, show count and first few rows
    if row_count <= 5:
        # Show all rows if 5 or fewer
        explanation = f"Found {row_count} results:\n"
        for i, (idx, row) in enumerate(result_df.iterrows()):
            parts = [f"{col} = {row[col]}" for col in result_df.columns]
            explanation += f"  {i+1}. {', '.join(parts)}\n"
        return explanation.strip()
    else:
        # Show first 5 and indicate there are more
        explanation = f"Found {row_count} results (showing first 5):\n"
        for i, (idx, row) in enumerate(result_df.head(5).iterrows()):
            parts = [f"{col} = {row[col]}" for col in result_df.columns]
            explanation += f"  {i+1}. {', '.join(parts)}\n"
        explanation += f"  ... and {row_count - 5} more"
        return explanation.strip()

