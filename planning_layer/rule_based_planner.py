import re
from schema_intelligence.schema_extractor import extract_schema


def classify_intent(question: str) -> str:
    """
    Classify query intent deterministically using pattern matching.
    No LLM involved.
    """
    q_lower = question.lower()
    
    # Lookup patterns: "What is X's Y?" or "Show Y for X"
    if re.search(r"what is .+'s", q_lower) or re.search(r"show .+ for", q_lower):
        return "lookup"
    
    # Filter patterns: numeric comparisons OR text filters
    if any(op in q_lower for op in ['>', '<', 'greater', 'less', 'above', 'below', '>=', '<=', 'equal to', 'equals']):
        return "filter"
    
    # Text filter patterns: "show X status", "show fulfilled", etc.
    if re.search(r'show \w+ (orders|items|students|records)', q_lower):
        return "filter"
    
    # Metric patterns: aggregations
    if any(word in q_lower for word in ['how many', 'count', 'average', 'avg', 'total', 'sum', 'max', 'min']):
        return "metric"
    
    # Rank patterns: "rank", "sort", "order" - returns ALL results ordered
    if any(word in q_lower for word in ['rank', 'sort', 'order by']):
        return "rank"
    
    # Min/Max lookup patterns: "Who has the least/most/highest/lowest X?" - returns TOP 1
    if any(word in q_lower for word in ['least', 'most', 'highest', 'lowest', 'minimum', 'maximum']):
        return "extrema_lookup"
    
    # List/Show all patterns: "Show all", "List", "Who has"
    if any(word in q_lower for word in ['show all', 'list all', 'who has', 'which', 'what are']):
        return "list"
    
    return "unsupported"


def extract_entity_name(question: str) -> str:
    """Extract entity name from lookup question"""
    # Pattern: "What is X's Y?"
    match = re.search(r"what is (.+?)'s", question, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Pattern: "Show Y for X"
    match = re.search(r"show .+ for (.+)", question, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    return None


def extract_filter_condition(question: str, schema: dict, table: str):
    """
    Extract filter conditions deterministically.
    Supports numeric, text, and equality filters.
    """
    q_lower = question.lower()
    
    # Find operator
    operator = None
    if '>=' in question:
        operator = '>='
    elif '<=' in question:
        operator = '<='
    elif '>' in question or 'above' in q_lower or 'greater' in q_lower:
        operator = '>'
    elif '<' in question or 'below' in q_lower or 'less' in q_lower:
        operator = '<'
    elif any(word in q_lower for word in ['equal to', 'equals', '=', ' is ']):
        operator = '='
    
    # Extract value (numeric or text)
    # Try numeric first
    match = re.search(r'(\d+\.?\d*)', question)
    if match and operator:
        value = float(match.group(1))
        # Find the column being filtered
        column = find_column_by_keyword(question, schema, table, "numeric_measure")
        if column:
            return {"column": column, "operator": operator, "value": value}
    
    # Try text value (e.g., "status = fulfilled", "month = November")
    # Pattern: "column = value" or "column is value"
    if operator == '=':
        text_pattern = r'(\w+)\s+(?:=|equals?|is)\s+(\w+)'
        match = re.search(text_pattern, q_lower)
        if match:
            col_keyword = match.group(1)
            value = match.group(2)
            # Find matching column
            column = find_column_by_keyword(question, schema, table, None)
            if column:
                return {"column": column, "operator": operator, "value": value}
    
    # Pattern: "show X orders/items/records" where X is the status/category
    show_pattern = r'show (\w+) (orders|items|students|records)'
    match = re.search(show_pattern, q_lower)
    if match:
        value = match.group(1)  # e.g., "fulfilled"
        entity_type = match.group(2)  # e.g., "orders"
        
        # Find a categorical column that might contain this value
        # Look for "status", "fulfillment", "financial", etc.
        table_meta = schema["tables"][table]
        for col_name, col_info in table_meta["columns"].items():
            col_lower = col_name.lower()
            if col_info.get("semantic_type") == "categorical_attribute":
                # Match if column name suggests it contains status/category info
                if any(word in col_lower for word in ['status', 'fulfillment', 'financial', 'category', 'type']):
                    return {"column": col_name, "operator": "=", "value": value}
        
        # Fallback: use first categorical column
        for col_name, col_info in table_meta["columns"].items():
            if col_info.get("semantic_type") == "categorical_attribute":
                return {"column": col_name, "operator": "=", "value": value}
    
    return None


def find_column_by_keyword(question: str, schema: dict, table: str, semantic_type: str = None):
    """
    Find column by keyword matching from question.
    Supports partial matching (e.g., "quantity" matches "Lineitem quantity").
    """
    q_lower = question.lower()
    table_meta = schema["tables"][table]
    
    # Extract potential column keywords from question
    keywords = q_lower.split()
    
    best_match = None
    best_score = 0
    
    for col_name, col_info in table_meta["columns"].items():
        col_lower = col_name.lower()
        
        # Skip if semantic type doesn't match (when specified)
        if semantic_type and col_info.get("semantic_type") != semantic_type:
            continue
        
        score = 0
        
        # Exact match
        if col_lower in q_lower:
            score += 10
        
        # Partial word matches
        for keyword in keywords:
            if keyword in col_lower or col_lower in keyword:
                score += 1
        
        if score > best_score:
            best_score = score
            best_match = col_name
    
    return best_match if best_score > 0 else None


def find_column_by_semantic(schema, semantic_type: str, table_name: str = None, prefer_name=False):
    """
    Find first column matching semantic type.
    If table_name is provided, search only within that table.
    Otherwise search all tables.
    """
    tables_to_search = {table_name: schema["tables"][table_name]} if table_name else schema["tables"]
    
    # If looking for entity_identifier and prefer_name is True, prioritize 'name' column
    if semantic_type == "entity_identifier" and prefer_name:
        for table, meta in tables_to_search.items():
            for col, info in meta["columns"].items():
                if col.lower() == "name" and info.get("semantic_type") == semantic_type:
                    return col
    
    # Otherwise return first match
    for table, meta in tables_to_search.items():
        for col, info in meta["columns"].items():
            if info.get("semantic_type") == semantic_type:
                return col
    return None


def detect_table(question: str, schema: dict) -> str:
    """
    Intelligently detect which table to query based on semantic relevance.
    
    Strategy:
    1. Extract keywords from question
    2. Score each table based on column name matches
    3. Return table with highest relevance score
    
    This allows queries like "which product sold most" to automatically
    select a grocery/products table instead of a students table.
    """
    q_lower = question.lower()
    available_tables = list(schema["tables"].keys())
    
    if not available_tables:
        return "sales"  # Fallback
    
    # First, check for explicit table name mentions (sorted by length)
    sorted_tables = sorted(available_tables, key=len, reverse=True)
    for table_name in sorted_tables:
        if table_name.lower() in q_lower:
            return table_name
    
    # If no explicit mention, use semantic scoring
    table_scores = {}
    
    for table_name in available_tables:
        score = 0
        table_meta = schema["tables"][table_name]
        
        # Get all column names for this table
        column_names = [col.lower() for col in table_meta["columns"].keys()]
        
        # Score based on keyword matches in column names
        # Common keywords mapped to domains
        keywords = {
            # Student-related
            'student': ['name', 'cgpa', 'gpa', 'grade', 'major', 'degree', 'campus', 'register'],
            'cgpa': ['cgpa', 'gpa', 'grade'],
            'grade': ['cgpa', 'gpa', 'grade'],
            'campus': ['campus', 'location'],
            'major': ['major', 'branch', 'degree'],
            
            # Product/Sales-related
            'product': ['product', 'item', 'name', 'price', 'quantity'],
            'sold': ['quantity', 'sales', 'sold', 'amount'],
            'price': ['price', 'cost', 'amount'],
            'grocery': ['product', 'item', 'category'],
            'sales': ['sales', 'revenue', 'amount', 'quantity'],
            
            # General
            'name': ['name'],
            'email': ['email', 'gmail'],
        }
        
        # Check which keywords from question match this table's columns
        for keyword, related_columns in keywords.items():
            if keyword in q_lower:
                for col in column_names:
                    if any(related in col for related in related_columns):
                        score += 1
        
        # Bonus: if table name semantically matches question context
        table_lower = table_name.lower()
        if any(word in table_lower for word in ['student', 'cgpa', 'grade']) and \
           any(word in q_lower for word in ['student', 'cgpa', 'grade', 'campus', 'major']):
            score += 3
        
        if any(word in table_lower for word in ['product', 'grocery', 'sales', 'item']) and \
           any(word in q_lower for word in ['product', 'sold', 'price', 'grocery', 'item']):
            score += 3
        
        table_scores[table_name] = score
    
    # Return table with highest score
    if table_scores:
        best_table = max(table_scores, key=table_scores.get)
        # Only use semantic match if score > 0, otherwise use default
        if table_scores[best_table] > 0:
            return best_table
    
    # Fallback: prioritize 'sales' if it exists, otherwise first table
    if "sales" in available_tables:
        return "sales"
    
    return available_tables[0]


def generate_plan(question: str, schema_context: list):
    """
    Generate query plan using ONLY rule-based logic.
    NO LLM INVOLVEMENT.
    """
    
    intent = classify_intent(question)
    
    if intent == "unsupported":
        raise ValueError("Query type not supported by rule-based planner")
    
    # Extract full schema for semantic lookup
    schema = extract_schema()
    
    # Detect table from question or use first available table
    table = detect_table(question, schema)
    
    if intent == "lookup":
        entity_name = extract_entity_name(question)
        if not entity_name:
            raise ValueError("Could not extract entity name from question")
        
        # Find entity identifier column (prefer 'name' column) within the selected table
        entity_col = find_column_by_semantic(schema, "entity_identifier", table_name=table, prefer_name=True)
        if not entity_col:
            raise ValueError(f"No entity_identifier column found in table '{table}'")
        
        # Find numeric measure column (e.g., cgpa) within the selected table
        measure_col = find_column_by_semantic(schema, "numeric_measure", table_name=table)
        if not measure_col:
            raise ValueError(f"No numeric_measure column found in table '{table}'")
        
        return {
            "query_type": "lookup",
            "table": table,
            "select_columns": [entity_col, measure_col],
            "filters": [
                {"column": entity_col, "operator": "LIKE", "value": f"%{entity_name}%"}
            ],
            "limit": 1
        }
    
    elif intent == "filter":
        condition = extract_filter_condition(question, schema, table)
        if not condition:
            raise ValueError("Could not extract filter condition from question")
        
        # Column is already determined by extract_filter_condition
        return {
            "query_type": "filter",
            "table": table,
            "select_columns": ["*"],
            "filters": [
                {"column": condition["column"], "operator": condition["operator"], "value": condition["value"]}
            ],
            "limit": 100
        }
    
    elif intent == "metric":
        # Use existing metric-based logic
        plan = {
            "query_type": "metric",
            "table": table,
            "metrics": [],
            "filters": [],
            "group_by": []
        }
        
        # Extract metrics from schema context
        for item in schema_context:
            meta = item.get("metadata", {})
            if meta.get("type") == "metric":
                plan["metrics"].append(meta["metric"])
        
        # Detect grouping dimensions
        q_lower = question.lower()
        if "campus" in q_lower:
            plan["group_by"].append("campus")
        if "major" in q_lower:
            plan["group_by"].append("major")
        if "degree" in q_lower:
            plan["group_by"].append("degree")
        
        return plan
    
    elif intent == "rank":
        # "Rank students by X", "Sort by Y" - Return ALL results with ordering
        q_lower = question.lower()
        
        # Determine order direction - prioritize explicit direction words
        if 'highest to lowest' in q_lower or 'descending' in q_lower:
            order = 'DESC'
        elif 'lowest to highest' in q_lower or 'ascending' in q_lower:
            order = 'ASC'
        elif any(word in q_lower for word in ['highest', 'most']):
            order = 'DESC'
        elif any(word in q_lower for word in ['lowest', 'least']):
            order = 'ASC'
        else:
            order = 'DESC'  # Default to descending for rankings
        
        # Find the measure column to rank by within the selected table
        measure_col = find_column_by_semantic(schema, "numeric_measure", table_name=table)
        entity_col = find_column_by_semantic(schema, "entity_identifier", table_name=table, prefer_name=True)
        
        if not measure_col:
            raise ValueError(f"No numeric_measure column found in table '{table}' for ranking")
        
        # Return all results, ordered
        return {
            "query_type": "rank",
            "table": table,
            "select_columns": [entity_col, measure_col] if entity_col else ["*"],
            "order_by": [[measure_col, order]],
            "limit": 100  # Return all (up to 100)
        }
    
    elif intent == "extrema_lookup":
        # "Who has the least/most X?" - Find row with min/max value
        q_lower = question.lower()
        
        # Determine if MIN or MAX
        if any(word in q_lower for word in ['least', 'lowest', 'minimum']):
            order = 'ASC'
        else:  # most, highest, maximum
            order = 'DESC'
        
        # Find the measure column (e.g., cgpa, quantity) within the selected table
        measure_col = find_column_by_semantic(schema, "numeric_measure", table_name=table)
        
        if not measure_col:
            raise ValueError(f"No numeric_measure column found in table '{table}' for extrema query")
        
        # Intelligently find the entity column based on what user is asking about
        entity_col = None
        table_meta = schema["tables"][table]
        
        # Check if user is asking about a specific column (e.g., "lineitem name", "product name")
        for col_name in table_meta["columns"].keys():
            col_lower = col_name.lower()
            # Match if column name appears in question
            if col_lower in q_lower or any(word in col_lower for word in q_lower.split()):
                if table_meta["columns"][col_name].get("semantic_type") == "entity_identifier":
                    entity_col = col_name
                    break
        
        # Fallback: use prefer_name logic
        if not entity_col:
            entity_col = find_column_by_semantic(schema, "entity_identifier", table_name=table, prefer_name=True)
        
        if not entity_col:
            raise ValueError(f"No entity_identifier column found in table '{table}' for extrema query")
        
        return {
            "query_type": "extrema_lookup",
            "table": table,
            "select_columns": [entity_col, measure_col],
            "order_by": [[measure_col, order]],  # List of lists, not tuple
            "limit": 1
        }
    
    elif intent == "list":
        # "Show all X", "List Y", "Who has Z" - General listing
        return {
            "query_type": "list",
            "table": table,
            "select_columns": ["*"],
            "limit": 100
        }
    
    raise ValueError(f"Query type '{intent}' not yet implemented")
