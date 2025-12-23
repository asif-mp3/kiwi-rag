from analytics_engine.metric_registry import MetricRegistry


def quote_identifier(name: str) -> str:
    """Quote SQL identifiers that contain spaces or special characters"""
    if ' ' in name or any(char in name for char in ['-', '.', '(', ')']):
        return f'"{name}"'
    return name


def compile_sql(plan: dict) -> str:
    """
    Converts a validated query plan into SQL.
    Deterministic. Template-based. Safe.
    """
    
    query_type = plan.get("query_type", "metric")
    
    if query_type == "lookup":
        return _compile_lookup(plan)
    elif query_type == "filter":
        return _compile_filter(plan)
    elif query_type == "metric":
        return _compile_metric(plan)
    elif query_type == "extrema_lookup":
        return _compile_extrema_lookup(plan)
    elif query_type == "rank":
        return _compile_rank(plan)
    elif query_type == "list":
        return _compile_list(plan)
    elif query_type == "aggregation_on_subset":
        return _compile_aggregation_on_subset(plan)
    else:
        raise ValueError(f"Unknown query type: {query_type}")


def _compile_lookup(plan):
    """Compile row lookup query"""
    table = quote_identifier(plan["table"])
    columns = plan["select_columns"]
    
    # Quote column names if needed
    quoted_columns = ", ".join([quote_identifier(col) for col in columns])
    
    where = _build_where_clause(plan["filters"])
    limit = plan.get("limit", 1)
    
    return f"SELECT {quoted_columns} FROM {table} {where} LIMIT {limit}".strip()


def _compile_filter(plan):
    """Compile filter query"""
    table = quote_identifier(plan["table"])
    columns = plan.get("select_columns", ["*"])
    
    # Quote column names if needed
    if columns != ["*"]:
        columns = ", ".join([quote_identifier(col) for col in columns])
    else:
        columns = "*"
    
    where = _build_where_clause(plan["filters"])
    limit = plan.get("limit", 100)
    
    return f"SELECT {columns} FROM {table} {where} LIMIT {limit}".strip()



def _build_where_clause(filters):
    """
    Build WHERE clause from filters.
    Handles both numeric and text values with proper escaping.
    """
    if not filters:
        return ""
    
    conditions = []
    for f in filters:
        column = quote_identifier(f["column"])
        operator = f["operator"]
        value = f["value"]
        
        # Handle different value types
        if isinstance(value, str):
            if operator == "LIKE":
                # Case-insensitive LIKE - cast to VARCHAR for timestamp columns
                safe_value = value.replace("'", "''")
                # Cast to VARCHAR to handle TIMESTAMP_NS columns
                conditions.append(f"LOWER(CAST({column} AS VARCHAR)) LIKE LOWER('{safe_value}')")
            else:
                # Use actual operator (=, >=, <=, !=, etc.) for string comparisons
                # Cast column to VARCHAR to handle TIMESTAMP_NS columns
                safe_value = value.replace("'", "''")
                conditions.append(f"CAST({column} AS VARCHAR) {operator} '{safe_value}'")
        else:
            # Numeric value
            conditions.append(f"{column} {operator} {value}")
    
    return "WHERE " + " AND ".join(conditions)


def _compile_metric(plan):
    """Compile metric-based aggregation query"""
    registry = MetricRegistry()

    metric_sql = []
    base_table = None

    for metric in plan.get("metrics", []):
        metric_def = registry.get_metric(metric)
        metric_sql.append(f"{metric_def['sql']} AS {metric}")

        if base_table is None:
            base_table = metric_def["base_table"]

    select_clause = ", ".join(metric_sql)

    group_by_clause = ""
    if plan.get("group_by"):
        group_by_clause = " GROUP BY " + ", ".join(plan["group_by"])
        select_clause += ", " + ", ".join(plan["group_by"])

    where_clause = _build_where_clause(plan.get("filters", []))

    sql = f"""
        SELECT {select_clause}
        FROM {base_table}
        {where_clause}
        {group_by_clause}
    """

    return sql.strip()


def _compile_extrema_lookup(plan):
    """Compile extrema lookup query (min/max with ordering)"""
    table = quote_identifier(plan["table"])
    columns = ", ".join([quote_identifier(col) for col in plan["select_columns"]])
    order_by = plan.get("order_by", [])
    limit = plan.get("limit", 1)
    
    # Build WHERE clause if filters exist
    where_clause = _build_where_clause(plan.get("filters", []))
    
    order_clause = ""
    if order_by:
        order_parts = [f"{quote_identifier(col)} {direction}" for col, direction in order_by]
        order_clause = "ORDER BY " + ", ".join(order_parts)
    
    return f"SELECT {columns} FROM {table} {where_clause} {order_clause} LIMIT {limit}".strip()


def _compile_rank(plan):
    """Compile rank query (ordered list of all results)"""
    table = quote_identifier(plan["table"])
    
    # Quote column names if needed
    select_columns = plan.get("select_columns", ["*"])
    if select_columns != ["*"]:
        columns = ", ".join([quote_identifier(col) for col in select_columns])
    else:
        columns = "*"
    
    order_by = plan.get("order_by", [])
    limit = plan.get("limit", 100)
    
    # Build WHERE clause if filters exist
    where_clause = _build_where_clause(plan.get("filters", []))
    
    order_clause = ""
    if order_by:
        order_parts = [f"{quote_identifier(col)} {direction}" for col, direction in order_by]
        order_clause = "ORDER BY " + ", ".join(order_parts)
    
    return f"SELECT {columns} FROM {table} {where_clause} {order_clause} LIMIT {limit}".strip()


def _compile_list(plan):
    """Compile list/show all query"""
    table = quote_identifier(plan["table"])
    
    # Quote column names if needed
    select_columns = plan.get("select_columns", ["*"])
    if select_columns != ["*"]:
        columns = ", ".join([quote_identifier(col) for col in select_columns])
    else:
        columns = "*"
    
    limit = plan.get("limit", 100)
    
    return f"SELECT {columns} FROM {table} LIMIT {limit}".strip()


def _compile_aggregation_on_subset(plan):
    """Compile aggregation on subset query (e.g., AVG of first 5 items)"""
    table = quote_identifier(plan["table"])
    aggregation_function = plan["aggregation_function"]
    aggregation_column = quote_identifier(plan["aggregation_column"])
    
    # Build the subquery to get the subset
    subset_filters = plan.get("subset_filters", [])
    subset_order_by = plan.get("subset_order_by", [])
    subset_limit = plan.get("subset_limit")
    
    # Handle None or missing subset_limit
    if subset_limit is None:
        subset_limit = 100  # Default to 100 if not specified
    
    # Get all columns we want to show in the breakdown
    # Include aggregation column, order columns, and common identifier columns
    subquery_columns = [aggregation_column]
    
    # Add ordering columns
    for col, _ in subset_order_by:
        quoted_col = quote_identifier(col)
        if quoted_col not in subquery_columns:
            subquery_columns.append(quoted_col)
    
    # Try to add common identifier columns (like name, id, etc.)
    # We'll select * from the subset to get all columns for the breakdown
    
    # Build WHERE clause for subset
    where_clause = _build_where_clause(subset_filters)
    
    # Build ORDER BY clause for subset
    order_clause = ""
    if subset_order_by:
        order_parts = [f"{quote_identifier(col)} {direction}" for col, direction in subset_order_by]
        order_clause = "ORDER BY " + ", ".join(order_parts)
    
    # Build LIMIT clause
    limit_clause = ""
    if subset_limit is not None:
        limit_clause = f"LIMIT {subset_limit}"
    
    # Build SQL with subquery that calculates the aggregation in the database
    # The outer query calculates the aggregation on the subset
    # The inner query (subquery) gets the subset of rows
    sql = f"""
SELECT 
    {aggregation_function}({aggregation_column}) as result,
    COUNT(*) as row_count,
    MIN({aggregation_column}) as min_value,
    MAX({aggregation_column}) as max_value
FROM (
    SELECT *
    FROM {table}
    {where_clause}
    {order_clause}
    {limit_clause}
) subset
    """
    
    return sql.strip()
