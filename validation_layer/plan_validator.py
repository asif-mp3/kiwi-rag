import json
from jsonschema import validate, ValidationError
from analytics_engine.metric_registry import MetricRegistry
from analytics_engine.duckdb_manager import DuckDBManager


def quote_identifier(name: str) -> str:
    """Quote SQL identifiers that contain spaces or special characters"""
    if ' ' in name or any(char in name for char in ['-', '.', '(', ')']):
        return f'"{name}"'
    return name


def get_table_schema(table_name: str) -> dict:
    """
    Get schema information for a table from DuckDB.
    Returns dict with column names and their types.
    """
    db = DuckDBManager()
    try:
        # Get column information
        quoted_table = quote_identifier(table_name)
        result = db.conn.execute(f"DESCRIBE {quoted_table}").fetchdf()
        schema = {}
        for _, row in result.iterrows():
            schema[row['column_name']] = row['column_type']
        return schema
    except Exception as e:
        raise ValueError(f"Table '{table_name}' does not exist in database: {e}")


def validate_table_exists(table_name: str):
    """Validate that table exists in DuckDB"""
    db = DuckDBManager()
    tables = db.list_tables()
    if table_name not in tables:
        raise ValueError(f"Table '{table_name}' does not exist. Available tables: {tables}")


def normalize_column_names(plan: dict, table_name: str) -> dict:
    """
    Normalize column names in plan to match actual database column names (case-insensitive).
    Returns updated plan with corrected column names.
    """
    table_schema = get_table_schema(table_name)
    column_map = {col.lower(): col for col in table_schema.keys()}
    
    # Normalize select_columns
    if "select_columns" in plan and plan["select_columns"] is not None and plan["select_columns"] != ["*"]:
        plan["select_columns"] = [
            column_map.get(col.lower(), col) for col in plan["select_columns"]
        ]
    
    # Normalize filters
    if "filters" in plan:
        for f in plan["filters"]:
            if "column" in f:
                f["column"] = column_map.get(f["column"].lower(), f["column"])
    
    # Normalize group_by
    if "group_by" in plan:
        plan["group_by"] = [
            column_map.get(col.lower(), col) for col in plan["group_by"]
        ]
    
    # Normalize order_by
    if "order_by" in plan:
        plan["order_by"] = [
            [column_map.get(col[0].lower(), col[0]), col[1]] for col in plan["order_by"]
        ]
    
    return plan


def validate_columns_exist(columns: list, table_name: str):
    """
    Validate that all columns exist in the specified table.
    Performs case-insensitive matching.
    """
    if not columns:
        return
    
    # Allow wildcard
    if columns == ["*"]:
        return
    
    table_schema = get_table_schema(table_name)
    available_columns = list(table_schema.keys())
    
    # Create case-insensitive lookup
    column_map = {col.lower(): col for col in available_columns}
    
    for column in columns:
        column_lower = column.lower()
        if column_lower not in column_map:
            raise ValueError(
                f"Column '{column}' does not exist in table '{table_name}'. "
                f"Available columns: {available_columns}"
            )



def validate_metric_table_mapping(metrics: list, table_name: str):
    """Validate that metrics are used with their correct base table (only if metrics are defined)"""
    if not metrics:
        return
    
    registry = MetricRegistry()
    
    # If no metrics are defined in the registry, skip validation
    if not registry.metrics:
        return
    
    for metric in metrics:
        if not registry.is_valid_metric(metric):
            raise ValueError(f"Invalid metric requested: {metric}")
        
        metric_def = registry.get_metric(metric)
        expected_table = metric_def.get("base_table")
        
        if expected_table and expected_table != table_name:
            raise ValueError(
                f"Metric '{metric}' can only be used with table '{expected_table}', "
                f"not '{table_name}'"
            )


def validate_filter_values(filters: list, table_name: str):
    """Validate that filter values match column types"""
    if not filters:
        return
    
    table_schema = get_table_schema(table_name)
    
    for f in filters:
        column = f.get("column")
        value = f.get("value")
        operator = f.get("operator")
        
        # Validate column exists
        if column not in table_schema:
            raise ValueError(
                f"Filter column '{column}' does not exist in table '{table_name}'"
            )
        
        # Validate operator
        allowed_ops = ["=", ">", "<", ">=", "<=", "LIKE"]
        if operator not in allowed_ops:
            raise ValueError(f"Unsafe operator: {operator}. Allowed: {allowed_ops}")
        
        # Basic type validation
        col_type = table_schema[column].upper()
        
        # Numeric columns should have numeric values (unless using LIKE)
        if any(t in col_type for t in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"]):
            if operator != "LIKE" and not isinstance(value, (int, float)):
                raise ValueError(
                    f"Column '{column}' is numeric ({col_type}) but filter value is {type(value).__name__}"
                )
        
        # LIKE operator should only be used with string values
        if operator == "LIKE" and not isinstance(value, str):
            raise ValueError(
                f"LIKE operator requires string value, got {type(value).__name__}"
            )


def validate_no_unknown_keys(plan: dict):
    """Reject plans with unexpected keys"""
    allowed_keys = {
        "query_type", "table", "metrics", "select_columns", 
        "filters", "group_by", "order_by", "limit",
        "aggregation_function", "aggregation_column", 
        "subset_filters", "subset_order_by", "subset_limit"
    }
    
    unknown_keys = set(plan.keys()) - allowed_keys
    if unknown_keys:
        raise ValueError(f"Plan contains unknown keys: {unknown_keys}")


def validate_plan(plan: dict, schema_path="planning_layer/plan_schema.json"):
    """
    Validates planner output against schema and registry.
    Enforces query type-specific rules.
    
    CRITICAL: This is the authoritative validation layer.
    All LLM output MUST pass through this validator.
    """
    
    # Normalize plan before validation (handle None values)
    if plan.get("limit") is None:
        # Set default limit based on query type
        query_type = plan.get("query_type", "metric")
        if query_type in ["lookup", "extrema_lookup"]:
            plan["limit"] = 1
        else:
            plan["limit"] = 100
    
    if plan.get("filters") is None:
        plan["filters"] = []
    
    if plan.get("group_by") is None:
        plan["group_by"] = []
    
    if plan.get("order_by") is None:
        plan["order_by"] = []

    # Load JSON schema
    with open(schema_path) as f:
        schema = json.load(f)

    # 1. Validate JSON structure
    try:
        validate(instance=plan, schema=schema)
    except ValidationError as e:
        raise ValueError(f"Plan schema violation: {e.message}")
    
    # 2. Reject unknown keys
    validate_no_unknown_keys(plan)
    
    # 3. Validate table exists
    table = plan.get("table")
    validate_table_exists(table)
    
    # 4. Normalize column names (case-insensitive matching)
    plan = normalize_column_names(plan, table)
    
    # 5. Validate columns exist
    select_columns = plan.get("select_columns", [])
    validate_columns_exist(select_columns, table)
    
    # 5. Validate filter columns and values
    filters = plan.get("filters", [])
    validate_filter_values(filters, table)
    
    # 6. Validate group_by columns exist
    group_by = plan.get("group_by", [])
    validate_columns_exist(group_by, table)
    
    # 7. Validate order_by columns exist
    order_by = plan.get("order_by", [])
    if order_by:
        order_columns = [col[0] for col in order_by]
        validate_columns_exist(order_columns, table)

    query_type = plan.get("query_type")
    
    # 8. Type-specific validation
    if query_type == "metric":
        # Validate metrics exist and match table
        metrics = plan.get("metrics", [])
        if not metrics:
            raise ValueError("Metric queries must specify at least one metric")
        
        validate_metric_table_mapping(metrics, table)
    
    elif query_type == "lookup":
        # Lookup queries cannot use metrics
        if plan.get("metrics"):
            raise ValueError("Lookup queries cannot use aggregation metrics")
        
        # Must have LIMIT 1
        if plan.get("limit") != 1:
            raise ValueError("Lookup queries must have LIMIT 1")
        
        # Must have filters
        if not plan.get("filters"):
            raise ValueError("Lookup queries must have filters")
    
    elif query_type == "filter":
        # Filter queries cannot use metrics
        if plan.get("metrics"):
            raise ValueError("Filter queries cannot use aggregation metrics")
        
        # Must have filters
        if not plan.get("filters"):
            raise ValueError("Filter queries must have filters")
    
    elif query_type == "extrema_lookup":
        # Extrema lookup must have order_by
        if not plan.get("order_by"):
            raise ValueError("Extrema lookup queries must have order_by")
        
        # Must have LIMIT 1
        if plan.get("limit") != 1:
            raise ValueError("Extrema lookup queries must have LIMIT 1")
    
    elif query_type == "rank":
        # Rank queries must have order_by
        if not plan.get("order_by"):
            raise ValueError("Rank queries must have order_by")
    
    elif query_type == "list":
        # List queries are simple, no special validation needed
        pass
    
    elif query_type == "aggregation_on_subset":
        # Aggregation on subset must have aggregation_function and aggregation_column
        if not plan.get("aggregation_function"):
            raise ValueError("Aggregation on subset queries must have aggregation_function")
        
        if not plan.get("aggregation_column"):
            raise ValueError("Aggregation on subset queries must have aggregation_column")
        
        # Validate aggregation function
        allowed_functions = ["AVG", "SUM", "COUNT", "MAX", "MIN"]
        if plan["aggregation_function"] not in allowed_functions:
            raise ValueError(f"Invalid aggregation function: {plan['aggregation_function']}. Allowed: {allowed_functions}")
        
        # Validate aggregation column exists
        agg_column = plan["aggregation_column"]
        validate_columns_exist([agg_column], table)
        
        # Validate subset_order_by if present
        subset_order_by = plan.get("subset_order_by", [])
        if subset_order_by:
            subset_order_columns = [col[0] for col in subset_order_by]
            validate_columns_exist(subset_order_columns, table)
        
        # Validate subset_filters if present
        subset_filters = plan.get("subset_filters", [])
        if subset_filters:
            validate_filter_values(subset_filters, table)

    return True
