import duckdb
import yaml


def quote_identifier(name: str) -> str:
    """Quote SQL identifiers that contain spaces or special characters"""
    if ' ' in name or any(char in name for char in ['-', '.', '(', ')']):
        return f'"{name}"'
    return name


def _infer_semantic_type(column_name: str, column_type: str):
    """
    Infer semantic type from column metadata.
    Enhanced for e-commerce/sales domains.
    No data access - metadata only.
    """
    col_lower = column_name.lower()
    
    # Entity identifiers (PII)
    if any(x in col_lower for x in ['name', 'email', 'gmail', 'id', 'identifier', 'register', 
                                      'customer', 'user', 'account', 'contact']):
        return "entity_identifier"
    
    # Numeric measures (aggregatable metrics)
    # Check actual DuckDB type first
    if column_type in ['DOUBLE', 'FLOAT', 'INTEGER', 'BIGINT', 'DECIMAL', 'NUMERIC', 'HUGEINT']:
        # Common business metrics
        if any(x in col_lower for x in ['cgpa', 'gpa', 'score', 'count', 'amount', 'total', 'sum', 
                                          'quantity', 'qty', 'price', 'cost', 'revenue', 'sales',
                                          'profit', 'margin', 'discount', 'tax', 'shipping', 'fee',
                                          'value', 'worth', 'payment', 'charge', 'rate', 'number']):
            return "numeric_measure"
        return "numeric_attribute"
    
    # Categorical attributes (dimensions for grouping)
    if any(x in col_lower for x in ['campus', 'major', 'degree', 'category', 'type', 'status',
                                      'state', 'country', 'region', 'city', 'area', 'zone',
                                      'product', 'item', 'sku', 'brand', 'model', 'variant',
                                      'channel', 'source', 'method', 'mode', 'platform',
                                      'fulfilled', 'pending', 'cancelled', 'shipped', 'delivered',
                                      'paid', 'unpaid', 'refund', 'return']):
        return "categorical_attribute"
    
    # Temporal attributes (time-based filtering/grouping)
    if any(x in col_lower for x in ['date', 'time', 'year', 'month', 'day', 'week',
                                      'created', 'updated', 'modified', 'timestamp',
                                      'at', 'on', 'period', 'quarter', 'season']):
        return "temporal_attribute"
    
    # Check for datetime types from DuckDB
    if column_type in ['DATE', 'TIMESTAMP', 'TIME', 'DATETIME']:
        return "temporal_attribute"
    
    # Boolean types
    if column_type in ['BOOLEAN', 'BOOL']:
        return "categorical_attribute"
    
    return "unknown"


def extract_schema(
    db_path="data_sources/snapshots/latest.duckdb",
    metric_path="config/metric_definitions.yaml"
):
    """
    Extracts schema metadata with semantic types.
    Filters out non-analytical tables.
    No row access. No aggregates. No samples.
    """

    conn = duckdb.connect(db_path)

    # Load metric definitions (optional)
    metrics = {}
    try:
        from pathlib import Path
        if Path(metric_path).exists():
            with open(metric_path) as f:
                config = yaml.safe_load(f)
                if config and "metrics" in config:
                    metrics = config["metrics"]
    except Exception:
        # Metrics are optional - continue without them
        pass

    schema = {
        "tables": {},
        "metrics": {}
    }

    tables = conn.execute("SHOW TABLES").fetchall()

    for (table_name,) in tables:
        # Include ALL tables (not just those in metrics)
        # This allows querying any sheet in the Google Sheets workbook
        
        quoted_table = quote_identifier(table_name)
        columns = conn.execute(f"DESCRIBE {quoted_table}").fetchall()

        if not columns:
            continue

        # Add table description for _long tables
        table_description = "INFERRED"
        if table_name.endswith("_long"):
            table_description = f"Long format version of {table_name[:-5]} with Date, Hours, and Status columns. Use this table for date-based queries like 'who was absent on [date]' or 'hours worked on [date]'. Status values: 'A' = Absent, 'P' = Present."

        schema["tables"][table_name] = {
            "columns": {},
            "grain": "UNKNOWN",
            "description": table_description
        }

        for col_name, col_type, *_ in columns:
            semantic_type = _infer_semantic_type(col_name, col_type)
            
            # Special descriptions for _long table columns
            description = "INFERRED"
            if table_name.endswith("_long"):
                if col_name == "Status":
                    description = "Attendance status: 'A' for Absent, 'P' for Present"
                elif col_name == "Date":
                    description = "Date in YYYY-MM-DD format"
                elif col_name == "Hours":
                    description = "Hours worked (0.0 for absent days)"
            
            schema["tables"][table_name]["columns"][col_name] = {
                "type": col_type,
                "semantic_type": semantic_type,
                "metric_candidate": semantic_type == "numeric_measure",
                "sensitive": semantic_type == "entity_identifier",
                "description": description
            }

    # Attach metric semantics (if any)
    for metric_name, definition in metrics.items():
        schema["metrics"][metric_name] = {
            "description": definition["description"],
            "base_table": definition["base_table"],
            "allowed_dimensions": definition["allowed_dimensions"]
        }

    conn.close()
    return schema
