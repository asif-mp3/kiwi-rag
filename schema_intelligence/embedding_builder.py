from schema_intelligence.schema_extractor import extract_schema


def build_schema_documents():
    """
    Converts schema metadata into text blocks for embedding.
    No data values included.
    """

    schema = extract_schema()
    documents = []

    # Table-level documents
    for table, meta in schema["tables"].items():
        column_descriptions = []
        
        for col, info in meta["columns"].items():
            semantic = info.get('semantic_type', 'unknown')
            col_desc = f"{col} ({info['type']}, {semantic})"
            column_descriptions.append(col_desc)
        
        column_str = ", ".join(column_descriptions)

        text = (
            f"Table '{table}'. "
            f"Columns: {column_str}. "
            f"Grain: {meta.get('grain', 'UNKNOWN')}."
        )

        documents.append({
            "id": f"table::{table}",
            "text": text,
            "type": "table",
            "table": table
        })

    # Metric-level documents
    for metric, meta in schema["metrics"].items():
        text = (
            f"Metric '{metric}': {meta['description']}. "
            f"Base table: {meta['base_table']}. "
            f"Allowed dimensions: {', '.join(meta['allowed_dimensions'])}."
        )

        documents.append({
            "id": f"metric::{metric}",
            "text": text,
            "type": "metric",
            "metric": metric
        })

    return documents
