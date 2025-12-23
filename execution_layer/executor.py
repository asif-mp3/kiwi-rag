from analytics_engine.duckdb_manager import DuckDBManager
from execution_layer.sql_compiler import compile_sql
from analytics_engine.sanity_checks import run_sanity_checks


def execute_plan(plan: dict):
    sql = compile_sql(plan)
    db = DuckDBManager()
    result_df = db.query(sql)

    # Pass query_type to sanity checks to allow empty results for filter/lookup queries
    run_sanity_checks(result_df, query_type=plan.get("query_type"))

    
    # For aggregation_on_subset, the result is already calculated by DuckDB
    # Just attach metadata for the explainer
    if plan.get("query_type") == "aggregation_on_subset":
        aggregation_function = plan["aggregation_function"]
        aggregation_column = plan["aggregation_column"]
        
        # Store metadata for the explainer
        result_df.attrs['aggregation_function'] = aggregation_function
        result_df.attrs['aggregation_column'] = aggregation_column
        result_df.attrs['query_type'] = 'aggregation_on_subset'

    return result_df

