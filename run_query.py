from schema_intelligence.hybrid_retriever import retrieve_schema
from planning_layer.planner_client import generate_plan  # Changed from rule_based_planner
from validation_layer.plan_validator import validate_plan
from execution_layer.executor import execute_plan
from explanation_layer.explainer_client import explain_results
from data_sources.gsheet.change_detector import needs_refresh, mark_synced
from data_sources.gsheet.snapshot_loader import load_snapshot
from schema_intelligence.chromadb_client import SchemaVectorStore # Moved import to top

def reset_snapshot(sheets):
    """Performs a full reset of the DuckDB snapshot and clears schema embeddings."""
    store = SchemaVectorStore()
    store.clear_collection()
    print("  ✓ Schema embeddings cleared")
    
    load_snapshot(sheets, full_reset=True)
    print("  ✓ DuckDB snapshot reset complete")

def update_snapshot(sheets):
    """Performs an incremental update of the DuckDB snapshot."""
    # For now, incremental update is just loading the snapshot without full_reset
    # In the future, this could be optimized to only update changed tables
    load_snapshot(sheets, full_reset=False)
    print("  ✓ DuckDB snapshot updated")

def run(question: str):
    # Smart refresh: only reload if Google Sheets has changed
    # IMPORTANT: needs_refresh() returns (bool, bool, sheets) to avoid double-fetching
    needs_refresh_flag, full_reset, current_sheets = needs_refresh()
    
    # Initialize schema store
    store = SchemaVectorStore()
    
    if needs_refresh_flag:
        if full_reset:
            # Full reset: Clear everything and rebuild from scratch
            print("📊 Sheet structure changed - performing FULL RESET...")
            reset_snapshot(current_sheets)
            store.rebuild()
            print("✓ Schema embeddings rebuilt\n")
        else:
            # Incremental refresh (content changed but structure same)
            print("📊 Content changed - performing incremental refresh...")
            update_snapshot(current_sheets)
            store.rebuild()
            print("✓ Schema embeddings rebuilt\n")
        
        # CRITICAL: Save new fingerprints after refresh
        mark_synced(current_sheets)
    
    print("\n" + "="*80)
    print("QUESTION:")
    print(question)
    print("="*80)

    # 1. Schema retrieval (meaning only)
    schema_context = retrieve_schema(question)
    # Uncomment to see schema context:
    print("\nRETRIEVED SCHEMA CONTEXT:")
    for item in schema_context:
        print("-", item["text"])

    # 2. Planning
    plan = generate_plan(question, schema_context)
    validate_plan(plan)
    
    # Uncomment to see query plan:
    print("\nQUERY PLAN:")
    print(plan)

    # 3. Execution
    result = execute_plan(plan)
    
    # Fallback: If lookup/filter query returns empty, try alternative tables.
    if result.empty and plan["query_type"] in ["lookup", "filter"]:
        print("\n⚠️  No results found. Trying alternative tables...")
        
        # Get all tables from schema context
        alternative_tables = []
        for item in schema_context:
            meta = item.get("metadata", {})
            if meta.get("type") == "table":
                table_name = meta.get("table")
                if table_name and table_name != plan["table"]:
                    alternative_tables.append(table_name)
        
        # Try each alternative table
        for alt_table in alternative_tables:
            try:
                alt_plan = plan.copy()
                alt_plan["table"] = alt_table
                
                print(f"  Trying table: {alt_table}")
                validate_plan(alt_plan)
                alt_result = execute_plan(alt_plan)
                
                if not alt_result.empty:
                    print(f"  ✓ Found results in {alt_table}!")
                    result = alt_result
                    plan = alt_plan  # Update plan for explanation
                    break
            except Exception as e:
                # Skip tables that don't have the required columns
                print(f"  ✗ {alt_table}: {str(e)[:50]}...")
                continue
    
    # Uncomment to see raw dataframe:
    print("\nEXECUTION RESULT (DATAFRAME):")
    print(result)

    # 4. Explanation
    explanation = explain_results(result, query_plan=plan, original_question=question)
    print("\n" + "="*80)
    print("ANSWER:")
    print("="*80)
    print(explanation)
    print("="*80 + "\n")


if __name__ == "__main__":
    run("What was the temperature at 10:00 on 02/01/2017?")  
