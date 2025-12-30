from schema_intelligence.hybrid_retriever import retrieve_schema
from planning_layer.planner_client import generate_plan  # Changed from rule_based_planner
from validation_layer.plan_validator import validate_plan
from execution_layer.executor import execute_plan
from explanation_layer.explainer_client import explain_results
from data_sources.gsheet.connector import fetch_sheets_with_tables
from data_sources.gsheet.change_detector import needs_refresh
from data_sources.gsheet.snapshot_loader import load_snapshot
from schema_intelligence.chromadb_client import SchemaVectorStore # Moved import to top
from utils.memory_detector import detect_memory_intent
from utils.permanent_memory import update_memory
from utils.greeting_detector import is_greeting, get_greeting_response

def run(question: str):
    """
    Main query execution pipeline with sheet-level hash-based change detection.
    
    Change Detection Logic:
    - Fetches sheets and computes raw sheet hashes
    - Compares hashes to detect changes
    - Performs targeted rebuilds for changed sheets only
    - Falls back to full reset if spreadsheet ID changed or first run
    """
    
    # STEP -1: Check for greetings FIRST
    if is_greeting(question):
        greeting_response = get_greeting_response(question)
        print("\n" + "="*80)
        print("ANSWER:")
        print("="*80)
        print(greeting_response)
        print("="*80 + "\n")
        return
    
    # STEP 0: Check for memory intent BEFORE any processing
    memory_result = detect_memory_intent(question)
    if memory_result and memory_result.get("has_memory_intent"):
        # Extract memory instruction
        category = memory_result["category"]
        key = memory_result["key"]
        value = memory_result["value"]
        
        # Store memory
        success = update_memory(category, key, value)
        
        if success:
            print(f"\n✓ Memory stored: {category}.{key} = {value}")
            print("Got it. I'll remember that.\n")
            return  # Exit early - memory storage is the only action needed
        else:
            print(f"\n⚠️  Failed to store memory")
    
    # STEP 1: Fetch sheets and detect changes
    # This computes raw sheet hashes before any processing
    print("🔍 Checking for data changes...")
    sheets_with_tables = fetch_sheets_with_tables()
    
    # STEP 2: Determine if refresh is needed
    # Returns (needs_refresh, full_reset_required, changed_sheets)
    needs_refresh_flag, full_reset, changed_sheets = needs_refresh(sheets_with_tables)
    
    # Initialize schema store
    store = SchemaVectorStore()
    
    if needs_refresh_flag:
        if full_reset:
            # FULL RESET: Spreadsheet ID changed or first run
            print("📊 Performing FULL RESET (spreadsheet ID changed or first run)...")
            
            # Clear ChromaDB
            store.clear_collection()
            print("  ✓ Schema embeddings cleared")
            
            # Reset DuckDB and rebuild all sheets
            load_snapshot(sheets_with_tables, full_reset=True)
            print("  ✓ DuckDB snapshot reset complete")
            
            # Rebuild all ChromaDB embeddings
            store.rebuild()
            print("  ✓ Schema embeddings rebuilt\n")
            
        else:
            # INCREMENTAL REBUILD: Only changed sheets
            print(f"📊 Performing INCREMENTAL REBUILD for {len(changed_sheets)} changed sheet(s)...")
            print(f"   Changed sheets: {', '.join(changed_sheets)}")
            
            # Get source_ids for changed sheets
            source_ids = []
            for sheet_name in changed_sheets:
                if sheet_name in sheets_with_tables and sheets_with_tables[sheet_name]:
                    source_id = sheets_with_tables[sheet_name][0].get('source_id')
                    if source_id:
                        source_ids.append(source_id)
            
            # Rebuild DuckDB tables for changed sheets only
            load_snapshot(sheets_with_tables, full_reset=False, changed_sheets=changed_sheets)
            print("  ✓ DuckDB tables rebuilt for changed sheets")
            
            # Rebuild ChromaDB embeddings for changed sheets only
            if source_ids:
                store.rebuild(source_ids=source_ids)
                print("  ✓ Schema embeddings rebuilt for changed sheets\n")
            else:
                # Fallback to full rebuild if source_ids not available
                store.rebuild()
                print("  ✓ Schema embeddings rebuilt (full rebuild)\n")
    else:
        print("✓ No changes detected - using cached data\n")
    
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
    
    # Fallback: If lookup/filter query returns empty, try alternative tables
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
    run("What average temperature on 02/01/2017?")  
 