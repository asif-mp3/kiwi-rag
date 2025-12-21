def run_sanity_checks(df, query_type=None, allow_nulls=True, allow_negatives=True, max_null_percentage=50):
    """
    Data quality checks with configurable thresholds.
    Designed for real-world data that may have NULLs and negative values.
    
    Args:
        df: DataFrame to validate
        query_type: Type of query (filter, lookup, etc.) - empty results are OK for some types
        allow_nulls: If True, allow NULL values (default: True)
        allow_negatives: If True, allow negative numbers (default: True)
        max_null_percentage: Maximum percentage of NULLs allowed per column (default: 50)
    """
    
    # Check for completely empty result
    # Empty results are valid for filter, lookup, list, rank queries (means no matches)
    # Also valid for aggregation_on_subset if the subset is empty
    if df.empty:
        # Allow empty results for queries that filter/search data
        if query_type in ["filter", "lookup", "list", "rank", "extrema_lookup", "aggregation_on_subset"]:
            return True  # Empty is OK - means no matches found
        # For metric queries, empty results are suspicious
        raise ValueError("Sanity check failed: Result set is empty")
    
    # Check NULL percentage per column (warn if high, error if extreme)
    if not allow_nulls:
        if df.isnull().any().any():
            raise ValueError("Sanity check failed: NULL values detected (strict mode)")
    else:
        # Warn about high NULL percentages
        for col in df.columns:
            null_pct = (df[col].isnull().sum() / len(df)) * 100
            if null_pct > max_null_percentage:
                print(f"⚠️  Warning: Column '{col}' has {null_pct:.1f}% NULL values")
            if null_pct == 100:
                print(f"⚠️  Warning: Column '{col}' is completely NULL")
    
    # Check for negative values (optional)
    if not allow_negatives:
        for col in df.columns:
            if df[col].dtype.kind in "if":  # numeric types
                if (df[col] < 0).any():
                    raise ValueError(f"Sanity check failed: negative values in {col} (strict mode)")
    
    # Warn about very large result sets (potential performance issue)
    if len(df) > 10000:
        print(f"⚠️  Warning: Large result set ({len(df):,} rows). Consider adding filters or limits.")
    
    # Check for suspicious data patterns
    for col in df.columns:
        if df[col].dtype.kind in "if":  # numeric types
            # Check for all zeros
            if (df[col].fillna(0) == 0).all():
                print(f"⚠️  Warning: Column '{col}' contains only zeros")
    
    return True
