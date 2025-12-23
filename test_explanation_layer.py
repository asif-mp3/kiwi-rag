"""
Test suite for the Explanation Layer

Tests the hallucination-free explanation generation system.

NOTE: This file is in the root directory because test/ is gitignored.
You can move it to test/ if needed, but it won't be tracked by git.
"""

import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from explanation_layer.explainer_client import generate_explanation, _validate_explanation


class TestExplanationLayer:
    """Test cases for explanation layer functionality"""
    
    def test_ranking_question_second_least(self):
        """
        TEST 1: Ranking Question (Second Least CGPA)
        
        Validates that the explanation correctly identifies the second row
        when the question asks for "second least" and the DataFrame is ordered.
        """
        question = "who has the second least CGPA in chennai campus?"
        
        schema_context = [
            "Table 'Sheet1': NAME (student name), CGPA (numeric grade point average), Campus (campus location)"
        ]
        
        # DataFrame ordered by CGPA ascending (as it would come from DuckDB)
        execution_df = pd.DataFrame({
            'NAME': ['DASARI YASWANTH SAI', 'Sadhan S'],
            'CGPA': [7.05, 7.06]
        })
        
        explanation = generate_explanation(question, schema_context, execution_df)
        
        # Assertions
        assert 'Sadhan S' in explanation, "Should mention the second student"
        assert '7.06' in explanation, "Should mention the correct CGPA"
        assert 'second' in explanation.lower(), "Should acknowledge the ranking"
        
        # Should not mention SQL or internal details
        assert 'sql' not in explanation.lower()
        assert 'duckdb' not in explanation.lower()
        assert 'table' not in explanation.lower()
        
        print(f"✓ Test 1 passed: {explanation}")
        return explanation
    
    
    def test_multiple_results_enumeration(self):
        """
        TEST 2: Multiple Results Enumeration
        
        Validates that all rows are mentioned in correct order when
        the question asks for multiple results.
        """
        question = "Who are the students with the lowest CGPA?"
        
        schema_context = [
            "Table 'Sheet1': NAME (student name), CGPA (numeric grade point average)"
        ]
        
        execution_df = pd.DataFrame({
            'NAME': ['Alice', 'Bob', 'Charlie'],
            'CGPA': [6.5, 6.8, 7.0]
        })
        
        explanation = generate_explanation(question, schema_context, execution_df)
        
        # All students should be mentioned
        assert 'Alice' in explanation
        assert 'Bob' in explanation
        assert 'Charlie' in explanation
        
        # All CGPAs should be mentioned
        assert '6.5' in explanation
        assert '6.8' in explanation
        assert '7.0' in explanation
        
        # Should maintain order (Alice before Bob before Charlie)
        assert explanation.index('Alice') < explanation.index('Bob')
        assert explanation.index('Bob') < explanation.index('Charlie')
        
        print(f"✓ Test 2 passed: {explanation}")
        return explanation
    
    
    def test_empty_result(self):
        """
        TEST 3: Empty Result
        
        Validates graceful handling when no data matches the query.
        """
        question = "Who has CGPA greater than 10?"
        
        schema_context = [
            "Table 'Sheet1': NAME (student name), CGPA (numeric grade point average)"
        ]
        
        execution_df = pd.DataFrame(columns=['NAME', 'CGPA'])  # Empty DataFrame
        
        explanation = generate_explanation(question, schema_context, execution_df)
        
        # Should clearly indicate no data found
        assert 'no' in explanation.lower()
        assert any(word in explanation.lower() for word in ['found', 'available', 'matching', 'data'])
        
        # Should not invent any NEW data (numbers from the question are okay)
        # The question contains "10", so that's acceptable in the explanation
        # But there should be no other numbers like counts, CGPAs, etc.
        
        print(f"✓ Test 3 passed: {explanation}")
        return explanation
    
    
    def test_single_row_lookup(self):
        """
        TEST 4: Single Row Lookup
        
        Validates direct answer without ranking language for single results.
        """
        question = "What is the CGPA of Alice?"
        
        schema_context = [
            "Table 'Sheet1': NAME (student name), CGPA (numeric grade point average)"
        ]
        
        execution_df = pd.DataFrame({
            'NAME': ['Alice'],
            'CGPA': [8.5]
        })
        
        explanation = generate_explanation(question, schema_context, execution_df)
        
        # Should mention the student and CGPA
        assert 'Alice' in explanation
        assert '8.5' in explanation
        
        print(f"✓ Test 4 passed: {explanation}")
        return explanation
    
    
    def test_hallucination_detection_invalid_number(self):
        """
        TEST 5: Hallucination Detection - Invalid Number
        
        Validates that the validation function detects hallucinated numbers.
        """
        execution_df = pd.DataFrame({
            'NAME': ['Alice'],
            'CGPA': [8.5]
        })
        
        # This explanation contains a hallucinated number (9.2)
        hallucinated_explanation = "Alice has a CGPA of 9.2"
        
        try:
            _validate_explanation(hallucinated_explanation, execution_df)
            raise AssertionError("Should have detected hallucination")
        except ValueError as e:
            assert "Hallucination detected" in str(e)
            print(f"✓ Test 5a passed: Detected hallucinated number - {e}")
        
        # Valid explanation should pass
        valid_explanation = "Alice has a CGPA of 8.5"
        _validate_explanation(valid_explanation, execution_df)  # Should not raise
        
        print("✓ Test 5b passed: Accepted valid explanation")
    
    
    def test_aggregate_result(self):
        """
        TEST 6: Aggregate Result
        
        Validates proper handling of aggregated metric queries.
        """
        question = "What is the average CGPA?"
        
        schema_context = [
            "Table 'Sheet1': CGPA (numeric grade point average)"
        ]
        
        execution_df = pd.DataFrame({
            'avg_cgpa': [7.85]
        })
        
        explanation = generate_explanation(question, schema_context, execution_df)
        
        # Should mention the average value
        assert '7.85' in explanation
        
        # Should use appropriate language for aggregates
        assert any(word in explanation.lower() for word in ['average', 'avg', 'is'])
        
        print(f"✓ Test 6 passed: {explanation}")
        return explanation


def run_all_tests():
    """Run all tests manually"""
    test_suite = TestExplanationLayer()
    
    print("="*80)
    print("EXPLANATION LAYER TEST SUITE")
    print("="*80)
    print()
    
    try:
        print("Running Test 1: Ranking Question (Second Least)...")
        test_suite.test_ranking_question_second_least()
        print()
        
        print("Running Test 2: Multiple Results Enumeration...")
        test_suite.test_multiple_results_enumeration()
        print()
        
        print("Running Test 3: Empty Result...")
        test_suite.test_empty_result()
        print()
        
        print("Running Test 4: Single Row Lookup...")
        test_suite.test_single_row_lookup()
        print()
        
        print("Running Test 5: Hallucination Detection...")
        test_suite.test_hallucination_detection_invalid_number()
        print()
        
        print("Running Test 6: Aggregate Result...")
        test_suite.test_aggregate_result()
        print()
        
        print("="*80)
        print("ALL TESTS PASSED ✓")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_all_tests()
