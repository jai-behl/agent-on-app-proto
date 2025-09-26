#!/usr/bin/env python3
"""
Test script for System Tables Agent capabilities.

This script tests the system tables tools and agent integration,
following Anthropic's evaluation methodology.
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from agent_server.system_tables_tools import (
    SYSTEM_TABLES_TOOLS, 
    SYSTEM_TABLES_FUNCTIONS,
    cost_daily_breakdown,
    cost_forecast_monthly,
    cost_top_drivers,
    hygiene_idle_warehouses,
    hygiene_underutilized_clusters,
    query_expensive_analysis
)
from agent_server.system_tables_evaluation import SystemTablesEvaluator, create_evaluation_report


def test_tool_registration():
    """Test that all tools are properly registered."""
    print("🧪 Testing tool registration...")
    
    expected_tools = [
        "cost_daily_breakdown",
        "cost_forecast_monthly", 
        "cost_top_drivers",
        "hygiene_idle_warehouses",
        "hygiene_underutilized_clusters",
        "query_expensive_analysis"
    ]
    
    # Check tool specs
    tool_names = [tool["function"]["name"] for tool in SYSTEM_TABLES_TOOLS]
    print(f"📋 Registered tool specs: {tool_names}")
    
    # Check function mapping
    function_names = list(SYSTEM_TABLES_FUNCTIONS.keys())
    print(f"🔧 Available functions: {function_names}")
    
    # Verify all expected tools are present
    missing_tools = set(expected_tools) - set(tool_names)
    if missing_tools:
        print(f"❌ Missing tools: {missing_tools}")
        return False
    
    missing_functions = set(expected_tools) - set(function_names)
    if missing_functions:
        print(f"❌ Missing functions: {missing_functions}")
        return False
    
    print("✅ All tools properly registered")
    return True


def test_tool_execution(warehouse_id: str):
    """Test individual tool execution."""
    print(f"\n🧪 Testing tool execution with warehouse: {warehouse_id}")
    
    # Test each tool with basic parameters
    test_cases = [
        ("cost_daily_breakdown", {"warehouse_id": warehouse_id, "days_back": 3}),
        ("cost_forecast_monthly", {"warehouse_id": warehouse_id}),
        ("cost_top_drivers", {"warehouse_id": warehouse_id, "top_n": 5, "days_back": 3}),
        ("hygiene_idle_warehouses", {"warehouse_id": warehouse_id, "days_back": 3}),
        ("hygiene_underutilized_clusters", {"warehouse_id": warehouse_id, "days_back": 3}),
        ("query_expensive_analysis", {"warehouse_id": warehouse_id, "top_n": 5, "days_back": 3})
    ]
    
    results = {}
    
    for tool_name, params in test_cases:
        print(f"  🔧 Testing {tool_name}...")
        try:
            func = SYSTEM_TABLES_FUNCTIONS[tool_name]
            result = func(**params)
            
            if isinstance(result, str) and "error" in result.lower():
                print(f"    ⚠️ Tool returned error: {result[:100]}...")
                results[tool_name] = {"success": False, "error": result}
            else:
                print(f"    ✅ Success: {len(str(result))} chars returned")
                results[tool_name] = {"success": True, "response_length": len(str(result))}
                
        except Exception as e:
            print(f"    ❌ Exception: {str(e)}")
            results[tool_name] = {"success": False, "error": str(e)}
    
    return results


def test_agent_integration():
    """Test agent integration with system tables tools."""
    print("\n🧪 Testing agent integration...")
    
    try:
        from agent_server.agent import TOOL_INFOS, ToolCallingAgent
        
        # Check if system tables tools are in TOOL_INFOS
        system_tool_names = [tool.name for tool in TOOL_INFOS if tool.name.startswith(('cost_', 'hygiene_', 'query_'))]
        print(f"📋 System tables tools in agent: {system_tool_names}")
        
        if len(system_tool_names) == 6:
            print("✅ All system tables tools integrated into agent")
            return True
        else:
            print(f"❌ Expected 6 system tables tools, found {len(system_tool_names)}")
            return False
            
    except Exception as e:
        print(f"❌ Agent integration test failed: {str(e)}")
        return False


def test_evaluation_framework(warehouse_id: str):
    """Test the evaluation framework."""
    print(f"\n🧪 Testing evaluation framework with warehouse: {warehouse_id}")
    
    try:
        evaluator = SystemTablesEvaluator(warehouse_id=warehouse_id)
        print(f"📋 Generated {len(evaluator.evaluation_tasks)} evaluation tasks")
        
        # Run a subset of evaluation tasks
        sample_results = evaluator.run_evaluation_suite(task_filter="cost_001")
        
        if sample_results and "evaluation_summary" in sample_results:
            summary = sample_results["evaluation_summary"]
            print(f"✅ Evaluation completed: {summary['passed_tasks']}/{summary['total_tasks']} passed")
            return True
        else:
            print("❌ Evaluation framework test failed")
            return False
            
    except Exception as e:
        print(f"❌ Evaluation framework test failed: {str(e)}")
        return False


def main():
    """Main test function."""
    print("🚀 System Tables Agent Test Suite")
    print("=" * 50)
    
    # Check for required environment variables
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        print("❌ DATABRICKS_WAREHOUSE_ID environment variable not set")
        print("   Please set it to test with actual Databricks connection")
        print("   Example: export DATABRICKS_WAREHOUSE_ID='your-warehouse-id'")
        warehouse_id = "test-warehouse-id"  # Use dummy ID for basic tests
    else:
        print(f"✅ Using warehouse: {warehouse_id}")
    
    test_results = {}
    
    # Run tests
    test_results["tool_registration"] = test_tool_registration()
    test_results["agent_integration"] = test_agent_integration()
    
    if warehouse_id != "test-warehouse-id":
        test_results["tool_execution"] = test_tool_execution(warehouse_id)
        test_results["evaluation_framework"] = test_evaluation_framework(warehouse_id)
    else:
        print("\n⚠️ Skipping tool execution and evaluation tests (no real warehouse ID)")
        test_results["tool_execution"] = "skipped"
        test_results["evaluation_framework"] = "skipped"
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    for test_name, result in test_results.items():
        if result is True:
            print(f"✅ {test_name}: PASSED")
        elif result is False:
            print(f"❌ {test_name}: FAILED")
        elif result == "skipped":
            print(f"⏭️ {test_name}: SKIPPED")
        elif isinstance(result, dict):
            passed = sum(1 for r in result.values() if isinstance(r, dict) and r.get("success", False))
            total = len(result)
            print(f"📊 {test_name}: {passed}/{total} tools passed")
    
    print("\n💡 Next steps:")
    print("1. Set DATABRICKS_WAREHOUSE_ID to test with real data")
    print("2. Run the agent server: python -m agent_server.agent")
    print("3. Test via the web UI or API calls")
    print("4. Run full evaluation suite for performance tuning")


if __name__ == "__main__":
    main()
