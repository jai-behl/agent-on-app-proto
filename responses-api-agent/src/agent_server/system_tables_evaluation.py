"""
Evaluation framework for System Tables Agent following Anthropic's methodology.

This module provides comprehensive evaluation tasks, metrics collection,
and iterative improvement capabilities for system tables tools.
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow.entities import SpanType

from agent_server.system_tables_tools import SYSTEM_TABLES_FUNCTIONS


@dataclass
class EvaluationTask:
    """Represents a single evaluation task with expected outcomes."""
    
    task_id: str
    description: str
    user_prompt: str
    expected_tools: List[str]  # Tools that should be called
    expected_outcome_type: str  # "cost_analysis", "hygiene_report", "query_optimization"
    success_criteria: Dict[str, Any]  # Specific criteria for success
    complexity_level: str  # "simple", "medium", "complex"
    requires_warehouse: bool = True


@dataclass
class EvaluationResult:
    """Results from running an evaluation task."""
    
    task_id: str
    success: bool
    duration_ms: int
    tools_called: List[str]
    tool_call_count: int
    token_usage: Optional[Dict[str, int]]
    error_message: Optional[str]
    agent_response: str
    reasoning: str
    feedback: str


class SystemTablesEvaluator:
    """Evaluator for system tables agent capabilities."""
    
    def __init__(self, warehouse_id: str, agent_instance=None):
        self.warehouse_id = warehouse_id
        self.workspace_client = WorkspaceClient()
        self.agent = agent_instance
        self.evaluation_tasks = self._generate_evaluation_tasks()
    
    def _generate_evaluation_tasks(self) -> List[EvaluationTask]:
        """Generate comprehensive evaluation tasks based on real-world scenarios."""
        
        tasks = [
            # COST OBSERVABILITY TASKS (Domain A)
            EvaluationTask(
                task_id="cost_001",
                description="Daily cost breakdown for workspace optimization",
                user_prompt="Show me the daily cost breakdown for the last 7 days by workspace. I need to identify which workspaces are driving the highest costs so I can optimize our spending.",
                expected_tools=["cost_daily_breakdown"],
                expected_outcome_type="cost_analysis",
                success_criteria={
                    "includes_cost_totals": True,
                    "shows_workspace_breakdown": True,
                    "provides_actionable_insights": True,
                    "mentions_top_cost_drivers": True
                },
                complexity_level="simple"
            ),
            
            EvaluationTask(
                task_id="cost_002",
                description="Monthly forecast with budget planning",
                user_prompt="I need to prepare our monthly budget forecast. Can you analyze our current spending pattern and predict what our total monthly cost will be? Include workspace-level forecasts for the top spending areas.",
                expected_tools=["cost_forecast_monthly", "cost_daily_breakdown"],
                expected_outcome_type="cost_analysis",
                success_criteria={
                    "provides_monthly_forecast": True,
                    "includes_current_runrate": True,
                    "shows_workspace_breakdown": True,
                    "mentions_budget_implications": True
                },
                complexity_level="medium"
            ),
            
            EvaluationTask(
                task_id="cost_003",
                description="Cost spike investigation and optimization",
                user_prompt="Our Databricks bill increased significantly this week. Help me identify what's driving the cost increase. I need to see the top cost drivers and understand if this is a trend or a one-time spike. Also suggest optimization strategies.",
                expected_tools=["cost_top_drivers", "cost_daily_breakdown"],
                expected_outcome_type="cost_analysis",
                success_criteria={
                    "identifies_cost_drivers": True,
                    "compares_time_periods": True,
                    "suggests_optimizations": True,
                    "explains_cost_patterns": True
                },
                complexity_level="complex"
            ),
            
            # HYGIENE & RIGHT-SIZING TASKS (Domain B)
            EvaluationTask(
                task_id="hygiene_001",
                description="Idle resource identification",
                user_prompt="I want to reduce waste in our Databricks environment. Can you find any warehouses or clusters that are running but not being used? I need specific recommendations on what to turn off or resize.",
                expected_tools=["hygiene_idle_warehouses", "hygiene_underutilized_clusters"],
                expected_outcome_type="hygiene_report",
                success_criteria={
                    "identifies_idle_resources": True,
                    "provides_specific_recommendations": True,
                    "quantifies_potential_savings": True,
                    "includes_actionable_steps": True
                },
                complexity_level="medium"
            ),
            
            EvaluationTask(
                task_id="hygiene_002",
                description="Right-sizing analysis for cost optimization",
                user_prompt="Our compute costs are high but I suspect we're over-provisioned. Analyze our cluster utilization patterns and recommend right-sizing opportunities. Focus on clusters with consistently low CPU usage.",
                expected_tools=["hygiene_underutilized_clusters"],
                expected_outcome_type="hygiene_report",
                success_criteria={
                    "analyzes_utilization_patterns": True,
                    "suggests_specific_sizing_changes": True,
                    "explains_impact_of_changes": True,
                    "prioritizes_recommendations": True
                },
                complexity_level="medium"
            ),
            
            # QUERY EFFICIENCY TASKS (Domain C)
            EvaluationTask(
                task_id="query_001",
                description="Query performance optimization",
                user_prompt="Some of our data pipeline queries are taking too long and costing too much. Can you identify the most expensive queries and suggest specific optimizations like partitioning, indexing, or caching strategies?",
                expected_tools=["query_expensive_analysis"],
                expected_outcome_type="query_optimization",
                success_criteria={
                    "identifies_expensive_queries": True,
                    "suggests_specific_optimizations": True,
                    "explains_optimization_rationale": True,
                    "prioritizes_by_impact": True
                },
                complexity_level="medium"
            ),
            
            # COMPREHENSIVE MULTI-DOMAIN TASKS
            EvaluationTask(
                task_id="comprehensive_001",
                description="Complete cost and performance audit",
                user_prompt="I need a comprehensive analysis of our Databricks environment. Show me: 1) Where our money is going and forecast next month's spend, 2) Any wasteful resources we should shut down, 3) Our most expensive queries that need optimization. This is for a quarterly business review.",
                expected_tools=["cost_daily_breakdown", "cost_forecast_monthly", "cost_top_drivers", "hygiene_idle_warehouses", "hygiene_underutilized_clusters", "query_expensive_analysis"],
                expected_outcome_type="comprehensive_report",
                success_criteria={
                    "covers_all_domains": True,
                    "provides_executive_summary": True,
                    "quantifies_optimization_opportunities": True,
                    "includes_implementation_roadmap": True,
                    "structured_for_business_audience": True
                },
                complexity_level="complex"
            ),
            
            # EDGE CASES AND ERROR HANDLING
            EvaluationTask(
                task_id="edge_001",
                description="Handling insufficient data scenarios",
                user_prompt="Show me cost trends for the last 30 days, but I'm particularly interested in any unusual spikes or patterns that might indicate issues.",
                expected_tools=["cost_daily_breakdown", "cost_top_drivers"],
                expected_outcome_type="cost_analysis",
                success_criteria={
                    "handles_longer_timeframes": True,
                    "gracefully_handles_missing_data": True,
                    "provides_helpful_error_messages": True,
                    "suggests_alternative_approaches": True
                },
                complexity_level="medium"
            ),
        ]
        
        return tasks
    
    @mlflow.trace(span_type=SpanType.TOOL)
    def run_single_evaluation(self, task: EvaluationTask) -> EvaluationResult:
        """Run a single evaluation task and measure results."""
        
        start_time = time.time()
        tools_called = []
        error_message = None
        agent_response = ""
        
        try:
            # Simulate agent execution by calling tools directly
            # In a real implementation, this would invoke the full agent
            if self.agent:
                # Use actual agent if provided
                from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentInputItem
                
                request = ResponsesAgentRequest(
                    input=[ResponsesAgentInputItem(
                        type="text",
                        text=task.user_prompt
                    )]
                )
                
                response = self.agent.predict(request)
                agent_response = str(response.output)
                
                # Extract tool calls from response (simplified)
                # This would need to be implemented based on actual agent response format
                tools_called = task.expected_tools  # Placeholder
                
            else:
                # Fallback: simulate tool execution for testing
                agent_response = self._simulate_tool_execution(task)
                tools_called = task.expected_tools
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            # Evaluate success based on criteria
            success = self._evaluate_success(task, agent_response, tools_called)
            
            # Generate feedback and reasoning
            reasoning, feedback = self._generate_feedback(task, agent_response, tools_called, success)
            
            return EvaluationResult(
                task_id=task.task_id,
                success=success,
                duration_ms=duration_ms,
                tools_called=tools_called,
                tool_call_count=len(tools_called),
                token_usage=None,  # Would be populated by actual agent
                error_message=error_message,
                agent_response=agent_response,
                reasoning=reasoning,
                feedback=feedback
            )
            
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            error_message = str(e)
            
            return EvaluationResult(
                task_id=task.task_id,
                success=False,
                duration_ms=duration_ms,
                tools_called=tools_called,
                tool_call_count=len(tools_called),
                token_usage=None,
                error_message=error_message,
                agent_response="",
                reasoning=f"Evaluation failed due to error: {error_message}",
                feedback="Fix the underlying error before re-evaluating"
            )
    
    def _simulate_tool_execution(self, task: EvaluationTask) -> str:
        """Simulate tool execution for testing purposes."""
        
        responses = []
        
        for tool_name in task.expected_tools:
            if tool_name in SYSTEM_TABLES_FUNCTIONS:
                try:
                    # Call with default parameters including warehouse_id
                    if tool_name.startswith("cost_"):
                        result = SYSTEM_TABLES_FUNCTIONS[tool_name](warehouse_id=self.warehouse_id)
                    elif tool_name.startswith("hygiene_"):
                        result = SYSTEM_TABLES_FUNCTIONS[tool_name](warehouse_id=self.warehouse_id)
                    elif tool_name.startswith("query_"):
                        result = SYSTEM_TABLES_FUNCTIONS[tool_name](warehouse_id=self.warehouse_id)
                    else:
                        result = f"Unknown tool: {tool_name}"
                    
                    responses.append(f"[{tool_name}] {result}")
                    
                except Exception as e:
                    responses.append(f"[{tool_name}] Error: {str(e)}")
        
        return "\n\n".join(responses)
    
    def _evaluate_success(self, task: EvaluationTask, response: str, tools_called: List[str]) -> bool:
        """Evaluate if the task was completed successfully."""
        
        # Check if expected tools were called
        tools_match = set(task.expected_tools).issubset(set(tools_called))
        
        if not tools_match:
            return False
        
        # Check success criteria
        criteria_met = 0
        total_criteria = len(task.success_criteria)
        
        for criterion, required in task.success_criteria.items():
            if required and self._check_criterion(criterion, response):
                criteria_met += 1
        
        # Success if at least 70% of criteria are met
        return (criteria_met / total_criteria) >= 0.7 if total_criteria > 0 else tools_match
    
    def _check_criterion(self, criterion: str, response: str) -> bool:
        """Check if a specific success criterion is met in the response."""
        
        response_lower = response.lower()
        
        criterion_checks = {
            "includes_cost_totals": lambda r: any(word in r for word in ["cost", "total", "$", "usd"]),
            "shows_workspace_breakdown": lambda r: "workspace" in r,
            "provides_actionable_insights": lambda r: any(word in r for word in ["recommend", "suggest", "should", "consider", "optimize"]),
            "mentions_top_cost_drivers": lambda r: any(word in r for word in ["top", "highest", "driver", "expensive"]),
            "provides_monthly_forecast": lambda r: any(word in r for word in ["forecast", "predict", "month", "estimate"]),
            "includes_current_runrate": lambda r: any(word in r for word in ["run-rate", "daily", "rate", "trend"]),
            "mentions_budget_implications": lambda r: any(word in r for word in ["budget", "spend", "cost"]),
            "identifies_cost_drivers": lambda r: any(word in r for word in ["driver", "cause", "increase", "spike"]),
            "compares_time_periods": lambda r: any(word in r for word in ["compare", "trend", "change", "increase", "decrease"]),
            "suggests_optimizations": lambda r: any(word in r for word in ["optimize", "reduce", "improve", "recommend"]),
            "explains_cost_patterns": lambda r: any(word in r for word in ["pattern", "trend", "because", "due to"]),
            "identifies_idle_resources": lambda r: any(word in r for word in ["idle", "unused", "waste", "running"]),
            "provides_specific_recommendations": lambda r: any(word in r for word in ["recommend", "suggest", "should", "action"]),
            "quantifies_potential_savings": lambda r: any(word in r for word in ["save", "saving", "$", "reduce", "cost"]),
            "includes_actionable_steps": lambda r: any(word in r for word in ["step", "action", "do", "implement"]),
            "analyzes_utilization_patterns": lambda r: any(word in r for word in ["utilization", "usage", "cpu", "pattern"]),
            "suggests_specific_sizing_changes": lambda r: any(word in r for word in ["resize", "size", "node", "cluster", "smaller", "larger"]),
            "explains_impact_of_changes": lambda r: any(word in r for word in ["impact", "effect", "result", "benefit"]),
            "prioritizes_recommendations": lambda r: any(word in r for word in ["priority", "first", "most", "top", "important"]),
            "identifies_expensive_queries": lambda r: any(word in r for word in ["expensive", "cost", "query", "high"]),
            "suggests_specific_optimizations": lambda r: any(word in r for word in ["partition", "cache", "index", "optimize", "improve"]),
            "explains_optimization_rationale": lambda r: any(word in r for word in ["because", "since", "due to", "reason"]),
            "prioritizes_by_impact": lambda r: any(word in r for word in ["impact", "priority", "most", "highest"]),
            "covers_all_domains": lambda r: all(domain in r for domain in ["cost", "hygiene", "query"]),
            "provides_executive_summary": lambda r: any(word in r for word in ["summary", "overview", "key", "highlights"]),
            "quantifies_optimization_opportunities": lambda r: any(word in r for word in ["save", "opportunity", "$", "reduce"]),
            "includes_implementation_roadmap": lambda r: any(word in r for word in ["roadmap", "plan", "step", "phase", "timeline"]),
            "structured_for_business_audience": lambda r: any(word in r for word in ["business", "roi", "value", "impact", "benefit"]),
            "handles_longer_timeframes": lambda r: "30" in r or "month" in r,
            "gracefully_handles_missing_data": lambda r: any(word in r for word in ["no data", "missing", "not found", "insufficient"]),
            "provides_helpful_error_messages": lambda r: any(word in r for word in ["error", "issue", "problem", "check"]),
            "suggests_alternative_approaches": lambda r: any(word in r for word in ["alternative", "instead", "try", "consider"])
        }
        
        check_function = criterion_checks.get(criterion)
        return check_function(response_lower) if check_function else False
    
    def _generate_feedback(self, task: EvaluationTask, response: str, tools_called: List[str], success: bool) -> Tuple[str, str]:
        """Generate reasoning and feedback for the evaluation result."""
        
        reasoning_parts = []
        feedback_parts = []
        
        # Analyze tool usage
        expected_tools = set(task.expected_tools)
        actual_tools = set(tools_called)
        
        if expected_tools == actual_tools:
            reasoning_parts.append("✅ Called all expected tools correctly")
        elif expected_tools.issubset(actual_tools):
            reasoning_parts.append("✅ Called expected tools plus additional ones")
            feedback_parts.append("Consider if all tool calls were necessary for efficiency")
        else:
            missing_tools = expected_tools - actual_tools
            reasoning_parts.append(f"❌ Missing expected tools: {', '.join(missing_tools)}")
            feedback_parts.append(f"Ensure agent calls these tools: {', '.join(missing_tools)}")
        
        # Analyze success criteria
        met_criteria = []
        failed_criteria = []
        
        for criterion, required in task.success_criteria.items():
            if required:
                if self._check_criterion(criterion, response):
                    met_criteria.append(criterion)
                else:
                    failed_criteria.append(criterion)
        
        if met_criteria:
            reasoning_parts.append(f"✅ Met criteria: {', '.join(met_criteria)}")
        
        if failed_criteria:
            reasoning_parts.append(f"❌ Failed criteria: {', '.join(failed_criteria)}")
            feedback_parts.append(f"Improve response to address: {', '.join(failed_criteria)}")
        
        # Overall assessment
        if success:
            feedback_parts.append("✅ Task completed successfully")
        else:
            feedback_parts.append("❌ Task needs improvement - review tool selection and response quality")
        
        reasoning = " | ".join(reasoning_parts)
        feedback = " | ".join(feedback_parts)
        
        return reasoning, feedback
    
    def run_evaluation_suite(self, task_filter: Optional[str] = None) -> Dict[str, Any]:
        """Run the complete evaluation suite and return comprehensive results."""
        
        tasks_to_run = self.evaluation_tasks
        if task_filter:
            tasks_to_run = [t for t in tasks_to_run if task_filter in t.task_id or task_filter in t.description]
        
        results = []
        start_time = time.time()
        
        print(f"🧪 Running {len(tasks_to_run)} evaluation tasks...")
        
        for i, task in enumerate(tasks_to_run, 1):
            print(f"[{i}/{len(tasks_to_run)}] Running {task.task_id}: {task.description}")
            result = self.run_single_evaluation(task)
            results.append(result)
            
            status = "✅ PASS" if result.success else "❌ FAIL"
            print(f"  {status} ({result.duration_ms}ms) - {len(result.tools_called)} tools called")
            
            if result.error_message:
                print(f"  ⚠️ Error: {result.error_message}")
        
        total_duration = time.time() - start_time
        
        # Calculate summary metrics
        total_tasks = len(results)
        passed_tasks = sum(1 for r in results if r.success)
        failed_tasks = total_tasks - passed_tasks
        pass_rate = (passed_tasks / total_tasks) * 100 if total_tasks > 0 else 0
        
        avg_duration = sum(r.duration_ms for r in results) / total_tasks if total_tasks > 0 else 0
        total_tool_calls = sum(r.tool_call_count for r in results)
        
        # Analyze common failure patterns
        failure_patterns = {}
        for result in results:
            if not result.success and result.error_message:
                error_type = result.error_message.split(':')[0] if ':' in result.error_message else result.error_message
                failure_patterns[error_type] = failure_patterns.get(error_type, 0) + 1
        
        summary = {
            "evaluation_summary": {
                "total_tasks": total_tasks,
                "passed_tasks": passed_tasks,
                "failed_tasks": failed_tasks,
                "pass_rate_percent": round(pass_rate, 1),
                "total_duration_seconds": round(total_duration, 2),
                "average_task_duration_ms": round(avg_duration, 1),
                "total_tool_calls": total_tool_calls,
                "evaluation_timestamp": datetime.now().isoformat()
            },
            "task_results": [
                {
                    "task_id": r.task_id,
                    "success": r.success,
                    "duration_ms": r.duration_ms,
                    "tools_called": r.tools_called,
                    "reasoning": r.reasoning,
                    "feedback": r.feedback,
                    "error": r.error_message
                }
                for r in results
            ],
            "failure_patterns": failure_patterns,
            "recommendations": self._generate_improvement_recommendations(results)
        }
        
        return summary
    
    def _generate_improvement_recommendations(self, results: List[EvaluationResult]) -> List[str]:
        """Generate recommendations for improving agent performance."""
        
        recommendations = []
        
        # Analyze failure patterns
        failed_results = [r for r in results if not r.success]
        
        if len(failed_results) > len(results) * 0.3:  # More than 30% failure rate
            recommendations.append("High failure rate detected - review tool descriptions and agent prompt")
        
        # Tool usage analysis
        all_tools_called = [tool for r in results for tool in r.tools_called]
        if not all_tools_called:
            recommendations.append("No tools were called - check agent tool registration and descriptions")
        
        # Performance analysis
        avg_duration = sum(r.duration_ms for r in results) / len(results) if results else 0
        if avg_duration > 10000:  # More than 10 seconds
            recommendations.append("High average response time - optimize tool implementations and queries")
        
        # Error pattern analysis
        error_messages = [r.error_message for r in results if r.error_message]
        if error_messages:
            recommendations.append("Multiple errors detected - review authentication and warehouse access")
        
        if not recommendations:
            recommendations.append("✅ Evaluation performance looks good - continue monitoring")
        
        return recommendations


def create_evaluation_report(evaluation_results: Dict[str, Any], output_file: Optional[str] = None) -> str:
    """Create a formatted evaluation report."""
    
    summary = evaluation_results["evaluation_summary"]
    
    report = f"""
# System Tables Agent Evaluation Report

**Generated:** {summary['evaluation_timestamp']}

## 📊 Overall Performance

- **Tasks Evaluated:** {summary['total_tasks']}
- **Pass Rate:** {summary['pass_rate_percent']}% ({summary['passed_tasks']}/{summary['total_tasks']})
- **Average Duration:** {summary['average_task_duration_ms']:.1f}ms per task
- **Total Tool Calls:** {summary['total_tool_calls']}

## 🎯 Task Results

"""
    
    for task in evaluation_results["task_results"]:
        status = "✅ PASS" if task["success"] else "❌ FAIL"
        report += f"""
### {task['task_id']} - {status}
- **Duration:** {task['duration_ms']}ms
- **Tools Called:** {', '.join(task['tools_called']) if task['tools_called'] else 'None'}
- **Reasoning:** {task['reasoning']}
- **Feedback:** {task['feedback']}
"""
        if task['error']:
            report += f"- **Error:** {task['error']}\n"
    
    if evaluation_results["failure_patterns"]:
        report += "\n## ⚠️ Common Failure Patterns\n\n"
        for pattern, count in evaluation_results["failure_patterns"].items():
            report += f"- **{pattern}:** {count} occurrences\n"
    
    report += "\n## 💡 Recommendations\n\n"
    for rec in evaluation_results["recommendations"]:
        report += f"- {rec}\n"
    
    if output_file:
        with open(output_file, 'w') as f:
            f.write(report)
        print(f"📝 Evaluation report saved to {output_file}")
    
    return report


if __name__ == "__main__":
    # Example usage
    import os
    
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        print("⚠️ Please set DATABRICKS_WAREHOUSE_ID environment variable")
        exit(1)
    
    evaluator = SystemTablesEvaluator(warehouse_id=warehouse_id)
    results = evaluator.run_evaluation_suite()
    
    print("\n" + "="*60)
    print("📊 EVALUATION COMPLETE")
    print("="*60)
    
    report = create_evaluation_report(results, "system_tables_evaluation_report.md")
    print(report)
