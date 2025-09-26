# System Tables Agent - Cost Observability & Performance Optimization

This branch implements a specialized Databricks agent for system tables analysis, focusing on cost observability, resource hygiene, and query efficiency optimization, following [Anthropic's methodology for building effective agent tools](https://www.anthropic.com/engineering/writing-tools-for-agents).

## 🚀 Features

### 🏢 Cost Observability
- **Daily Cost Breakdown**: Analyze spending by workspace, product, team, or custom tags
- **Monthly Forecasting**: Predict monthly costs based on current run-rate
- **Top Cost Drivers**: Identify highest-impact SKUs and services with trend analysis

### 🔧 Hygiene & Right-sizing  
- **Idle Warehouse Detection**: Find warehouses running without queries
- **Cluster Utilization**: Identify underutilized compute clusters
- **Auto-termination Gaps**: Detect missing termination policies

### ⚡ Query Efficiency
- **Expensive Query Analysis**: Find costly queries with optimization suggestions
- **Cache Performance**: Analyze cache hit ratios and recommendations
- **Partitioning Recommendations**: Suggest Z-order and partitioning strategies

## 🏗️ Architecture

Following Anthropic's best practices for agent tools:

- **Domain-based Namespacing**: `cost_`, `hygiene_`, `query_` prefixes
- **Meaningful Context**: Human-readable outputs over technical IDs
- **Token Efficiency**: Concise/detailed response formats with pagination
- **Natural Language**: Clear descriptions and actionable recommendations

## 🛠️ Implementation

### Core Components

1. **`system_tables_tools.py`** - Six specialized tools for system table analysis
2. **`system_tables_evaluation.py`** - Comprehensive evaluation framework
3. **`agent.py`** - Updated agent with system tables capabilities
4. **`test_system_tables_agent.py`** - Test suite for validation

### Tools Available

```python
# Cost Observability
cost_daily_breakdown(warehouse_id, days_back=7, group_by="workspace")
cost_forecast_monthly(warehouse_id, current_month_only=True) 
cost_top_drivers(warehouse_id, top_n=10, include_trends=True)

# Hygiene & Right-sizing
hygiene_idle_warehouses(warehouse_id, idle_hours_threshold=4)
hygiene_underutilized_clusters(warehouse_id, cpu_threshold=20.0)

# Query Efficiency  
query_expensive_analysis(warehouse_id, top_n=10, min_cost_threshold=1.0)
```

## 🧪 Testing & Evaluation

### Quick Test
```bash
cd responses-api-agent
source .venv/bin/activate
python test_system_tables_agent.py
```

### With Real Warehouse
```bash
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
python test_system_tables_agent.py
```

### Run Evaluation Suite
```python
from agent_server.system_tables_evaluation import SystemTablesEvaluator

evaluator = SystemTablesEvaluator(warehouse_id="your-warehouse-id")
results = evaluator.run_evaluation_suite()
```

## 🚀 Running the Agent

### Start Server
```bash
cd responses-api-agent
source .venv/bin/activate
export DATABRICKS_WAREHOUSE_ID="your-warehouse-id"
export MLFLOW_EXPERIMENT_ID="0"
python -m agent_server.agent
```

### Access UI
- **Web Interface**: http://localhost:8000
- **Health Check**: http://localhost:8000/health
- **API Endpoint**: POST http://localhost:8000/invocations

## 💬 Example Conversations

### Cost Analysis
```
User: "Show me our daily cost breakdown for the last week by workspace"
Agent: Calls cost_daily_breakdown() and provides insights like:
📊 Cost Breakdown (7 days by workspace)
💰 Total Cost: $1,234.56
🔥 Top Cost Drivers:
1. Production Workspace: $567.89 (46%)
2. Analytics Workspace: $234.56 (19%)
...
```

### Hygiene Check
```
User: "Find any wasteful resources we should shut down"
Agent: Calls hygiene_idle_warehouses() and hygiene_underutilized_clusters():
⚠️ Idle Warehouse Analysis (7 days)
🚨 Found 2 potentially idle warehouses
🏭 Warehouse: test-warehouse-123
   📅 Last Query: Never
   💡 Action: Consider stopping or deleting if unused
...
```

### Query Optimization
```
User: "What are our most expensive queries and how can we optimize them?"
Agent: Calls query_expensive_analysis():
💸 Expensive Query Analysis (7 days)
🔍 Top query abc123... ($45.67)
   💡 Suggestions: Enable result caching, Add partition filters
...
```

## 🎯 Evaluation Results

The evaluation framework tests 8 comprehensive scenarios:

- **Simple Tasks**: Single-domain analysis (cost, hygiene, queries)
- **Medium Tasks**: Multi-tool workflows with business context
- **Complex Tasks**: Comprehensive audits across all domains
- **Edge Cases**: Error handling and missing data scenarios

Success criteria include:
- ✅ Correct tool selection and execution
- ✅ Actionable insights and recommendations  
- ✅ Business-friendly explanations
- ✅ Quantified optimization opportunities

## 🔧 Development Notes

### Following Anthropic's Methodology

1. **Evaluation-Driven Development**: Comprehensive test scenarios first
2. **Tool Design Principles**: Clear boundaries, meaningful context
3. **Iterative Improvement**: Use Claude to optimize tool descriptions
4. **Real-world Grounding**: Based on actual system tables and use cases

### System Tables Used

- `system.billing.usage` - Cost and usage data
- `system.billing.list_prices` - Pricing information
- `system.query.history` - Query performance metrics
- `system.compute.warehouse_events` - Warehouse lifecycle events
- `system.compute.node_timeline` - Cluster utilization data

### Authentication

Uses Databricks SDK `WorkspaceClient()` with automatic authentication:
- Environment variables (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`)
- Databricks CLI profiles
- Databricks Apps context (when deployed)

## 📈 Next Steps

1. **Deploy to Databricks Apps** using `app.yaml`
2. **Add More System Tables** (audit logs, lineage, etc.)
3. **Enhance Visualizations** with charts and dashboards
4. **Add Alerting** for cost spikes and resource waste
5. **Integration with Budgets** and approval workflows

## 🤝 Contributing

When adding new tools:

1. Follow the naming convention (`domain_specific_action`)
2. Add comprehensive docstrings and parameter validation
3. Include both concise and detailed response formats
4. Add evaluation tasks to test the new functionality
5. Update the agent's system prompt with new capabilities

---

Built with ❤️ following Anthropic's best practices for agent tool development.
