"""Framework tools package — each tool is a self-contained module.

To add a new tool:
  1. Create agentcore/tools/my_tool.py with DEFINITION and execute(tool_input, context)
  2. Import it here and add to _REGISTRY
"""

from agentcore.tools import datetime_tool, plan_tool, sql_tool

FRAMEWORK_TOOLS = [
    sql_tool.DEFINITION,
    datetime_tool.DEFINITION,
    {**plan_tool.DEFINITION, "cache_control": {"type": "ephemeral"}},
]

_REGISTRY = {
    "execute_sql":            sql_tool.execute,
    "get_current_datetime":   datetime_tool.execute,
    "get_action_plan":        plan_tool.execute,
}


def execute_tool(name: str, tool_input: dict, context: dict) -> str:
    """Dispatch a tool call by name. Returns the result as a string."""
    fn = _REGISTRY.get(name)
    if fn is None:
        return f"[Unknown tool: {name}]"
    return fn(tool_input, context)
