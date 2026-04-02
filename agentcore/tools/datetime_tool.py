"""Tool: get_current_datetime — returns the current date and time."""

from datetime import datetime

DEFINITION = {
    "name": "get_current_datetime",
    "description": (
        "Returns the current date and time. Use this whenever you need to know "
        "today's date, the current time, or need to calculate relative dates "
        "(e.g. 'next Monday', 'in 3 days', 'yesterday')."
    ),
    "input_schema": {"type": "object", "properties": {}, "required": []},
}


def execute(tool_input: dict, context: dict) -> str:
    result = datetime.now().strftime("%Y-%m-%d %H:%M:%S (%A)")

    if context.get("verbose"):
        print(f"    [TOOL] get_current_datetime -> {result}")

    context["query_log"].append({
        "type": "tool",
        "name": "get_current_datetime",
        "result": result,
    })

    return result
