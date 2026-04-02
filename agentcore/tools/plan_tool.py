"""Tool: get_action_plan — returns what the agent must collect before inserting a record."""

import json

DEFINITION = {
    "name": "get_action_plan",
    "description": (
        "Before inserting a new record, call this to get the exact list of fields "
        "you must collect from the user. Returns required fields, unique fields, "
        "and FK dependencies for the target table. "
        "Always call this before starting to collect information for an INSERT."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "table": {
                "type": "string",
                "description": "The database table name you plan to INSERT into.",
            },
        },
        "required": ["table"],
    },
}


def execute(tool_input: dict, context: dict) -> str:
    table           = tool_input.get("table", "").lower()
    validation_spec = context.get("validation_spec", {})
    tv              = validation_spec.get(table)

    if tv is None:
        result = {"error": f"Unknown table '{table}'. Check the schema for valid table names."}
    else:
        result = {
            "table": table,
            "must_collect_from_user": tv["required_fields"],
            "must_be_unique":         tv["unique_fields"],
            "fk_dependencies":        [
                {"column": col, "references": ref}
                for col, ref in tv["fk_dependencies"]
            ],
        }

    if context.get("verbose"):
        print(f"    [TOOL] get_action_plan({table}) -> {result}")

    context["query_log"].append({
        "type":   "tool",
        "name":   "get_action_plan",
        "result": json.dumps(result),
    })

    return json.dumps(result)
