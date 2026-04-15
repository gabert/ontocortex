"""LLM tool-schema builder.

Loads the base SIF JSON schema from sif_schema.json and injects
domain-specific enums for entities, relations, and actions. This runs
once per pipeline startup and the resulting tool definition is passed
to the Anthropic client so invalid names are prevented at generation
time rather than caught after execution.
"""

import copy
import json
from pathlib import Path

from agentcore.actions import get_registered_actions
from agentcore.sif.schema_map import SchemaMap

_SIF_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sif_schema.json"
_BASE_SIF_SCHEMA = json.loads(_SIF_SCHEMA_PATH.read_text(encoding="utf-8"))


def build_sif_tool(schema_map: SchemaMap) -> dict:
    """Build the submit_sif tool definition with domain-specific enums injected.

    The model cannot emit an entity, relation, or action name outside the
    lists derived from the current domain — bad names are prevented at
    generation time rather than caught after commit.
    """
    schema = copy.deepcopy(_BASE_SIF_SCHEMA)
    op_props = schema["properties"]["operations"]["items"]["properties"]

    class_names = sorted(schema_map.tables.keys())
    rel_names = sorted(schema_map.relations.keys())
    action_names = sorted(get_registered_actions().keys())

    if class_names:
        op_props["entity"]["enum"] = class_names
        rel_item_props = op_props["relations"]["items"]["properties"]
        rel_item_props["entity"]["enum"] = class_names
        # link/unlink endpoints are entity references — constrain them too
        op_props["from"]["properties"]["entity"]["enum"] = class_names
        op_props["to"]["properties"]["entity"]["enum"] = class_names

    if rel_names:
        op_props["relations"]["items"]["properties"]["rel"]["enum"] = rel_names
        # resolve is a dict keyed by relation name — constrain the keys
        op_props["resolve"]["propertyNames"] = {"enum": rel_names}
        # link/unlink relation reference
        op_props["relation"]["enum"] = rel_names

    if action_names:
        op_props["action"]["enum"] = action_names

    return {
        "name": "submit_sif",
        "description": (
            "Submit structured operations against the domain model. "
            "Use ontology class names for entities and ontology property names for fields. "
            "Call this whenever the user asks you to look up, create, update, or delete information."
        ),
        "input_schema": schema,
    }
