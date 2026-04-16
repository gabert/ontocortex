"""SIF: Structured Intent Format — backend-neutral core.

This package holds the parts of SIF that don't depend on any specific
backend: the ontology↔physical mapping (SchemaMap), the validation
layer, the LLM tool-schema builder, and the shared types
(TranslationError, LinkPlan).

Backend-specific translators + executors live in sibling packages like
`agentcore.sif_sql`. A future `agentcore.sif_graphql` would implement
the same contract: translate a validated SIF op into a backend plan,
then execute it against a connection.
"""

from agentcore.sif.mapping import build_schema_map_from_mapping, generate_mapping_from_schema
from agentcore.sif.schema_map import JoinStep, RelationMap, SchemaMap, TableMap
from agentcore.sif.tool import build_sif_tool
from agentcore.sif.types import LinkPlan, TranslationError
from agentcore.sif.validation import validate_operations

__all__ = [
    "JoinStep",
    "LinkPlan",
    "RelationMap",
    "SchemaMap",
    "TableMap",
    "TranslationError",
    "build_schema_map_from_mapping",
    "build_sif_tool",
    "generate_mapping_from_schema",
    "validate_operations",
]
