"""Shared SIF types — TranslationError and LinkPlan.

These are used by every backend: the error type is raised when a SIF
op can't be mapped to the backend, and LinkPlan is the structured
description of a link/unlink op that an executor walks to resolve
endpoints and insert/delete the edge. Backends can either consume a
LinkPlan directly or emit their own plan types from their translator.
"""

from dataclasses import dataclass

from agentcore.sif.schema_map import TableMap


class TranslationError(Exception):
    """Raised when a SIF operation cannot be translated for a backend."""


@dataclass
class LinkPlan:
    """Structured plan for a link/unlink op.

    Link/unlink are not pure translations — they need runtime lookups
    to resolve both endpoints to primary keys, a presence check on the
    junction, and finally an insert or delete. The translator produces
    this plan; the executor walks it against an open connection.
    """
    op: str                  # "link" or "unlink"
    relation_name: str
    from_table: TableMap
    from_filters: dict
    to_table: TableMap
    to_filters: dict
    junction_table: str
    from_fk_column: str      # FK column in the junction pointing at from_table
    to_fk_column: str        # FK column in the junction pointing at to_table
