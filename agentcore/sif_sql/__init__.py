"""SIF SQL backend: translator + SQLAlchemy executor.

Turns backend-neutral SIF ops (validated against a SchemaMap by
`agentcore.sif.validation`) into parameterized SQL statements or
LinkPlans, and runs them atomically against a SQLAlchemy engine.

This is one implementation of the SIF contract; a future backend
(GraphQL, REST) would live in a sibling package and expose the same
two entry points: a translator that produces a plan from an op dict
and an executor that runs a list of ops against a connection.
"""

from agentcore.sif_sql.executor import execute_sif
from agentcore.sif_sql.translator import (
    SQLStatement,
    translate,
    translate_all,
)

__all__ = [
    "SQLStatement",
    "execute_sif",
    "translate",
    "translate_all",
]
