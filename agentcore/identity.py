"""Identity context and ownership mapping.

IdentityContext is established at the application boundary (CLI login,
Streamlit user picker, API key, etc.) — never by the LLM.  It flows
through the pipeline as an opaque value that the deterministic layers
use to scope every SQL statement.

OwnershipMap is built once at pipeline startup from the SchemaMap plus
a single ``identity_entity`` declaration in domain.json.  It auto-derives
which tables are scoped (have a direct FK to the user entity's table)
and what column to scope on.  Tables without a direct FK are unscoped
(reference data, lookup tables) and pass through untouched.
"""

from dataclasses import dataclass

from agentcore.sif.schema_map import SchemaMap


@dataclass
class IdentityContext:
    """Who the authenticated user is — established outside the LLM.

    Only carries the user's primary key.  Which ontology class the user
    belongs to is a domain-level fact (``DomainConfig.identity_entity``),
    not a session-level one — it's baked into the OwnershipMap at startup.
    """
    user_id: int | str


@dataclass
class TableOwnership:
    """How a single table is scoped to the authenticated user."""
    table_name: str
    scope_column: str    # FK column pointing at the user table, or PK for the user table itself


class OwnershipMap:
    """Derives per-table ownership from SchemaMap + identity_entity.

    Built once at pipeline startup.  Call ``get_scope(table_name)`` to
    check whether a table is scoped and learn which column enforces it.
    Returns None for unscoped tables.
    """

    def __init__(self, identity_entity: str, schema_map: SchemaMap) -> None:
        self.identity_entity = identity_entity
        self.scoped: dict[str, TableOwnership] = {}
        self._build(identity_entity, schema_map)

    def get_scope(self, table_name: str) -> TableOwnership | None:
        return self.scoped.get(table_name)

    def _build(self, identity_entity: str, schema_map: SchemaMap) -> None:
        user_table = schema_map.tables.get(identity_entity)
        if not user_table:
            raise ValueError(
                f"identity_entity '{identity_entity}' not found in SchemaMap. "
                f"Available entities: {', '.join(sorted(schema_map.tables))}"
            )

        # The user table itself is scoped by its PK.
        self.scoped[user_table.table_name] = TableOwnership(
            table_name=user_table.table_name,
            scope_column=user_table.primary_key,
        )

        # Find every other table that has a direct FK to the user table.
        for fk_table, fk_column in schema_map.fk_index.get(user_table.table_name, []):
            if fk_table == user_table.table_name:
                continue
            self.scoped[fk_table] = TableOwnership(
                table_name=fk_table,
                scope_column=fk_column,
            )
