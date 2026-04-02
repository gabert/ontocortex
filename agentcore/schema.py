"""Deterministic transformation: logical schema (JSON) → database tables.

Reads a database-agnostic schema.json (produced by the architect module)
and uses SQLAlchemy to create concrete tables for any supported backend.
Also handles loading seed data SQL.

No LLM calls — pure mapping logic.
"""

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    event,
    func,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# ── Logical type → SQLAlchemy type factories ──────────────────────────────────
# Factories (not instances) because each Column needs its own type object.

_TYPE_FACTORIES = {
    "string":   lambda: String(255),
    "text":     lambda: Text(),
    "integer":  lambda: Integer(),
    "decimal":  lambda: Numeric(12, 2),
    "boolean":  lambda: Boolean(),
    "date":     lambda: Date(),
    "datetime": lambda: DateTime(),
}


def _format_server_default(value, logical_type: str) -> str:
    """Format a JSON default value as a SQL literal for DDL."""
    if logical_type == "boolean":
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


# ── MetaData builder ──────────────────────────────────────────────────────────

def build_metadata(schema: dict) -> MetaData:
    """Build a SQLAlchemy MetaData from a logical schema dict.

    Tables are created in creation_order so that FK references resolve.
    """
    metadata = MetaData()
    by_name = {t["name"]: t for t in schema["tables"]}

    for table_name in schema["creation_order"]:
        table_def = by_name[table_name]

        # Primary key
        columns: list = [
            Column(
                table_def["primary_key"],
                Integer,
                primary_key=True,
                autoincrement=True,
            ),
        ]

        # Data columns
        for col in table_def.get("columns", []):
            sa_type = _TYPE_FACTORIES[col["type"]]()
            kwargs: dict = {}
            if col.get("not_null"):
                kwargs["nullable"] = False
            if col.get("unique"):
                kwargs["unique"] = True
            if "default" in col:
                kwargs["server_default"] = text(
                    _format_server_default(col["default"], col["type"])
                )
            columns.append(Column(col["name"], sa_type, **kwargs))

        # Foreign key columns
        for fk in table_def.get("foreign_keys", []):
            ref = f"{fk['references_table']}.{fk['references_column']}"
            columns.append(
                Column(
                    fk["column"],
                    Integer,
                    ForeignKey(ref),
                    nullable=not fk.get("not_null", False),
                )
            )

        # Automatic timestamp
        columns.append(Column("created_at", DateTime(), server_default=func.now()))

        # CHECK constraints from allowed_values
        constraints: list = []
        for col in table_def.get("columns", []):
            if "allowed_values" in col:
                values = ", ".join(f"'{v}'" for v in col["allowed_values"])
                constraints.append(
                    CheckConstraint(
                        f"{col['name']} IN ({values})",
                        name=f"ck_{table_name}_{col['name']}",
                    )
                )

        Table(table_name, metadata, *columns, *constraints)

    return metadata


# ── Public API ────────────────────────────────────────────────────────────────

def render_ddl(schema: dict) -> str:
    """Render the logical schema as PostgreSQL DDL (for inspection only).

    Uses SQLAlchemy's DDL compiler — no database connection required.
    """
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateTable

    metadata = build_metadata(schema)
    dialect = postgresql.dialect()
    blocks: list[str] = []
    for table_name in schema["creation_order"]:
        table = metadata.tables[table_name]
        blocks.append(str(CreateTable(table).compile(dialect=dialect)).strip() + ";")
    return "\n\n".join(blocks)


def create_tables(engine: Engine, schema: dict) -> list[str]:
    """Create all tables from a logical schema. Returns created table names."""
    metadata = build_metadata(schema)
    metadata.create_all(engine)
    return list(schema["creation_order"])


def drop_tables(engine: Engine, schema: dict) -> None:
    """Drop all tables defined in the schema (reverse creation order)."""
    metadata = build_metadata(schema)
    metadata.drop_all(engine)


def load_seed_sql(engine: Engine, seed_sql: str) -> int:
    """Execute seed SQL statements. Returns number of INSERTs executed."""
    statements = [
        chunk.strip()
        for chunk in seed_sql.split(";")
        if chunk.strip()
        and not all(
            line.strip().startswith("--") or not line.strip()
            for line in chunk.strip().splitlines()
        )
    ]

    with engine.connect() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
        conn.commit()

    return sum(1 for s in statements if "INSERT" in s.upper())


# ── Seed data validator ───────────────────────────────────────────────────────

def validate_seed_data(schema: dict, seed_sql: str) -> list[str]:
    """Validate seed SQL against the schema using an in-memory SQLite database.

    Creates all tables (via build_metadata), enables FK enforcement, then
    executes every INSERT statement.  Returns a list of error messages;
    an empty list means everything passed.

    SQLite differences from PostgreSQL that don't affect INSERT validation:
    - SERIAL → INTEGER PRIMARY KEY AUTOINCREMENT (auto-assigned, not in INSERTs)
    - created_at server default → NULL in SQLite (column is nullable)
    - String/Numeric/Boolean types all map correctly
    """
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _set_pragmas(conn, _):
        conn.execute("PRAGMA foreign_keys = ON")

    build_metadata(schema).create_all(engine)

    statements = [
        chunk.strip()
        for chunk in seed_sql.split(";")
        if chunk.strip()
        and not all(
            line.strip().startswith("--") or not line.strip()
            for line in chunk.strip().splitlines()
        )
    ]

    errors: list[str] = []
    with engine.connect() as conn:
        for i, stmt in enumerate(statements, 1):
            try:
                conn.execute(text(stmt))
            except SQLAlchemyError as exc:
                errors.append(f"Statement {i}: {exc.args[0].splitlines()[0]}\n  SQL: {stmt[:120]}")
        if not errors:
            conn.commit()

    return errors


# ── Schema description for agent system prompt ───────────────────────────────

def build_schema_description(schema: dict) -> str:
    """Render a human-readable schema summary from the logical schema JSON.

    Format matches what the business-logic agent expects::

        Table: customers  (PK: customer_id)
          Columns : first_name [REQUIRED], last_name [REQUIRED], ...
          FK      : (none)
    """
    by_name = {t["name"]: t for t in schema["tables"]}
    lines: list[str] = []

    for table_name in schema["creation_order"]:
        t = by_name[table_name]
        lines.append(f"Table: {t['name']}  (PK: {t['primary_key']})")

        # Data columns
        col_parts: list[str] = []
        for c in t.get("columns", []):
            tags = []
            if c.get("required"):
                tags.append("REQUIRED")
            if c.get("unique"):
                tags.append("UNIQUE")
            if c.get("allowed_values"):
                tags.append("VALUES: " + "|".join(c["allowed_values"]))
            col_parts.append(f"{c['name']} [{', '.join(tags)}]" if tags else c["name"])
        col_parts.append("created_at")
        lines.append(f"  Columns : {', '.join(col_parts)}")

        # Foreign keys
        fks = t.get("foreign_keys", [])
        if fks:
            first = True
            for fk in fks:
                tags = []
                if fk.get("required"):
                    tags.append("REQUIRED")
                suffix = f" [{', '.join(tags)}]" if tags else ""
                prefix = "  FK      : " if first else "            "
                lines.append(f"{prefix}{fk['column']} -> {fk['references_table']}{suffix}")
                first = False
        lines.append("")

    return "\n".join(lines).strip()


def build_validation_spec(schema: dict) -> tuple[dict, str]:
    """Build per-table validation rules from the logical schema.

    Returns:
        spec — dict[table_name → dict] with required_fields, unique_fields, fk_dependencies
        text — human-readable block for injection into the agent system prompt
    """
    by_name = {t["name"]: t for t in schema["tables"]}
    spec: dict[str, dict] = {}

    for table_name in schema["creation_order"]:
        t = by_name[table_name]
        required = [c["name"] for c in t.get("columns", []) if c.get("required")]
        required += [fk["column"] for fk in t.get("foreign_keys", []) if fk.get("required")]
        unique = [c["name"] for c in t.get("columns", []) if c.get("unique")]
        fk_deps = [
            (fk["column"], fk["references_table"])
            for fk in t.get("foreign_keys", [])
        ]

        if required or unique or fk_deps:
            spec[table_name] = {
                "required_fields": required,
                "unique_fields": unique,
                "fk_dependencies": fk_deps,
            }

    # Render text
    lines: list[str] = []
    for table_name in schema["creation_order"]:
        tv = spec.get(table_name)
        if tv is None:
            continue
        lines.append(f"{table_name} — before INSERT:")
        if tv["required_fields"]:
            lines.append(f"  Must collect from user : {', '.join(tv['required_fields'])}")
        for field_name in tv["unique_fields"]:
            lines.append(f"  Check unique           : SELECT 1 FROM {table_name} WHERE {field_name} = ?")
        if tv["fk_dependencies"]:
            fk_parts = [f"{col} -> {ref}" for col, ref in tv["fk_dependencies"]]
            lines.append(f"  FK must exist          : {', '.join(fk_parts)}")
        lines.append("")

    return spec, "\n".join(lines).strip()
