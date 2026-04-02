"""Tool: execute_sql — run a PostgreSQL query against the domain database."""

import json
import re

from agentcore.database import execute_query

DEFINITION = {
    "name": "execute_sql",
    "description": (
        "Execute a SQL query against the domain database. "
        "ALL literal values MUST use :name named placeholders — never embed values directly in SQL. "
        "Example: SELECT * FROM clients WHERE email = :email  "
        "with params: {\"email\": \"john@example.com\"}. "
        "Always use RETURNING * on INSERT and UPDATE."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": (
                    "SQL query using :name placeholders for every value. "
                    "Never embed string or numeric literals directly."
                ),
            },
            "params": {
                "type": "object",
                "description": (
                    "Named parameter values keyed by placeholder name. "
                    "Example: {\"email\": \"john@example.com\", \"age\": 30}. "
                    "Empty object {} if the query has no placeholders."
                ),
            },
            "write": {
                "type": "boolean",
                "description": "True for INSERT / UPDATE / DELETE. False for SELECT.",
            },
        },
        "required": ["query", "params", "write"],
    },
}

_WRITE_RE         = re.compile(r"\b(INSERT|UPDATE|DELETE)\b", re.IGNORECASE)
_DDL_RE           = re.compile(r"\b(DROP|TRUNCATE|ALTER|CREATE)\b", re.IGNORECASE)
_STRING_LITERAL   = re.compile(r"'[^']*'")           # single-quoted string → embedded value
_NUMERIC_LITERAL  = re.compile(                      # bare numeric literals in comparison positions
    r"(?:(?<![<>!])=|!=|<>|(?<![<>])>(?!=)|(?<![<>!])<(?!=))\s*-?\d+(?:\.\d+)?\b"
)
_NAMED_PH         = re.compile(r"(?<!:):(\w+)")      # :name placeholders (negative lookbehind avoids ::cast)
_INSERT_TABLE_RE  = re.compile(r"\bINSERT\s+INTO\s+(\w+)", re.IGNORECASE)
_INSERT_COLS_RE   = re.compile(r"\bINSERT\s+INTO\s+\w+\s*\(([^)]+)\)", re.IGNORECASE)


def _is_write_query(query: str) -> bool:
    """Detect write operations server-side, ignoring Claude's self-reported flag."""
    return bool(_WRITE_RE.search(query))


def _reject_non_parameterized(query: str, params: dict) -> dict | None:
    """Return an error dict if the query contains embedded literals,
    or if the named placeholders do not match the params dict keys."""
    if _STRING_LITERAL.search(query):
        return {
            "error":  "non_parameterized_sql",
            "detail": "Embedded string literals are not allowed. Use :name placeholders.",
        }
    if _NUMERIC_LITERAL.search(query):
        return {
            "error":  "non_parameterized_sql",
            "detail": "Embedded numeric literals in comparisons are not allowed. Use :name placeholders.",
        }
    in_query = set(_NAMED_PH.findall(query))
    in_params = set(params.keys())
    missing = in_query - in_params
    if missing:
        return {
            "error":  "params_mismatch",
            "detail": f"Placeholders missing from params: {sorted(missing)}",
        }
    extra = in_params - in_query
    if extra:
        return {
            "error":  "params_mismatch",
            "detail": f"Params provided but not used in query: {sorted(extra)}",
        }
    return None


def execute(tool_input: dict, context: dict) -> str:
    query    = tool_input.get("query", "")
    params   = tool_input.get("params", {})
    is_write = _is_write_query(query)
    label    = "WRITE" if is_write else "READ"
    verbose  = context.get("verbose", False)

    # Reject DDL — schema changes are not permitted at runtime.
    if _DDL_RE.search(query):
        rejection = {
            "error":  "ddl_not_allowed",
            "detail": "DDL statements (DROP, TRUNCATE, ALTER, CREATE) are not permitted at runtime.",
        }
        if verbose:
            print(f"    [REJECTED DDL] {rejection['detail']}")
        context["query_log"].append({
            "type": "sql", "query": query, "is_write": False, "results": rejection,
        })
        return json.dumps(rejection, default=str)

    if verbose:
        short = " ".join(query.split())
        print(f"    [DB {label}] {short[:120]}{'...' if len(short) > 120 else ''}")

    # Enforce parameterized queries
    rejection = _reject_non_parameterized(query, params)
    if rejection:
        if verbose:
            print(f"    [REJECTED] {rejection['detail']}")
        context["query_log"].append({
            "type": "sql", "query": query, "is_write": is_write, "results": rejection,
        })
        return json.dumps(rejection, default=str)

    # Pre-flight validation for INSERT
    if is_write and "INSERT" in query.upper():
        errors = _preflight(query, context)
        if errors:
            if verbose:
                for e in errors:
                    print(f"    [VALIDATION] {e}")
            context["query_log"].append({
                "type": "sql", "query": query, "is_write": True, "results": errors,
            })
            return json.dumps(errors, default=str)

    result = execute_query(context["db_config"], query, is_write, params=params or None)

    if verbose:
        if isinstance(result, list):
            print(f"    [DB] {len(result)} row(s) returned")
        elif isinstance(result, dict) and "error" in result:
            print(f"    [DB ERROR] {result['error']}")
        elif isinstance(result, dict) and result.get("success"):
            print(f"    [DB] {result.get('rows_affected', 0)} row(s) affected")

    context["query_log"].append({
        "type": "sql", "query": query, "is_write": is_write, "results": result,
    })
    return json.dumps(result, default=str)


# ── Pre-flight validation ──────────────────────────────────────────────────────

def _preflight(sql: str, context: dict) -> list[dict]:
    """Check that all required fields are present in an INSERT statement.

    Unique constraint violations are intentionally left to the DB — the database
    already returns a structured UniqueViolation error (see database.py).
    """
    table_m = _INSERT_TABLE_RE.search(sql)
    if not table_m:
        return []

    table_name      = table_m.group(1).lower()
    validation_spec = context.get("validation_spec", {})
    tv              = validation_spec.get(table_name)
    if tv is None:
        return []

    cols_m = _INSERT_COLS_RE.search(sql)
    if not cols_m:
        # INSERT without explicit column list (INSERT...SELECT or INSERT...VALUES without cols)
        # — DB NOT NULL constraints are the safety net.
        return []

    cols    = [c.strip().lower() for c in cols_m.group(1).split(",")]
    missing = [f for f in tv["required_fields"] if f not in cols]
    if missing:
        return [{
            "error":  "validation",
            "check":  "required_missing",
            "table":  table_name,
            "fields": missing,
        }]

    return []
