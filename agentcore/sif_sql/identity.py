"""Post-translation identity injection.

Takes a translated SQLStatement and injects ownership scoping based on
the authenticated user.  The SIF translator is completely unaware of
identity — this module post-processes its output using the predictable
SQL patterns the translator generates.

When a table is unscoped (no direct FK to the user entity), the
statement passes through unchanged.
"""

import re
from dataclasses import replace

from agentcore.identity import IdentityContext, OwnershipMap
from agentcore.sif import LinkPlan
from agentcore.sif_sql.translator import SQLStatement

# Reserved param name — won't collide with translator's p1, p2, ... scheme.
_IDENTITY_PARAM = "_identity_id"


def apply_identity(
    stmt: SQLStatement,
    identity: IdentityContext,
    ownership: OwnershipMap,
) -> SQLStatement:
    """Return a new SQLStatement with identity scoping, or the original if unscoped."""
    scope = ownership.get_scope(stmt.table_name)
    if scope is None:
        return stmt

    col = scope.scope_column
    table = stmt.table_name

    if stmt.op_type == "query":
        sql = _inject_query(stmt.sql, table, col)
    elif stmt.op_type == "create":
        sql = _inject_create(stmt.sql, col)
    elif stmt.op_type in ("update", "delete"):
        sql = _inject_where(stmt.sql, col)
    else:
        return stmt

    params = {**stmt.params, _IDENTITY_PARAM: identity.user_id}
    return replace(stmt, sql=sql, params=params)


def scope_link_plan(
    plan: LinkPlan,
    identity: IdentityContext,
    ownership: OwnershipMap,
) -> LinkPlan:
    """Scope a LinkPlan's endpoint filters so lookups only find the user's rows."""
    from_scope = ownership.get_scope(plan.from_table.table_name)
    to_scope = ownership.get_scope(plan.to_table.table_name)

    new_from = dict(plan.from_filters)
    new_to = dict(plan.to_filters)

    if from_scope:
        new_from[from_scope.scope_column] = identity.user_id
    if to_scope:
        new_to[to_scope.scope_column] = identity.user_id

    return LinkPlan(
        op=plan.op,
        relation_name=plan.relation_name,
        from_table=plan.from_table,
        from_filters=new_from,
        to_table=plan.to_table,
        to_filters=new_to,
        junction_table=plan.junction_table,
        from_fk_column=plan.from_fk_column,
        to_fk_column=plan.to_fk_column,
    )


# ── SQL injection helpers ────────────────────────────────────────────────────
#
# These work on the translator's known SQL patterns.  The translator
# generates SQL in a predictable format — these helpers rely on that.

def _inject_query(sql: str, table: str, col: str) -> str:
    """Add AND scope to a SELECT statement.

    Handles:
      - WHERE ... ORDER BY / LIMIT / end
      - No WHERE clause (FROM ... ORDER BY / LIMIT / end)
    """
    clause = f"{table}.{col} = :{_IDENTITY_PARAM}"

    # If there's a WHERE clause, add AND before ORDER BY / LIMIT / end.
    if " WHERE " in sql:
        return _insert_and_clause(sql, clause)

    # No WHERE — insert WHERE before ORDER BY / LIMIT / end of string.
    insertion = f" WHERE {clause}"
    for keyword in (" ORDER BY ", " LIMIT "):
        pos = sql.find(keyword)
        if pos != -1:
            return sql[:pos] + insertion + sql[pos:]
    return sql + insertion


def _inject_create(sql: str, col: str) -> str:
    """Add scope column + param to an INSERT statement.

    Pattern: INSERT INTO table (col1, col2) VALUES (:p1, :p2) RETURNING *

    If `resolve` already included the scope column (e.g. via a subselect),
    replace its value with the identity parameter so the user can only
    create records for themselves.
    """
    # Check if the scope column already exists in the INSERT column list.
    # The column list sits between "INSERT INTO <table> (" and ") VALUES (".
    col_list_match = re.search(r"INSERT INTO \S+ \((.+?)\) VALUES \((.+?)\) RETURNING", sql, re.DOTALL)
    if not col_list_match:
        return sql

    col_list = [c.strip() for c in col_list_match.group(1).split(",")]
    val_list = _split_values(col_list_match.group(2))

    if col in col_list:
        # Replace the existing value with the identity param.
        idx = col_list.index(col)
        val_list[idx] = f":{_IDENTITY_PARAM}"
    else:
        # Append the scope column and identity param.
        col_list.append(col)
        val_list.append(f":{_IDENTITY_PARAM}")

    new_cols = ", ".join(col_list)
    new_vals = ", ".join(val_list)
    prefix = sql[:col_list_match.start()]
    suffix = sql[col_list_match.end():]
    table_match = re.search(r"INSERT INTO (\S+)", sql)
    table_name = table_match.group(1)
    return f"{prefix}INSERT INTO {table_name} ({new_cols}) VALUES ({new_vals}) RETURNING{suffix}"


def _split_values(values_str: str) -> list[str]:
    """Split the VALUES list, respecting parenthesised subselects."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in values_str:
        if ch == "(" :
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return parts


def _inject_where(sql: str, col: str) -> str:
    """Add AND scope to an UPDATE or DELETE statement.

    Handles:
      - WHERE ... RETURNING
      - No WHERE (SET ... RETURNING or DELETE FROM table RETURNING)
    """
    clause = f"{col} = :{_IDENTITY_PARAM}"

    if " WHERE " in sql:
        return _insert_and_clause(sql, clause)

    # No WHERE — insert WHERE before RETURNING.
    insertion = f" WHERE {clause}"
    pos = sql.find(" RETURNING")
    if pos != -1:
        return sql[:pos] + insertion + sql[pos:]
    return sql + insertion


def _insert_and_clause(sql: str, clause: str) -> str:
    """Insert AND clause into existing WHERE before ORDER BY / LIMIT / RETURNING / end."""
    for keyword in (" ORDER BY ", " LIMIT ", " RETURNING"):
        pos = sql.find(keyword)
        if pos != -1:
            return sql[:pos] + f" AND {clause}" + sql[pos:]
    return sql + f" AND {clause}"
