"""SQL executor — transactional runtime for translated SIF operations.

The translator (`agentcore.sif_sql.translator`) turns SIF op dicts into
pure SQLStatement or LinkPlan values. This module does everything else:

  - opens a single DB transaction per SIF batch
  - runs each statement/plan against the connection
  - dispatches `action` ops to the registered Python functions
  - rolls the whole batch back on any failure
  - formats successes and failures as text the LLM can react to

The split keeps the translator pure and unit-testable without a
database, while everything that touches connections, transactions, or
agent-facing text lives here.
"""

import json

from agentcore.actions import get_registered_actions
from agentcore.database import execute_on_conn, open_transaction
from agentcore.identity import IdentityContext, OwnershipMap
from agentcore.sif import LinkPlan, SchemaMap, TableMap, TranslationError
from agentcore.sif.validation import validate_operations
from agentcore.sif_sql.identity import apply_identity, scope_link_plan
from agentcore.sif_sql.translator import SQLStatement, translate


# ── Main entry point ────────────────────────────────────────────────────────


def execute_sif(
    operations: list[dict],
    schema_map: SchemaMap,
    db_config,
    verbose: bool = False,
    identity: IdentityContext | None = None,
    ownership: OwnershipMap | None = None,
) -> tuple[str, list[dict]]:
    """Translate and execute SIF operations atomically. Returns (result_text, query_log).

    All operations in a single call run inside one DB transaction: either
    every write commits or none do. On the first failing op the transaction
    is rolled back, so prior successes within the batch are reverted — the
    agent sees a clean 'nothing applied' state and can retry without dealing
    with half-written rows.

    Always returns a string the LLM can react to. Validation failures,
    translation errors, unknown actions, and DB errors are formatted into
    the text rather than raised, so the tool-use loop keeps running and the
    agent can self-correct. Genuine bugs in our own code still propagate.
    """
    query_log: list[dict] = []
    prior: list[dict] = []

    validation_errors = validate_operations(operations, schema_map)
    if validation_errors:
        return _format_validation_failure(validation_errors), query_log

    with open_transaction(db_config) as (conn, tx):
        for idx, op in enumerate(operations, start=1):
            op_type = op.get("op")

            if op_type == "action":
                failure = _run_action(op, schema_map, db_config, query_log, prior, verbose)
            elif op_type in ("link", "unlink"):
                failure = _run_link(op, schema_map, identity, ownership, conn, query_log, prior, verbose)
            else:
                failure = _run_crud(op, schema_map, identity, ownership, conn, query_log, prior, verbose)

            if failure is not None:
                return _format_with_failure(prior, idx, failure), query_log

        tx.commit()

    return _format_results(prior), query_log


# ── Op runners ──────────────────────────────────────────────────────────────
#
# Each returns None on success (mutating query_log and prior in place)
# or an error string on failure (triggering rollback in the caller).


def _run_action(
    op: dict, schema_map: SchemaMap, db_config,
    query_log: list[dict], prior: list[dict], verbose: bool,
) -> str | None:
    """Dispatch an action op to its registered Python function."""
    action_name = op.get("action", "")
    registry = get_registered_actions()
    action_entry = registry.get(action_name)
    if not action_entry:
        return f"Unknown action: {action_name}"

    if verbose:
        print(f"    [SIF ACTION] {action_name}")

    try:
        result = action_entry["fn"](op.get("params", {}), db_config, schema_map)
    except Exception as e:
        return f"Action error ({action_name}): {e}"

    query_log.append({"type": "action", "action": action_name,
                       "params": op.get("params", {}), "result": result})
    prior.append({"operation": "action", "entity": action_name, "result": result})
    return None


def _run_link(
    op: dict, schema_map: SchemaMap,
    identity: IdentityContext | None, ownership: OwnershipMap | None,
    conn, query_log: list[dict], prior: list[dict], verbose: bool,
) -> str | None:
    """Translate a link/unlink op and execute it."""
    try:
        plan = translate(op, schema_map)
    except TranslationError as e:
        return f"Translation error: {e}"
    assert isinstance(plan, LinkPlan)

    if identity and ownership:
        plan = scope_link_plan(plan, identity, ownership)

    ok, message, log_entry = _execute_link_plan(op, plan, conn, verbose)
    if log_entry is not None:
        query_log.append(log_entry)
    if not ok:
        return message

    prior.append({
        "operation": plan.op,
        "entity": f"{plan.from_table.class_name}\u2194{plan.to_table.class_name}",
        "result": message,
    })
    return None


def _run_crud(
    op: dict, schema_map: SchemaMap,
    identity: IdentityContext | None, ownership: OwnershipMap | None,
    conn, query_log: list[dict], prior: list[dict], verbose: bool,
) -> str | None:
    """Translate a CRUD op, apply identity scoping, and execute."""
    try:
        stmt = translate(op, schema_map)
    except TranslationError as e:
        return f"Translation error: {e}"
    assert isinstance(stmt, SQLStatement)

    if identity and ownership:
        stmt = apply_identity(stmt, identity, ownership)

    if verbose:
        short = " ".join(stmt.sql.split())
        label = "WRITE" if stmt.is_write else "READ"
        print(f"    [SIF {label}] {short[:120]}{'...' if len(short) > 120 else ''}")

    result = execute_on_conn(conn, stmt.sql, stmt.is_write, params=stmt.params)
    query_log.append({
        "type": "sif", "operation": op,
        "sql": stmt.sql, "params": stmt.params,
        "is_write": stmt.is_write, "result": result,
    })

    if _is_db_error(result):
        return "Database error: " + _db_error_text(result)

    prior.append({"operation": op["op"], "entity": op.get("entity", ""), "result": result})
    return None


# ── Link / unlink execution ─────────────────────────────────────────────────


def _execute_link_plan(
    op: dict, plan: LinkPlan, conn, verbose: bool,
) -> tuple[bool, str, dict | None]:
    """Execute a LinkPlan on an open transaction connection.

    Returns (ok, message, log_entry).
    """
    from_pk, err = _resolve_single_pk(conn, plan.from_table, plan.from_filters)
    if err:
        return False, f"{plan.op} from-side ({plan.from_table.class_name}): {err}", None
    to_pk, err = _resolve_single_pk(conn, plan.to_table, plan.to_filters)
    if err:
        return False, f"{plan.op} to-side ({plan.to_table.class_name}): {err}", None

    link_params = {"from_id": from_pk, "to_id": to_pk}
    already_linked, err = _check_link_exists(plan, conn, link_params)
    if err:
        return False, f"{plan.op} lookup failed: {err}", None

    from_cls = plan.from_table.class_name
    to_cls = plan.to_table.class_name

    if plan.op == "link":
        return _do_link(op, plan, conn, link_params, already_linked, from_cls, to_cls, verbose)
    return _do_unlink(op, plan, conn, link_params, already_linked, from_cls, to_cls, verbose)


def _check_link_exists(
    plan: LinkPlan, conn, params: dict,
) -> tuple[bool, str | None]:
    """Check whether a junction row already exists. Returns (exists, error_text)."""
    sql = (
        f"SELECT 1 FROM {plan.junction_table} "
        f"WHERE {plan.from_fk_column} = :from_id AND {plan.to_fk_column} = :to_id LIMIT 1"
    )
    rows = execute_on_conn(conn, sql, is_write=False, params=params)
    if _is_db_error(rows):
        return False, _db_error_text(rows)
    return len(rows) > 0, None


def _do_link(
    op: dict, plan: LinkPlan, conn, params: dict,
    already_linked: bool, from_cls: str, to_cls: str, verbose: bool,
) -> tuple[bool, str, dict | None]:
    if already_linked:
        return True, f"{from_cls} and {to_cls} are already linked via {plan.relation_name} — nothing to do.", {
            "type": "sif", "operation": op,
            "sql": "(exists check)", "params": params,
            "is_write": False, "result": {"success": True, "already_linked": True},
        }

    sql = (
        f"INSERT INTO {plan.junction_table} "
        f"({plan.from_fk_column}, {plan.to_fk_column}) "
        f"VALUES (:from_id, :to_id)"
    )
    if verbose:
        print(f"    [SIF LINK] {sql}")
    result = execute_on_conn(conn, sql, is_write=True, params=params)
    log_entry = {"type": "sif", "operation": op, "sql": sql,
                 "params": params, "is_write": True, "result": result}

    if _is_db_error(result):
        return False, f"link failed: {_db_error_text(result)}", log_entry
    return True, f"Linked {from_cls} and {to_cls} via {plan.relation_name}.", log_entry


def _do_unlink(
    op: dict, plan: LinkPlan, conn, params: dict,
    already_linked: bool, from_cls: str, to_cls: str, verbose: bool,
) -> tuple[bool, str, dict | None]:
    if not already_linked:
        return True, f"{from_cls} and {to_cls} are not linked via {plan.relation_name} — nothing to do.", {
            "type": "sif", "operation": op,
            "sql": "(exists check)", "params": params,
            "is_write": False, "result": {"success": True, "already_unlinked": True},
        }

    sql = (
        f"DELETE FROM {plan.junction_table} "
        f"WHERE {plan.from_fk_column} = :from_id AND {plan.to_fk_column} = :to_id"
    )
    if verbose:
        print(f"    [SIF UNLINK] {sql}")
    result = execute_on_conn(conn, sql, is_write=True, params=params)
    log_entry = {"type": "sif", "operation": op, "sql": sql,
                 "params": params, "is_write": True, "result": result}

    if _is_db_error(result):
        return False, f"unlink failed: {_db_error_text(result)}", log_entry
    return True, f"Unlinked {from_cls} and {to_cls} via {plan.relation_name}.", log_entry


def _resolve_single_pk(conn, table: TableMap, filters: dict) -> tuple[object, str | None]:
    """Look up a row by filters and return (pk_value, None) or (None, error_text)."""
    if not filters:
        return None, "no filters provided — cannot locate the target row"

    # Accept ontology column names + physical PK/FK names (from identity scoping).
    physical_cols = set(table.column_map.values()) if table.column_map else set()
    valid = set(table.columns) | physical_cols | {table.primary_key}

    where_parts = []
    params: dict = {}
    for i, (col, val) in enumerate(filters.items()):
        if col not in valid:
            return None, f"unknown filter field '{col}' on {table.class_name}"
        phys = table.physical_column(col)
        pname = f"p{i}"
        where_parts.append(f"{phys} = :{pname}")
        params[pname] = val

    sql = (
        f"SELECT {table.primary_key} FROM {table.table_name} "
        f"WHERE {' AND '.join(where_parts)} LIMIT 2"
    )
    rows = execute_on_conn(conn, sql, is_write=False, params=params)
    if _is_db_error(rows):
        return None, f"lookup failed: {_db_error_text(rows)}"
    if len(rows) == 0:
        return None, f"no {table.class_name} matches filters {filters}"
    if len(rows) > 1:
        return None, f"filters {filters} match multiple {table.class_name} rows — narrow them to a unique row"
    return rows[0][table.primary_key], None


# ── DB error helpers ────────────────────────────────────────────────────────


def _is_db_error(result) -> bool:
    return isinstance(result, dict) and "error" in result


def _db_error_text(result: dict) -> str:
    text = result["error"]
    if result.get("detail"):
        text += f"\nDetail: {result['detail']}"
    return text


# ── Result formatting ───────────────────────────────────────────────────────


def _format_validation_failure(errors: list[str]) -> str:
    return (
        "SIF validation failed (nothing was executed):\n"
        + "\n".join(f"- {e}" for e in errors)
        + "\n\nFix the operations and resubmit."
    )


def _format_with_failure(prior_results: list[dict], failed_idx: int, error_text: str) -> str:
    """Report failing op + rollback notice so the agent can self-correct."""
    parts = [f"Operation {failed_idx} FAILED:", error_text, ""]
    if prior_results:
        parts.append(
            f"All {len(prior_results)} earlier op(s) in this batch were rolled back — "
            f"the database is unchanged."
        )
        parts.append("")
    parts.append(
        "Decide how to proceed: correct the failing op, ask the user for missing "
        "information, or try a different approach. Do not retry with the same values. "
        "Do not go silent — always respond to the user."
    )
    return "\n".join(parts)


def _format_results(results: list[dict]) -> str:
    """Format execution results as text for the conversation agent."""
    parts = []

    for r in results:
        op = r["operation"]
        entity = r["entity"]
        data = r["result"]

        if op == "query":
            if isinstance(data, list):
                if len(data) == 0:
                    parts.append(f"No {entity} records found.")
                elif len(data) == 1 and len(data[0]) == 1:
                    key = list(data[0].keys())[0]
                    parts.append(f"{data[0][key]}")
                else:
                    parts.append(json.dumps(data, default=str, indent=2))
            else:
                parts.append(json.dumps(data, default=str))

        elif op == "action":
            parts.append(str(data))

        elif op in ("link", "unlink"):
            parts.append(str(data))

        elif op in ("create", "update", "delete"):
            if isinstance(data, dict):
                returned = data.get("returned_data", [])
                affected = data.get("rows_affected", 0)
                if returned:
                    parts.append(json.dumps(returned, default=str, indent=2))
                else:
                    parts.append(f"{op.title()}d {affected} {entity} record(s).")
            else:
                parts.append(json.dumps(data, default=str))

    return "\n".join(parts)
