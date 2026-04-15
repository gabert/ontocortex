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
from agentcore.sif import LinkPlan, SchemaMap, TableMap, TranslationError
from agentcore.sif.validation import validate_operations
from agentcore.sif_sql.translator import SQLStatement, translate


def execute_sif(
    operations: list[dict],
    schema_map: SchemaMap,
    db_config,
    verbose: bool = False,
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

    # Pre-flight validation — reject the whole batch before touching the DB
    validation_errors = validate_operations(operations, schema_map)
    if validation_errors:
        return (
            "SIF validation failed (nothing was executed):\n"
            + "\n".join(f"- {e}" for e in validation_errors)
            + "\n\nFix the operations and resubmit.",
            query_log,
        )

    with open_transaction(db_config) as (conn, tx):
        for idx, op in enumerate(operations, start=1):
            op_type = op.get("op")

            # Action ops dispatch to registered Python functions
            if op_type == "action":
                failure = _dispatch_action(
                    op, schema_map, db_config, query_log, prior, verbose,
                )
                if failure is not None:
                    return _format_with_failure(prior, idx, failure), query_log
                continue

            # Link / unlink — translate to a LinkPlan, then walk it
            if op_type in ("link", "unlink"):
                try:
                    plan = translate(op, schema_map)
                except TranslationError as e:
                    return (
                        _format_with_failure(prior, idx, f"Translation error: {e}"),
                        query_log,
                    )
                assert isinstance(plan, LinkPlan)

                ok, message, log_entry = _execute_link_plan(op, plan, conn, verbose)
                if log_entry is not None:
                    query_log.append(log_entry)
                if not ok:
                    return _format_with_failure(prior, idx, message), query_log
                prior.append({
                    "operation": op_type,
                    "entity": f"{plan.from_table.class_name}↔{plan.to_table.class_name}",
                    "result": message,
                })
                continue

            # CRUD ops translate to SQL
            try:
                stmt = translate(op, schema_map)
            except TranslationError as e:
                return (
                    _format_with_failure(prior, idx, f"Translation error: {e}"),
                    query_log,
                )
            assert isinstance(stmt, SQLStatement)

            if verbose:
                short = " ".join(stmt.sql.split())
                label = "WRITE" if stmt.is_write else "READ"
                print(f"    [SIF {label}] {short[:120]}{'...' if len(short) > 120 else ''}")

            result = execute_on_conn(conn, stmt.sql, stmt.is_write, params=stmt.params)
            query_log.append({
                "type": "sif",
                "operation": op,
                "sql": stmt.sql,
                "params": stmt.params,
                "is_write": stmt.is_write,
                "result": result,
            })

            if isinstance(result, dict) and "error" in result:
                err_lines = [f"{result['error']}"]
                if result.get("detail"):
                    err_lines.append(f"Detail: {result['detail']}")
                return (
                    _format_with_failure(prior, idx, "Database error: " + "\n".join(err_lines)),
                    query_log,
                )

            prior.append({
                "operation": op["op"],
                "entity": op.get("entity", ""),
                "result": result,
            })

        # All ops succeeded — commit before leaving the context.
        tx.commit()

    return _format_results(prior), query_log


# ── Action dispatch ──────────────────────────────────────────────────────────

def _dispatch_action(
    op: dict,
    schema_map: SchemaMap,
    db_config,
    query_log: list[dict],
    prior: list[dict],
    verbose: bool,
) -> str | None:
    """Run one action op. Returns None on success, error text on failure.

    Mutates `query_log` and `prior` in place on success.
    """
    action_name = op.get("action", "")
    registry = get_registered_actions()
    action_entry = registry.get(action_name)
    if not action_entry:
        return f"Unknown action: {action_name}"

    if verbose:
        print(f"    [SIF ACTION] {action_name}")

    try:
        action_result = action_entry["fn"](
            op.get("params", {}), db_config, schema_map,
        )
    except Exception as e:
        return f"Action error ({action_name}): {e}"

    query_log.append({
        "type": "action",
        "action": action_name,
        "params": op.get("params", {}),
        "result": action_result,
    })
    prior.append({
        "operation": "action",
        "entity": action_name,
        "result": action_result,
    })
    return None


# ── Link / unlink execution ──────────────────────────────────────────────────

def _execute_link_plan(
    op: dict, plan: LinkPlan, conn, verbose: bool,
) -> tuple[bool, str, dict | None]:
    """Execute a LinkPlan on an open transaction connection.

    Returns (ok, message, log_entry). On failure, `ok` is False and
    `message` is the error text already formatted for the agent — the
    caller routes it through _format_with_failure.
    """
    # Resolve both endpoints to primary keys
    from_pk, err = _resolve_single_pk(conn, plan.from_table, plan.from_filters)
    if err:
        return False, f"{plan.op} from-side ({plan.from_table.class_name}): {err}", None
    to_pk, err = _resolve_single_pk(conn, plan.to_table, plan.to_filters)
    if err:
        return False, f"{plan.op} to-side ({plan.to_table.class_name}): {err}", None

    # Check current link state
    exists_sql = (
        f"SELECT 1 FROM {plan.junction_table} "
        f"WHERE {plan.from_fk_column} = :from_id AND {plan.to_fk_column} = :to_id LIMIT 1"
    )
    existing = execute_on_conn(
        conn, exists_sql, is_write=False,
        params={"from_id": from_pk, "to_id": to_pk},
    )
    if isinstance(existing, dict) and "error" in existing:
        detail = existing.get("detail", "")
        return (
            False,
            f"{plan.op} lookup failed: {existing.get('error')}{': ' + detail if detail else ''}",
            None,
        )
    already_linked = len(existing) > 0

    from_cls = plan.from_table.class_name
    to_cls = plan.to_table.class_name

    if plan.op == "link":
        if already_linked:
            msg = f"{from_cls} and {to_cls} are already linked via {plan.relation_name} — nothing to do."
            return True, msg, {
                "type": "sif",
                "operation": op,
                "sql": exists_sql,
                "params": {"from_id": from_pk, "to_id": to_pk},
                "is_write": False,
                "result": {"success": True, "already_linked": True},
            }

        insert_sql = (
            f"INSERT INTO {plan.junction_table} "
            f"({plan.from_fk_column}, {plan.to_fk_column}) "
            f"VALUES (:from_id, :to_id)"
        )
        if verbose:
            print(f"    [SIF LINK] {insert_sql}")
        result = execute_on_conn(
            conn, insert_sql, is_write=True,
            params={"from_id": from_pk, "to_id": to_pk},
        )
        log_entry = {
            "type": "sif",
            "operation": op,
            "sql": insert_sql,
            "params": {"from_id": from_pk, "to_id": to_pk},
            "is_write": True,
            "result": result,
        }
        if isinstance(result, dict) and "error" in result:
            err = result.get("error", "")
            detail = result.get("detail", "")
            return False, f"link failed: {err}{': ' + detail if detail else ''}", log_entry
        return True, f"Linked {from_cls} and {to_cls} via {plan.relation_name}.", log_entry

    # unlink
    if not already_linked:
        msg = f"{from_cls} and {to_cls} are not linked via {plan.relation_name} — nothing to do."
        return True, msg, {
            "type": "sif",
            "operation": op,
            "sql": exists_sql,
            "params": {"from_id": from_pk, "to_id": to_pk},
            "is_write": False,
            "result": {"success": True, "already_unlinked": True},
        }

    delete_sql = (
        f"DELETE FROM {plan.junction_table} "
        f"WHERE {plan.from_fk_column} = :from_id AND {plan.to_fk_column} = :to_id"
    )
    if verbose:
        print(f"    [SIF UNLINK] {delete_sql}")
    result = execute_on_conn(
        conn, delete_sql, is_write=True,
        params={"from_id": from_pk, "to_id": to_pk},
    )
    log_entry = {
        "type": "sif",
        "operation": op,
        "sql": delete_sql,
        "params": {"from_id": from_pk, "to_id": to_pk},
        "is_write": True,
        "result": result,
    }
    if isinstance(result, dict) and "error" in result:
        err = result.get("error", "")
        detail = result.get("detail", "")
        return False, f"unlink failed: {err}{': ' + detail if detail else ''}", log_entry
    return True, f"Unlinked {from_cls} and {to_cls} via {plan.relation_name}.", log_entry


def _resolve_single_pk(conn, table: TableMap, filters: dict) -> tuple[object, str | None]:
    """Look up a row by filters and return (pk_value, None) or (None, error_text).

    Returns an error when the filters do not match exactly one row — both
    zero matches (missing) and multiple matches (ambiguous) are treated as
    user-fixable, not raised.
    """
    if not filters:
        return None, "no filters provided — cannot locate the target row"

    where_parts = []
    params: dict = {}
    for i, (col, val) in enumerate(filters.items()):
        if col not in table.columns:
            return None, f"unknown filter field '{col}' on {table.class_name}"
        pname = f"p{i}"
        where_parts.append(f"{col} = :{pname}")
        params[pname] = val

    sql = (
        f"SELECT {table.primary_key} FROM {table.table_name} "
        f"WHERE {' AND '.join(where_parts)} LIMIT 2"
    )
    rows = execute_on_conn(conn, sql, is_write=False, params=params)
    if isinstance(rows, dict) and "error" in rows:
        detail = rows.get("detail", "")
        return None, f"lookup failed: {rows.get('error')}{': ' + detail if detail else ''}"
    if len(rows) == 0:
        return None, f"no {table.class_name} matches filters {filters}"
    if len(rows) > 1:
        return None, f"filters {filters} match multiple {table.class_name} rows — narrow them to a unique row"
    return rows[0][table.primary_key], None


# ── Result formatting ────────────────────────────────────────────────────────

def _format_with_failure(prior_results: list[dict], failed_idx: int, error_text: str) -> str:
    """Report failing op + rollback notice so the agent can self-correct.

    Prior successes within the same batch are rolled back by the surrounding
    transaction, so we tell the agent exactly that — no 'partially applied'
    state to reason about.
    """
    parts: list[str] = []
    parts.append(f"Operation {failed_idx} FAILED:")
    parts.append(error_text)
    parts.append("")
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
                    # Single aggregate value
                    key = list(data[0].keys())[0]
                    parts.append(f"{data[0][key]}")
                else:
                    parts.append(json.dumps(data, default=str, indent=2))
            else:
                parts.append(json.dumps(data, default=str))

        elif op == "action":
            # Action results are already strings from the action function
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
