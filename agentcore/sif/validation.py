"""SIF operation validation — backend-neutral pre-flight checks.

Every SIF batch is validated against the SchemaMap before any backend
sees it. Errors are returned as a list of actionable strings the agent
can read and correct: unknown entity, unknown relation, unknown field,
endpoint mismatch, missing filters, etc. No exceptions, no partial
execution.

This module never touches a database. It only reads the SchemaMap and
the action registry, so it's safe to run on any backend.
"""

from agentcore.actions import get_registered_actions
from agentcore.sif.schema_map import SchemaMap, TableMap

_VALID_OPS = {"query", "create", "update", "delete", "action", "link", "unlink"}
_VALID_AGG_FNS = {"count", "sum", "avg", "min", "max"}
_VALID_SORT_DIRS = {"asc", "desc"}


def validate_operations(operations: list[dict], schema_map: SchemaMap) -> list[str]:
    """Validate SIF operations against the schema map.

    Returns a list of error messages. Empty list means valid.
    Errors are written to be actionable — they tell the LLM what's wrong
    and what the valid values are.
    """
    errors = []
    for i, op in enumerate(operations):
        prefix = f"Operation {i + 1}" if len(operations) > 1 else "Operation"
        errors.extend(_validate_one(op, schema_map, prefix))
    return errors


def _validate_one(op: dict, smap: SchemaMap, prefix: str) -> list[str]:
    errors = []
    op_type = op.get("op")

    if not op_type:
        return [f"{prefix}: missing 'op' field."]
    if op_type not in _VALID_OPS:
        return [f"{prefix}: invalid op '{op_type}'. Must be one of: {', '.join(sorted(_VALID_OPS))}"]

    if op_type == "action":
        return _validate_action(op, prefix)
    if op_type in ("link", "unlink"):
        return _validate_link_op(op, smap, prefix)

    # CRUD ops — need a valid entity
    entity = op.get("entity")
    if not entity:
        return [f"{prefix}: missing 'entity' field."]

    table = smap.tables.get(entity)
    if not table:
        available = sorted(smap.tables.keys())
        return [f"{prefix}: unknown entity '{entity}'. Valid entities: {', '.join(available)}"]

    errors.extend(_validate_fields(op, "fields", table, prefix))
    errors.extend(_validate_fields(op, "filters", table, prefix, label="filter field"))
    errors.extend(_validate_fields(op, "data", table, prefix, label="data field"))
    errors.extend(_validate_relations(op, smap, prefix))
    errors.extend(_validate_resolve(op, table, smap, prefix))
    errors.extend(_validate_aggregate(op, table, prefix))
    errors.extend(_validate_sort(op, table, prefix))

    return errors


# ── Field validation ────────────────────────────────────────────────────────


def _validate_fields(
    op: dict, key: str, table: TableMap, prefix: str,
    label: str | None = None,
) -> list[str]:
    """Check that all field names under `op[key]` exist on the table.

    Works for both list values (fields) and dict keys (filters, data).
    """
    raw = op.get(key)
    if not raw:
        return []

    names = raw if isinstance(raw, list) else raw.keys()
    valid = set(table.columns)
    label = label or "field"
    valid_str = ", ".join(sorted(valid))

    return [
        f"{prefix}: unknown {label} '{f}' on {table.class_name}. Valid fields: {valid_str}"
        for f in names if f not in valid
    ]


# ── Relation validation (query) ─────────────────────────────────────────────


def _validate_relations(op: dict, smap: SchemaMap, prefix: str) -> list[str]:
    errors = []
    for j, rel in enumerate(op.get("relations") or []):
        tag = f"{prefix}, relation {j + 1}"
        rel_name = rel.get("rel")
        rel_entity = rel.get("entity")

        if not rel_name:
            errors.append(f"{tag}: missing 'rel' field.")
            continue
        if rel_name not in smap.relations:
            available = sorted(smap.relations.keys())
            errors.append(f"{tag}: unknown relation '{rel_name}'. Valid relations: {', '.join(available)}")
            continue

        if not rel_entity:
            errors.append(f"{tag}: missing 'entity' field.")
            continue
        rel_table = smap.tables.get(rel_entity)
        if not rel_table:
            available = sorted(smap.tables.keys())
            errors.append(f"{tag}: unknown entity '{rel_entity}'. Valid entities: {', '.join(available)}")
            continue

        # Validate filters on the related entity
        rel_columns = set(rel_table.columns)
        for f in (rel.get("filters") or {}):
            if f not in rel_columns:
                errors.append(
                    f"{tag}: unknown filter field '{f}' on {rel_entity}. "
                    f"Valid fields: {', '.join(sorted(rel_columns))}"
                )

    return errors


# ── Resolve validation (create) ─────────────────────────────────────────────


def _validate_resolve(op: dict, table: TableMap, smap: SchemaMap, prefix: str) -> list[str]:
    errors = []
    for rel_name, resolve in (op.get("resolve") or {}).items():
        tag = f"{prefix}, resolve '{rel_name}'"

        if rel_name not in smap.relations:
            available = sorted(smap.relations.keys())
            errors.append(f"{prefix}, resolve: unknown relation '{rel_name}'. Valid relations: {', '.join(available)}")
            continue

        rel_map = smap.relations[rel_name]
        if not rel_map.is_direct:
            errors.append(
                f"{prefix}, resolve: relation '{rel_name}' cannot be resolved in a create "
                f"(it traverses a junction table — use a 'link' op after the create)."
            )
            continue
        if rel_map.fk_table != table.table_name:
            errors.append(
                f"{prefix}, resolve: relation '{rel_name}' cannot be resolved on {table.class_name} "
                f"(FK is on a different table)."
            )

        resolve_entity = resolve.get("entity")
        if not resolve_entity:
            continue
        resolve_table = smap.tables.get(resolve_entity)
        if not resolve_table:
            available = sorted(smap.tables.keys())
            errors.append(f"{tag}: unknown entity '{resolve_entity}'. Valid entities: {', '.join(available)}")
        else:
            resolve_columns = set(resolve_table.columns)
            for f in (resolve.get("filters") or {}):
                if f not in resolve_columns:
                    errors.append(
                        f"{tag}: unknown filter field '{f}' on {resolve_entity}. "
                        f"Valid fields: {', '.join(sorted(resolve_columns))}"
                    )

    return errors


# ── Aggregate + sort validation (query) ─────────────────────────────────────


def _validate_aggregate(op: dict, table: TableMap, prefix: str) -> list[str]:
    agg = op.get("aggregate")
    if not agg:
        return []
    errors = []
    fn = agg.get("fn")
    if fn not in _VALID_AGG_FNS:
        errors.append(f"{prefix}: invalid aggregate fn '{fn}'. Must be one of: {', '.join(sorted(_VALID_AGG_FNS))}")
    agg_field = agg.get("field")
    if agg_field and agg_field not in set(table.columns):
        errors.append(
            f"{prefix}: unknown aggregate field '{agg_field}' on {table.class_name}. "
            f"Valid fields: {', '.join(sorted(table.columns))}"
        )
    return errors


def _validate_sort(op: dict, table: TableMap, prefix: str) -> list[str]:
    sort = op.get("sort")
    if not sort:
        return []
    errors = []
    sort_field = sort.get("field")
    if sort_field and sort_field not in set(table.columns):
        errors.append(
            f"{prefix}: unknown sort field '{sort_field}' on {table.class_name}. "
            f"Valid fields: {', '.join(sorted(table.columns))}"
        )
    sort_dir = sort.get("dir", "asc")
    if sort_dir not in _VALID_SORT_DIRS:
        errors.append(f"{prefix}: invalid sort dir '{sort_dir}'. Must be 'asc' or 'desc'.")
    return errors


# ── Action validation ───────────────────────────────────────────────────────


def _validate_action(op: dict, prefix: str) -> list[str]:
    action_name = op.get("action")
    registry = get_registered_actions()
    if not action_name:
        return [f"{prefix}: action op requires 'action' field with the action name."]
    if action_name not in registry:
        available = sorted(registry.keys()) if registry else ["(none registered)"]
        return [f"{prefix}: unknown action '{action_name}'. Available actions: {', '.join(available)}"]
    return []


# ── Link / unlink validation ────────────────────────────────────────────────


def _validate_link_op(op: dict, smap: SchemaMap, prefix: str) -> list[str]:
    """Validate a link/unlink op."""
    op_type = op.get("op", "link")
    errors: list[str] = []

    relation_name = op.get("relation")
    if not relation_name:
        return [f"{prefix}: {op_type} op requires 'relation' field."]
    rel_map = smap.relations.get(relation_name)
    if not rel_map:
        available = sorted(smap.relations.keys())
        return [f"{prefix}: unknown relation '{relation_name}'. Valid relations: {', '.join(available)}"]
    if rel_map.is_direct:
        return [
            f"{prefix}: relation '{relation_name}' is a direct FK — use create/update "
            f"with 'resolve' instead of {op_type}."
        ]

    from_spec = op.get("from") or {}
    to_spec = op.get("to") or {}
    if not from_spec or not to_spec:
        return [f"{prefix}: {op_type} op requires 'from' and 'to' with {{entity, filters}}."]

    from_entity = from_spec.get("entity")
    to_entity = to_spec.get("entity")
    if not from_entity or not to_entity:
        return [f"{prefix}: {op_type} op requires 'entity' on both 'from' and 'to'."]

    allowed_pair = {rel_map.from_class, rel_map.to_class}
    given_pair = {from_entity, to_entity}
    if given_pair != allowed_pair:
        return [
            f"{prefix}: {op_type} endpoints must be {sorted(allowed_pair)} for relation "
            f"'{relation_name}', got {sorted(given_pair)}."
        ]

    for side, spec in (("from", from_spec), ("to", to_spec)):
        ent = spec.get("entity")
        tmap = smap.tables.get(ent)
        if not tmap:
            errors.append(f"{prefix}: unknown entity '{ent}' on {op_type}.{side}.")
            continue
        filters = spec.get("filters") or {}
        if not filters:
            errors.append(f"{prefix}: {op_type}.{side} requires 'filters' to locate the {ent} row.")
            continue
        valid_cols = set(tmap.columns)
        for f in filters:
            if f not in valid_cols:
                errors.append(
                    f"{prefix}: {op_type}.{side} unknown filter field '{f}' on {ent}. "
                    f"Valid fields: {', '.join(sorted(valid_cols))}"
                )

    return errors
