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
from agentcore.sif.schema_map import SchemaMap

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

    # op type
    if not op_type:
        errors.append(f"{prefix}: missing 'op' field.")
        return errors
    if op_type not in _VALID_OPS:
        errors.append(f"{prefix}: invalid op '{op_type}'. Must be one of: {', '.join(sorted(_VALID_OPS))}")
        return errors

    # action ops — check registry
    if op_type == "action":
        action_name = op.get("action")
        registry = get_registered_actions()
        if not action_name:
            errors.append(f"{prefix}: action op requires 'action' field with the action name.")
        elif action_name not in registry:
            available = sorted(registry.keys()) if registry else ["(none registered)"]
            errors.append(f"{prefix}: unknown action '{action_name}'. Available actions: {', '.join(available)}")
        return errors

    # link / unlink — junction-backed relations only
    if op_type in ("link", "unlink"):
        errors.extend(_validate_link_op(op, smap, prefix))
        return errors

    # CRUD ops — check entity
    entity = op.get("entity")
    if not entity:
        errors.append(f"{prefix}: missing 'entity' field.")
        return errors

    table = smap.tables.get(entity)
    if not table:
        available = sorted(smap.tables.keys())
        errors.append(f"{prefix}: unknown entity '{entity}'. Valid entities: {', '.join(available)}")
        return errors

    valid_columns = set(table.columns)

    # fields (query)
    if op.get("fields"):
        for f in op["fields"]:
            if f not in valid_columns:
                errors.append(
                    f"{prefix}: unknown field '{f}' on {entity}. "
                    f"Valid fields: {', '.join(sorted(valid_columns))}"
                )

    # filters
    if op.get("filters"):
        for f in op["filters"]:
            if f not in valid_columns:
                errors.append(
                    f"{prefix}: unknown filter field '{f}' on {entity}. "
                    f"Valid fields: {', '.join(sorted(valid_columns))}"
                )

    # data (create/update)
    if op.get("data"):
        for f in op["data"]:
            if f not in valid_columns:
                errors.append(
                    f"{prefix}: unknown data field '{f}' on {entity}. "
                    f"Valid fields: {', '.join(sorted(valid_columns))}"
                )

    # relations (query)
    for j, rel in enumerate(op.get("relations") or []):
        rel_name = rel.get("rel")
        rel_entity = rel.get("entity")

        if not rel_name:
            errors.append(f"{prefix}, relation {j + 1}: missing 'rel' field.")
            continue
        if rel_name not in smap.relations:
            available = sorted(smap.relations.keys())
            errors.append(
                f"{prefix}, relation {j + 1}: unknown relation '{rel_name}'. "
                f"Valid relations: {', '.join(available)}"
            )
            continue

        if not rel_entity:
            errors.append(f"{prefix}, relation {j + 1}: missing 'entity' field.")
            continue
        rel_table = smap.tables.get(rel_entity)
        if not rel_table:
            available = sorted(smap.tables.keys())
            errors.append(
                f"{prefix}, relation {j + 1}: unknown entity '{rel_entity}'. "
                f"Valid entities: {', '.join(available)}"
            )
            continue

        # Validate filters on the related entity
        rel_columns = set(rel_table.columns)
        for f in (rel.get("filters") or {}):
            if f not in rel_columns:
                errors.append(
                    f"{prefix}, relation {j + 1}: unknown filter field '{f}' on {rel_entity}. "
                    f"Valid fields: {', '.join(sorted(rel_columns))}"
                )

    # resolve (create)
    for rel_name, resolve in (op.get("resolve") or {}).items():
        if rel_name not in smap.relations:
            available = sorted(smap.relations.keys())
            errors.append(
                f"{prefix}, resolve: unknown relation '{rel_name}'. "
                f"Valid relations: {', '.join(available)}"
            )
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
                f"{prefix}, resolve: relation '{rel_name}' cannot be resolved on {entity} "
                f"(FK is on a different table)."
            )

        resolve_entity = resolve.get("entity")
        if resolve_entity:
            resolve_table = smap.tables.get(resolve_entity)
            if not resolve_table:
                available = sorted(smap.tables.keys())
                errors.append(
                    f"{prefix}, resolve '{rel_name}': unknown entity '{resolve_entity}'. "
                    f"Valid entities: {', '.join(available)}"
                )
            else:
                resolve_columns = set(resolve_table.columns)
                for f in (resolve.get("filters") or {}):
                    if f not in resolve_columns:
                        errors.append(
                            f"{prefix}, resolve '{rel_name}': unknown filter field '{f}' on {resolve_entity}. "
                            f"Valid fields: {', '.join(sorted(resolve_columns))}"
                        )

    # aggregate (query)
    agg = op.get("aggregate")
    if agg:
        fn = agg.get("fn")
        if fn not in _VALID_AGG_FNS:
            errors.append(f"{prefix}: invalid aggregate fn '{fn}'. Must be one of: {', '.join(sorted(_VALID_AGG_FNS))}")
        agg_field = agg.get("field")
        if agg_field and agg_field not in valid_columns:
            errors.append(
                f"{prefix}: unknown aggregate field '{agg_field}' on {entity}. "
                f"Valid fields: {', '.join(sorted(valid_columns))}"
            )

    # sort (query)
    sort = op.get("sort")
    if sort:
        sort_field = sort.get("field")
        if sort_field and sort_field not in valid_columns:
            errors.append(
                f"{prefix}: unknown sort field '{sort_field}' on {entity}. "
                f"Valid fields: {', '.join(sorted(valid_columns))}"
            )
        sort_dir = sort.get("dir", "asc")
        if sort_dir not in _VALID_SORT_DIRS:
            errors.append(f"{prefix}: invalid sort dir '{sort_dir}'. Must be 'asc' or 'desc'.")

    return errors


def _validate_link_op(op: dict, smap: SchemaMap, prefix: str) -> list[str]:
    """Validate a link/unlink op.

    Rules:
      - 'relation' must be a known relation
      - relation must traverse a junction table (direct-FK relations should
        use create/update/delete instead)
      - from/to specs must name known entities
      - entities must match the two endpoints of the relation (order-free)
      - filters on each side must reference valid columns
    """
    op_type = op.get("op", "link")
    errors: list[str] = []

    relation_name = op.get("relation")
    if not relation_name:
        errors.append(f"{prefix}: {op_type} op requires 'relation' field.")
        return errors
    rel_map = smap.relations.get(relation_name)
    if not rel_map:
        available = sorted(smap.relations.keys())
        errors.append(
            f"{prefix}: unknown relation '{relation_name}'. "
            f"Valid relations: {', '.join(available)}"
        )
        return errors
    if rel_map.is_direct:
        errors.append(
            f"{prefix}: relation '{relation_name}' is a direct FK — use create/update "
            f"with 'resolve' instead of {op_type}."
        )
        return errors

    from_spec = op.get("from") or {}
    to_spec = op.get("to") or {}
    if not from_spec or not to_spec:
        errors.append(f"{prefix}: {op_type} op requires 'from' and 'to' with {{entity, filters}}.")
        return errors

    from_entity = from_spec.get("entity")
    to_entity = to_spec.get("entity")
    if not from_entity or not to_entity:
        errors.append(f"{prefix}: {op_type} op requires 'entity' on both 'from' and 'to'.")
        return errors

    allowed_pair = {rel_map.from_class, rel_map.to_class}
    given_pair = {from_entity, to_entity}
    if given_pair != allowed_pair:
        errors.append(
            f"{prefix}: {op_type} endpoints must be {sorted(allowed_pair)} for relation "
            f"'{relation_name}', got {sorted(given_pair)}."
        )
        return errors

    # Validate filter columns against each endpoint's table
    for side, spec in (("from", from_spec), ("to", to_spec)):
        ent = spec.get("entity")
        tmap = smap.tables.get(ent)
        if not tmap:
            errors.append(f"{prefix}: unknown entity '{ent}' on {op_type}.{side}.")
            continue
        filters = spec.get("filters") or {}
        if not filters:
            errors.append(
                f"{prefix}: {op_type}.{side} requires 'filters' to locate the {ent} row."
            )
            continue
        valid_cols = set(tmap.columns)
        for f in filters:
            if f not in valid_cols:
                errors.append(
                    f"{prefix}: {op_type}.{side} unknown filter field '{f}' on {ent}. "
                    f"Valid fields: {', '.join(sorted(valid_cols))}"
                )

    return errors
