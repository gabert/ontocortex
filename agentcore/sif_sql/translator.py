"""SQL translator — turns SIF op dicts into parameterized SQL.

Pure and I/O-free. Every CRUD op produces a SQLStatement (SQL string +
bind params + write flag). Every link/unlink op produces a LinkPlan
(from the backend-neutral `agentcore.sif` package) that the executor
walks against an open transaction.

This module never opens connections or runs queries — that's
`agentcore.sif_sql.executor`. Keeping the split means the translator
is unit-testable without a database and the executor stays focused on
transaction handling and result formatting.
"""

from dataclasses import dataclass

from agentcore.sif import LinkPlan, SchemaMap, TableMap, TranslationError


@dataclass
class SQLStatement:
    """A parameterized SQL statement ready for execution (CRUD ops)."""
    sql: str
    params: dict
    is_write: bool
    table_name: str = ""   # Physical table name (for post-translation layers)
    op_type: str = ""      # "query"/"create"/"update"/"delete"


class _Params:
    """Auto-incrementing named parameter builder (:p1, :p2, ...)."""

    def __init__(self) -> None:
        self.values: dict = {}
        self._counter = 0

    def add(self, value) -> str:
        """Register a value and return its bind placeholder (e.g. ':p1')."""
        self._counter += 1
        name = f"p{self._counter}"
        self.values[name] = value
        return f":{name}"


# ── Public API ──────────────────────────────────────────────────────────────


def translate(operation: dict, schema_map: SchemaMap) -> SQLStatement | LinkPlan:
    """Translate a single SIF operation dict.

    Returns a SQLStatement for CRUD ops (query/create/update/delete) and a
    LinkPlan for link/unlink. Raises TranslationError for unknown entities,
    relations, invalid structure, or action ops (which are dispatched
    directly by the executor, not translated).
    """
    op_type = operation.get("op")

    if op_type == "action":
        raise TranslationError("Action ops are dispatched directly, not translated to SQL")

    if op_type in ("link", "unlink"):
        return _translate_link(operation, schema_map)

    entity = operation.get("entity")
    table = schema_map.tables.get(entity)
    if not table:
        raise TranslationError(f"Unknown entity: {entity}")

    if op_type == "query":
        return _translate_query(operation, table, schema_map)
    elif op_type == "create":
        return _translate_create(operation, table, schema_map)
    elif op_type == "update":
        return _translate_update(operation, table, schema_map)
    elif op_type == "delete":
        return _translate_delete(operation, table, schema_map)
    else:
        raise TranslationError(f"Unknown operation type: {op_type}")


def translate_all(
    operations: list[dict], schema_map: SchemaMap,
) -> list[SQLStatement | LinkPlan]:
    """Translate a list of SIF operations."""
    return [translate(op, schema_map) for op in operations]


# ── Query ───────────────────────────────────────────────────────────────────


def _translate_query(op: dict, table: TableMap, smap: SchemaMap) -> SQLStatement:
    p = _Params()
    t = table.table_name

    select = _build_select(op, table, t)
    joins, join_wheres = _build_joins(op, table, smap, p)
    direct_wheres = _build_where(op.get("filters"), table, p, qualified=t)
    wheres = join_wheres + direct_wheres

    sql = f"SELECT {select} FROM {t}"
    for j in joins:
        sql += f" {j}"
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)

    sort = op.get("sort")
    if sort:
        direction = sort.get("dir", "asc").upper()
        sql += f" ORDER BY {t}.{table.physical_column(sort['field'])} {direction}"

    limit = op.get("limit")
    if limit:
        sql += f" LIMIT {p.add(limit)}"

    return SQLStatement(sql=sql, params=p.values, is_write=False,
                        table_name=t, op_type="query")


def _build_select(op: dict, table: TableMap, table_alias: str) -> str:
    """Build the SELECT clause: aggregate, explicit fields, or wildcard."""
    agg = op.get("aggregate")
    if agg:
        fn = agg["fn"]
        agg_field = agg.get("field")
        if agg_field:
            return f"{fn}({table_alias}.{table.physical_column(agg_field)})"
        return f"{fn}(*)"

    fields = op.get("fields")
    if fields:
        return ", ".join(f"{table_alias}.{table.physical_column(f)}" for f in fields)

    return f"{table_alias}.*"


def _build_joins(
    op: dict, table: TableMap, smap: SchemaMap, p: _Params,
) -> tuple[list[str], list[str]]:
    """Build JOIN clauses and WHERE conditions from relation traversals.

    Returns (join_clauses, where_conditions).
    """
    joins: list[str] = []
    wheres: list[str] = []
    t = table.table_name

    for rel in op.get("relations") or []:
        rel_name = rel["rel"]
        rel_entity = rel["entity"]

        rel_map = smap.relations.get(rel_name)
        rel_table = smap.tables.get(rel_entity)
        if not rel_map:
            raise TranslationError(f"Unknown relation: {rel_name}")
        if not rel_table:
            raise TranslationError(f"Unknown entity in relation: {rel_entity}")

        _emit_join_steps(joins, t, rel_map, rel_name)

        for field_name, value in (rel.get("filters") or {}).items():
            phys = rel_table.physical_column(field_name)
            wheres.append(f"{rel_table.table_name}.{phys} = {p.add(value)}")

    return joins, wheres


def _emit_join_steps(
    joins: list[str], anchor: str, rel_map, rel_name: str,
) -> None:
    """Emit one JOIN per step in topological order.

    At each iteration, pick a remaining step whose endpoints include at
    least one table already in `joined`, and join the other endpoint.
    This way the same stored step list works whether the query starts
    from the domain or range side of the ontology relation.
    """
    joined = {anchor}
    remaining = list(rel_map.steps)

    while remaining:
        for i, step in enumerate(remaining):
            endpoints = {step.fk_table, step.ref_table}
            anchored = endpoints & joined
            if not anchored:
                continue
            unjoined = endpoints - joined
            if not unjoined:
                remaining.pop(i)
                break
            target = next(iter(unjoined))
            join_cond = (
                f"{step.fk_table}.{step.fk_column} = "
                f"{step.ref_table}.{step.ref_column}"
            )
            joins.append(f"JOIN {target} ON {join_cond}")
            joined.add(target)
            remaining.pop(i)
            break
        else:
            raise TranslationError(
                f"Cannot build join path for relation '{rel_name}' "
                f"from {anchor}: no remaining step connects to "
                f"already-joined tables {joined}"
            )


# ── Create ──────────────────────────────────────────────────────────────────


def _translate_create(op: dict, table: TableMap, smap: SchemaMap) -> SQLStatement:
    p = _Params()
    columns: list[str] = []
    values: list[str] = []

    for field_name, value in (op.get("data") or {}).items():
        columns.append(table.physical_column(field_name))
        values.append(p.add(value))

    for rel_name, resolve in (op.get("resolve") or {}).items():
        col, subquery = _resolve_fk_subquery(rel_name, resolve, table, smap, p)
        columns.append(col)
        values.append(subquery)

    sql = (
        f"INSERT INTO {table.table_name} "
        f"({', '.join(columns)}) VALUES ({', '.join(values)}) RETURNING *"
    )
    return SQLStatement(sql=sql, params=p.values, is_write=True,
                        table_name=table.table_name, op_type="create")


def _resolve_fk_subquery(
    rel_name: str, resolve: dict, table: TableMap, smap: SchemaMap, p: _Params,
) -> tuple[str, str]:
    """Build a (fk_column, subselect) pair for a resolve clause.

    Validates that the relation is a direct FK on the target table.
    """
    rel_map = smap.relations.get(rel_name)
    if not rel_map:
        raise TranslationError(f"Unknown relation for resolve: {rel_name}")

    if not rel_map.is_direct:
        raise TranslationError(
            f"Cannot resolve {rel_name}: traverses a junction table. "
            f"Use a 'link' op after the create instead."
        )
    if rel_map.fk_table != table.table_name:
        raise TranslationError(
            f"Cannot resolve {rel_name}: FK is on {rel_map.fk_table}, not {table.table_name}"
        )

    resolve_entity = resolve.get("entity")
    resolve_table = smap.tables.get(resolve_entity)
    if not resolve_table:
        raise TranslationError(f"Unknown entity in resolve: {resolve_entity}")

    sub_wheres = [
        f"{resolve_table.physical_column(f)} = {p.add(v)}"
        for f, v in (resolve.get("filters") or {}).items()
    ]

    subquery = f"(SELECT {resolve_table.primary_key} FROM {resolve_table.table_name}"
    if sub_wheres:
        subquery += " WHERE " + " AND ".join(sub_wheres)
    subquery += " LIMIT 1)"

    return rel_map.fk_column, subquery


# ── Update ──────────────────────────────────────────────────────────────────


def _translate_update(op: dict, table: TableMap, smap: SchemaMap) -> SQLStatement:
    p = _Params()

    sets = [
        f"{table.physical_column(f)} = {p.add(v)}"
        for f, v in (op.get("data") or {}).items()
    ]
    if not sets:
        raise TranslationError("Update operation requires at least one field in 'data'")

    wheres = _build_where(op.get("filters"), table, p)

    sql = f"UPDATE {table.table_name} SET {', '.join(sets)}"
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " RETURNING *"

    return SQLStatement(sql=sql, params=p.values, is_write=True,
                        table_name=table.table_name, op_type="update")


# ── Delete ──────────────────────────────────────────────────────────────────


def _translate_delete(op: dict, table: TableMap, smap: SchemaMap) -> SQLStatement:
    p = _Params()
    wheres = _build_where(op.get("filters"), table, p)

    sql = f"DELETE FROM {table.table_name}"
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " RETURNING *"

    return SQLStatement(sql=sql, params=p.values, is_write=True,
                        table_name=table.table_name, op_type="delete")


# ── Shared helpers ──────────────────────────────────────────────────────────


def _build_where(
    filters: dict | None, table: TableMap, p: _Params,
    qualified: str | None = None,
) -> list[str]:
    """Build WHERE conditions from a filters dict.

    When `qualified` is set, column references are prefixed with it
    (e.g. 'sl_loans.loan_status'). Otherwise bare column names are used.
    """
    wheres: list[str] = []
    for field_name, value in (filters or {}).items():
        phys = table.physical_column(field_name)
        col_ref = f"{qualified}.{phys}" if qualified else phys
        wheres.append(f"{col_ref} = {p.add(value)}")
    return wheres


# ── Link / Unlink ───────────────────────────────────────────────────────────


def _translate_link(op: dict, smap: SchemaMap) -> LinkPlan:
    """Produce a structured plan for a link/unlink op.

    The plan carries the junction table, the two endpoint tables, their
    filters, and the FK columns on the junction. The executor turns this
    into three SELECTs + one INSERT/DELETE inside a transaction.
    """
    op_type = op.get("op")
    if op_type not in ("link", "unlink"):
        raise TranslationError(f"_translate_link called with op '{op_type}'")

    relation_name = op.get("relation")
    rel_map = smap.relations.get(relation_name)
    if not rel_map:
        raise TranslationError(f"Unknown relation: {relation_name}")
    if rel_map.is_direct or not rel_map.junction_table:
        raise TranslationError(
            f"{op_type}: relation '{relation_name}' is not junction-backed"
        )

    from_spec = op.get("from") or {}
    to_spec = op.get("to") or {}
    from_entity = from_spec.get("entity")
    to_entity = to_spec.get("entity")

    from_table = smap.tables.get(from_entity)
    to_table = smap.tables.get(to_entity)
    if not from_table or not to_table:
        raise TranslationError(
            f"{op_type}: unknown endpoint entity ({from_entity!r}, {to_entity!r})"
        )

    from_step = next(
        (s for s in rel_map.steps if s.ref_table == from_table.table_name),
        None,
    )
    to_step = next(
        (s for s in rel_map.steps if s.ref_table == to_table.table_name),
        None,
    )
    if not from_step or not to_step:
        raise TranslationError(
            f"{op_type}: junction steps do not match endpoints "
            f"{from_entity}/{to_entity} for relation '{relation_name}'"
        )

    return LinkPlan(
        op=op_type,
        relation_name=relation_name,
        from_table=from_table,
        from_filters=dict(from_spec.get("filters") or {}),
        to_table=to_table,
        to_filters=dict(to_spec.get("filters") or {}),
        junction_table=rel_map.junction_table,
        from_fk_column=from_step.fk_column,
        to_fk_column=to_step.fk_column,
    )
