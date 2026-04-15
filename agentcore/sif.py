"""SIF: Structured Intent Format — deterministic ontology-to-SQL translator.

Two responsibilities:
1. SchemaMap — builds ontology-to-physical mapping from compact ontology + schema.json
2. translate() — converts SIF operation dicts to parameterized SQL

No LLM calls. Pure deterministic mapping.
"""

import copy
import json
from dataclasses import dataclass, field
from pathlib import Path


# ── Mapping dataclasses ──────────────────────────────────────────────────────

@dataclass
class TableMap:
    class_name: str      # Ontology local name: "Customer"
    class_iri: str       # Ontology IRI: "https://.../ontology#Customer"
    table_name: str      # Physical: "ins_customers"
    primary_key: str     # Physical: "ins_customer_id"
    columns: list[str]   # Physical data column names (no PK/FK/created_at)
    comment: str = ""


@dataclass
class JoinStep:
    """A single FK-based JOIN between two physical tables."""
    fk_table: str        # Physical table holding the FK
    fk_column: str       # FK column name
    ref_table: str       # Referenced table
    ref_column: str      # Referenced PK column


@dataclass
class RelationMap:
    name: str                  # Ontology local name: "hasPolicy"
    iri: str                   # Ontology IRI of the ObjectProperty
    from_class: str            # Ontology domain class local name: "Customer"
    to_class: str              # Ontology range class local name: "Policy"
    steps: list[JoinStep] = field(default_factory=list)
    junction_table: str | None = None  # Set when the relation traverses a junction

    # Convenience accessors for single-step (direct FK) relations — used by
    # create/resolve and validate. Multi-step relations raise on access.
    @property
    def is_direct(self) -> bool:
        return len(self.steps) == 1

    @property
    def fk_table(self) -> str:
        if not self.is_direct:
            raise ValueError(f"Relation '{self.name}' is not a direct FK (it goes through a junction)")
        return self.steps[0].fk_table

    @property
    def fk_column(self) -> str:
        if not self.is_direct:
            raise ValueError(f"Relation '{self.name}' is not a direct FK")
        return self.steps[0].fk_column


class SchemaMap:
    """Ontology-to-physical schema mapping.

    Built once per domain from a structured ontology model + schema.json.
    Classes are matched to tables by IRI (with rdfs:comment used as a
    deterministic fallback during first attachment). After construction,
    all lookups are IRI-keyed — there is no more text matching anywhere.
    """

    def __init__(self, ontology_model: dict, schema: dict) -> None:
        self.tables: dict[str, TableMap] = {}        # class local_name -> TableMap
        self.tables_by_iri: dict[str, TableMap] = {}
        self.relations: dict[str, RelationMap] = {}  # object-property local_name -> RelationMap
        self.junction_tables: set[str] = set()
        self._schema = schema
        self._build(ontology_model, schema)

    def _build(self, model: dict, schema: dict) -> None:
        self._build_tables(model, schema)
        self.junction_tables = self._detect_junction_tables(schema)
        self._build_relations(model, schema)

    # ── Class → table mapping ────────────────────────────────────────────

    def _build_tables(self, model: dict, schema: dict) -> None:
        """Attach an ontology class IRI to each physical table.

        Match order (most reliable first):
          1. Table already carries an `ontology_iri` field (forward-compat).
          2. `rdfs:comment` on the class equals the table's `comment`.
        Failure to match any class is surfaced loudly — we never ship a
        half-mapped schema.
        """
        tables_by_comment = {}
        for t in schema["tables"]:
            c = (t.get("comment") or "").strip()
            if c:
                tables_by_comment.setdefault(c, t)

        for cls in model["classes"]:
            iri = cls["iri"]
            local = cls["local_name"]
            comment = (cls["comment"] or "").strip()

            table = None
            for t in schema["tables"]:
                if t.get("ontology_iri") == iri:
                    table = t
                    break
            if table is None and comment:
                table = tables_by_comment.get(comment)

            if table is None:
                continue  # class has no physical table (value sets, abstract, etc.)

            cols = [c["name"] for c in table.get("columns", [])]
            tmap = TableMap(
                class_name=local,
                class_iri=iri,
                table_name=table["name"],
                primary_key=table["primary_key"],
                columns=cols,
                comment=table.get("comment", ""),
            )
            self.tables[local] = tmap
            self.tables_by_iri[iri] = tmap

    # ── Junction detection ───────────────────────────────────────────────

    _SKIP_COL_NAMES = {"created_at", "updated_at"}

    def _detect_junction_tables(self, schema: dict) -> set[str]:
        """A junction table carries only FK columns (plus PK/timestamps).

        Such a table represents a many-to-many relationship between its two
        (or more) FK targets and is not itself an ontology class.
        """
        junctions: set[str] = set()
        for t in schema["tables"]:
            if t.get("lookup_table"):
                continue
            fks = t.get("foreign_keys") or []
            if len(fks) < 2:
                continue
            data_cols = [
                c for c in (t.get("columns") or [])
                if c["name"] not in self._SKIP_COL_NAMES
            ]
            if not data_cols:
                junctions.add(t["name"])
        return junctions

    # ── Relation → join-path mapping ─────────────────────────────────────

    def _build_relations(self, model: dict, schema: dict) -> None:
        tables_by_name = {t["name"]: t for t in schema["tables"]}

        for prop in model["object_properties"]:
            from_iri = prop.get("domain_iri")
            to_iri = prop.get("range_iri")
            if not from_iri or not to_iri:
                continue
            from_map = self.tables_by_iri.get(from_iri)
            to_map = self.tables_by_iri.get(to_iri)
            if not from_map or not to_map:
                continue

            steps, junction = self._find_relation_path(tables_by_name, from_map, to_map)
            if not steps:
                continue

            self.relations[prop["local_name"]] = RelationMap(
                name=prop["local_name"],
                iri=prop["iri"],
                from_class=from_map.class_name,
                to_class=to_map.class_name,
                steps=steps,
                junction_table=junction,
            )

    def _find_relation_path(
        self, tables_by_name: dict, from_map: TableMap, to_map: TableMap,
    ) -> tuple[list[JoinStep], str | None]:
        """Find a 1-step direct FK or 2-step junction path between two tables."""
        # Direct FK, either direction
        direct = self._direct_fk(tables_by_name, from_map.table_name, to_map.table_name)
        if direct:
            return [direct], None
        direct = self._direct_fk(tables_by_name, to_map.table_name, from_map.table_name)
        if direct:
            return [direct], None

        # Junction path: find a junction table with FKs to both endpoints.
        for junction_name in self.junction_tables:
            j = tables_by_name[junction_name]
            fk_to_from = next(
                (fk for fk in j.get("foreign_keys", []) if fk["references_table"] == from_map.table_name),
                None,
            )
            fk_to_to = next(
                (fk for fk in j.get("foreign_keys", []) if fk["references_table"] == to_map.table_name),
                None,
            )
            if fk_to_from and fk_to_to:
                step1 = JoinStep(
                    fk_table=junction_name,
                    fk_column=fk_to_from["column"],
                    ref_table=from_map.table_name,
                    ref_column=fk_to_from["references_column"],
                )
                step2 = JoinStep(
                    fk_table=junction_name,
                    fk_column=fk_to_to["column"],
                    ref_table=to_map.table_name,
                    ref_column=fk_to_to["references_column"],
                )
                return [step1, step2], junction_name

        return [], None

    @staticmethod
    def _direct_fk(tables_by_name: dict, child_name: str, parent_name: str) -> JoinStep | None:
        """Return a JoinStep if `child_name` has a FK referencing `parent_name`."""
        child = tables_by_name.get(child_name)
        if not child:
            return None
        for fk in child.get("foreign_keys", []):
            if fk["references_table"] == parent_name:
                return JoinStep(
                    fk_table=child_name,
                    fk_column=fk["column"],
                    ref_table=parent_name,
                    ref_column=fk["references_column"],
                )
        return None


# ── Tool schema builder ──────────────────────────────────────────────────────

_SIF_SCHEMA_PATH = Path(__file__).resolve().parent / "sif_schema.json"
_BASE_SIF_SCHEMA = json.loads(_SIF_SCHEMA_PATH.read_text(encoding="utf-8"))


def build_sif_tool(schema_map: "SchemaMap") -> dict:
    """Build the submit_sif tool definition with domain-specific enums injected.

    The model cannot emit an entity, relation, or action name outside the
    lists derived from the current domain — bad names are prevented at
    generation time rather than caught after commit.
    """
    schema = copy.deepcopy(_BASE_SIF_SCHEMA)
    op_props = schema["properties"]["operations"]["items"]["properties"]

    class_names = sorted(schema_map.tables.keys())
    rel_names = sorted(schema_map.relations.keys())
    action_names = sorted(_action_registry.keys())

    if class_names:
        op_props["entity"]["enum"] = class_names
        rel_item_props = op_props["relations"]["items"]["properties"]
        rel_item_props["entity"]["enum"] = class_names

    if rel_names:
        op_props["relations"]["items"]["properties"]["rel"]["enum"] = rel_names
        # resolve is a dict keyed by relation name — constrain the keys
        op_props["resolve"]["propertyNames"] = {"enum": rel_names}

    if action_names:
        op_props["action"]["enum"] = action_names

    return {
        "name": "submit_sif",
        "description": (
            "Submit structured operations against the domain model. "
            "Use ontology class names for entities and ontology property names for fields. "
            "Call this whenever the user asks you to look up, create, update, or delete information."
        ),
        "input_schema": schema,
    }


# ── SQL output ───────────────────────────────────────────────────────────────

@dataclass
class SQLStatement:
    sql: str
    params: dict
    is_write: bool


# ── Translator ───────────────────────────────────────────────────────────────

class TranslationError(Exception):
    """Raised when a SIF operation cannot be translated to SQL."""


# ── Validation ──────────────────────────────────────────────────────────────

_VALID_OPS = {"query", "create", "update", "delete", "action"}
_VALID_AGG_FNS = {"count", "sum", "avg", "min", "max"}
_VALID_SORT_DIRS = {"asc", "desc"}


def validate_operations(operations: list[dict], schema_map: "SchemaMap") -> list[str]:
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


def _validate_one(op: dict, smap: "SchemaMap", prefix: str) -> list[str]:
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
        if not action_name:
            errors.append(f"{prefix}: action op requires 'action' field with the action name.")
        elif action_name not in _action_registry:
            available = sorted(_action_registry.keys()) if _action_registry else ["(none registered)"]
            errors.append(f"{prefix}: unknown action '{action_name}'. Available actions: {', '.join(available)}")
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
                f"(it traverses a junction table — create the junction row separately)."
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


# ── Action registry ─────────────────────────────────────────────────────────

# Each action is a callable: (params: dict, db_config, schema_map) -> str
# Registered per-domain by the pipeline at startup.
ActionFn = None  # type alias placeholder — it's Callable[[dict, Any, SchemaMap], str]

_action_registry: dict[str, dict] = {}
# Structure: {"action_name": {"fn": callable, "description": "...", "params_schema": {...}}}


def register_action(name: str, fn, description: str = "", params_schema: dict | None = None) -> None:
    """Register a domain action by name."""
    _action_registry[name] = {
        "fn": fn,
        "description": description,
        "params_schema": params_schema or {},
    }


def clear_actions() -> None:
    """Clear all registered actions (used when switching domains)."""
    _action_registry.clear()


def get_registered_actions() -> dict[str, dict]:
    """Return the current action registry (for building LLM prompts)."""
    return dict(_action_registry)


def load_domain_actions(domain_dir) -> int:
    """Load actions from a domain's actions/ directory.

    Each .py file must define:
      DEFINITION = {"name": "...", "description": "...", "params_schema": {...}}
      def execute(params: dict, db_config, schema_map) -> str: ...

    Returns the number of actions loaded.
    """
    import importlib.util
    from pathlib import Path

    actions_dir = Path(domain_dir) / "actions"
    if not actions_dir.is_dir():
        return 0

    count = 0
    for path in sorted(actions_dir.glob("*.py")):
        if path.name.startswith("_"):
            continue
        spec = importlib.util.spec_from_file_location(path.stem, path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        defn = getattr(mod, "DEFINITION", None)
        fn = getattr(mod, "execute", None)
        if defn and fn:
            register_action(
                name=defn["name"],
                fn=fn,
                description=defn.get("description", ""),
                params_schema=defn.get("params_schema", {}),
            )
            count += 1

    return count


def translate(operation: dict, schema_map: SchemaMap) -> SQLStatement:
    """Translate a single SIF operation dict to a parameterized SQL statement.

    Raises TranslationError for unknown entities, relations, or invalid structure.
    Does not handle 'action' ops — those are dispatched directly by execute_sif().
    """
    op_type = operation.get("op")
    entity = operation.get("entity")

    if op_type == "action":
        raise TranslationError("Action ops are dispatched directly, not translated to SQL")

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


def translate_all(operations: list[dict], schema_map: SchemaMap) -> list[SQLStatement]:
    """Translate a list of SIF operations."""
    return [translate(op, schema_map) for op in operations]


# ── Query ────────────────────────────────────────────────────────────────────

def _translate_query(op: dict, table: TableMap, smap: SchemaMap) -> SQLStatement:
    params = {}
    counter = [0]

    def _p(value):
        counter[0] += 1
        name = f"p{counter[0]}"
        params[name] = value
        return f":{name}"

    t = table.table_name

    # SELECT
    agg = op.get("aggregate")
    if agg:
        fn = agg["fn"]
        agg_field = agg.get("field")
        select = f"{fn}({t}.{agg_field})" if agg_field else f"{fn}(*)"
    else:
        fields = op.get("fields")
        if fields:
            select = ", ".join(f"{t}.{f}" for f in fields)
        else:
            select = f"{t}.*"

    # JOINs + WHERE from relations
    joins = []
    wheres = []

    for rel in op.get("relations") or []:
        rel_name = rel["rel"]
        rel_entity = rel["entity"]

        rel_map = smap.relations.get(rel_name)
        rel_table = smap.tables.get(rel_entity)
        if not rel_map:
            raise TranslationError(f"Unknown relation: {rel_name}")
        if not rel_table:
            raise TranslationError(f"Unknown entity in relation: {rel_entity}")

        # Emit one JOIN per step. A direct FK is 1 step; a junction-table
        # relation is 2 steps (endpoint -> junction -> other endpoint). The
        # JOIN target is whichever side of the step is not already in the
        # FROM clause / already joined.
        joined = {t}
        for step in rel_map.steps:
            endpoints = {step.fk_table, step.ref_table}
            target = next((x for x in endpoints if x not in joined), step.ref_table)
            join_cond = f"{step.fk_table}.{step.fk_column} = {step.ref_table}.{step.ref_column}"
            joins.append(f"JOIN {target} ON {join_cond}")
            joined.add(target)

        for field_name, value in (rel.get("filters") or {}).items():
            wheres.append(f"{rel_table.table_name}.{field_name} = {_p(value)}")

    # Direct filters on main entity
    for field_name, value in (op.get("filters") or {}).items():
        wheres.append(f"{t}.{field_name} = {_p(value)}")

    # BUILD
    sql = f"SELECT {select} FROM {t}"
    for j in joins:
        sql += f" {j}"
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)

    sort = op.get("sort")
    if sort:
        direction = sort.get("dir", "asc").upper()
        sql += f" ORDER BY {t}.{sort['field']} {direction}"

    limit = op.get("limit")
    if limit:
        sql += f" LIMIT {_p(limit)}"

    return SQLStatement(sql=sql, params=params, is_write=False)


# ── Create ───────────────────────────────────────────────────────────────────

def _translate_create(op: dict, table: TableMap, smap: SchemaMap) -> SQLStatement:
    params = {}
    counter = [0]

    def _p(value):
        counter[0] += 1
        name = f"p{counter[0]}"
        params[name] = value
        return f":{name}"

    columns = []
    values = []

    # Data fields
    for field_name, value in (op.get("data") or {}).items():
        columns.append(field_name)
        values.append(_p(value))

    # Resolve relations -> FK subqueries
    for rel_name, resolve in (op.get("resolve") or {}).items():
        rel_map = smap.relations.get(rel_name)
        if not rel_map:
            raise TranslationError(f"Unknown relation for resolve: {rel_name}")

        # Create/resolve only supports direct FKs — junction relations need
        # an explicit second create op for the junction row.
        if not rel_map.is_direct:
            raise TranslationError(
                f"Cannot resolve {rel_name}: traverses a junction table. "
                f"Create the junction row explicitly."
            )

        # FK must be on the table we're inserting into
        if rel_map.fk_table != table.table_name:
            raise TranslationError(
                f"Cannot resolve {rel_name}: FK is on {rel_map.fk_table}, not {table.table_name}"
            )

        resolve_entity = resolve.get("entity")
        resolve_table = smap.tables.get(resolve_entity)
        if not resolve_table:
            raise TranslationError(f"Unknown entity in resolve: {resolve_entity}")

        # Build subquery
        sub_wheres = []
        for f, v in (resolve.get("filters") or {}).items():
            sub_wheres.append(f"{f} = {_p(v)}")

        subquery = f"(SELECT {resolve_table.primary_key} FROM {resolve_table.table_name}"
        if sub_wheres:
            subquery += " WHERE " + " AND ".join(sub_wheres)
        subquery += " LIMIT 1)"

        columns.append(rel_map.fk_column)
        values.append(subquery)

    sql = f"INSERT INTO {table.table_name} ({', '.join(columns)}) VALUES ({', '.join(values)}) RETURNING *"
    return SQLStatement(sql=sql, params=params, is_write=True)


# ── Update ───────────────────────────────────────────────────────────────────

def _translate_update(op: dict, table: TableMap, smap: SchemaMap) -> SQLStatement:
    params = {}
    counter = [0]

    def _p(value):
        counter[0] += 1
        name = f"p{counter[0]}"
        params[name] = value
        return f":{name}"

    sets = []
    for field_name, value in (op.get("data") or {}).items():
        sets.append(f"{field_name} = {_p(value)}")

    if not sets:
        raise TranslationError("Update operation requires at least one field in 'data'")

    wheres = []
    for field_name, value in (op.get("filters") or {}).items():
        wheres.append(f"{field_name} = {_p(value)}")

    sql = f"UPDATE {table.table_name} SET {', '.join(sets)}"
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " RETURNING *"

    return SQLStatement(sql=sql, params=params, is_write=True)


# ── Delete ───────────────────────────────────────────────────────────────────

def _translate_delete(op: dict, table: TableMap, smap: SchemaMap) -> SQLStatement:
    params = {}
    counter = [0]

    def _p(value):
        counter[0] += 1
        name = f"p{counter[0]}"
        params[name] = value
        return f":{name}"

    wheres = []
    for field_name, value in (op.get("filters") or {}).items():
        wheres.append(f"{field_name} = {_p(value)}")

    sql = f"DELETE FROM {table.table_name}"
    if wheres:
        sql += " WHERE " + " AND ".join(wheres)
    sql += " RETURNING *"

    return SQLStatement(sql=sql, params=params, is_write=True)


# ── Execution helper ─────────────────────────────────────────────────────────

def execute_sif(
    operations: list[dict],
    schema_map: SchemaMap,
    db_config,
    verbose: bool = False,
) -> tuple[bool, str, list[dict]]:
    """Translate and execute SIF operations. Returns (success, result_text, query_log).

    Validates operations against the schema map first. If validation fails,
    returns actionable error messages (no DB call).
    Each valid operation is translated deterministically and executed in order.
    """
    from agentcore.database import execute_query

    query_log = []
    all_results = []

    # Validate before doing anything
    validation_errors = validate_operations(operations, schema_map)
    if validation_errors:
        error_text = "SIF validation failed:\n" + "\n".join(f"- {e}" for e in validation_errors)
        return False, error_text, query_log

    for op in operations:
        # Action ops dispatch to registered Python functions
        if op.get("op") == "action":
            action_name = op.get("action", "")
            action_entry = _action_registry.get(action_name)
            if not action_entry:
                return False, f"Unknown action: {action_name}", query_log

            if verbose:
                print(f"    [SIF ACTION] {action_name}")

            try:
                action_result = action_entry["fn"](op.get("params", {}), db_config, schema_map)
            except Exception as e:
                return False, f"Action error ({action_name}): {e}", query_log

            query_log.append({"type": "action", "action": action_name, "params": op.get("params", {}), "result": action_result})
            all_results.append({"operation": "action", "entity": action_name, "result": action_result})
            continue

        # CRUD ops translate to SQL
        try:
            stmt = translate(op, schema_map)
        except TranslationError as e:
            return False, f"Translation error: {e}", query_log

        if verbose:
            short = " ".join(stmt.sql.split())
            label = "WRITE" if stmt.is_write else "READ"
            print(f"    [SIF {label}] {short[:120]}{'...' if len(short) > 120 else ''}")

        result = execute_query(db_config, stmt.sql, stmt.is_write, params=stmt.params)
        query_log.append({
            "type": "sif",
            "operation": op,
            "sql": stmt.sql,
            "params": stmt.params,
            "is_write": stmt.is_write,
            "result": result,
        })

        if isinstance(result, dict) and "error" in result:
            return False, f"Database error: {result['error']}", query_log

        all_results.append({"operation": op["op"], "entity": op.get("entity", ""), "result": result})

    # Format results as text for the conversation agent
    result_text = _format_results(all_results)
    return True, result_text, query_log


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
