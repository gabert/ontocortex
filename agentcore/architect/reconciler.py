"""Schema Reconciler — Phase 3 of the scaled architect pipeline.

Pure Python. No LLM call.

Takes:
  - a deterministic schema plan (from the Planner, Phase 1)
  - one Builder output file per module (from Phase 2, one parallel LLM
    call per module, each emitting only data-column detail)
  - the ontology model (for value sets and datatype-property metadata)

Returns a merged logical schema in the same shape that
`agentcore/schema.py` already consumes. The legacy architect's
`render_ddl()` and runtime code work against this output unchanged.

Merge responsibilities, in order:

  1. Identity    — take each table's name/PK/IRI/comment from the plan.
  2. Columns     — append data columns from the matching module build.
  3. Foreign keys — inject the pinned FK columns from plan relationships.
  4. Junctions   — add junction tables from plan relationships.
  5. Lookups     — generate lookup tables from ontology value sets.
  6. Value-set patching — wire value-set columns to their lookup tables.
  7. Creation order — flatten plan levels, prepend lookups, append junctions.
  8. Validate    — reject structural errors before writing.

See NOTES_builder_contract.md for the full Builder/Reconciler contract.
"""

from __future__ import annotations

import json
from pathlib import Path

from agentcore.architect.planner import PLAN_SCHEMA_VERSION as _PLAN_SCHEMA_VERSION
from agentcore.architect.planner import ontology_hash
from agentcore.domain import DomainConfig
from agentcore.domain.ontology import to_snake_case, to_table_name

# Structural version of the emitted schema.json. Bump whenever the output
# shape changes so downstream tools can detect stale artifacts.
SCHEMA_VERSION = 1

# Logical types the downstream schema.py understands. Anything outside
# this set is a Builder bug and must be rejected.
_LOGICAL_TYPES = frozenset({
    "string", "text", "integer", "decimal", "boolean", "date", "datetime",
})

# Relative path (under domain.generated_dir) where per-module Builder
# outputs live. The Builder writes `module_<name>.json` here.
BUILDS_SUBDIR = "_builds"


class ReconcileError(ValueError):
    """Raised when the Reconciler cannot merge plan + module builds into
    a valid logical schema. The message is human-actionable — it names
    the offending module, table, column, or FK so the user knows where
    to look."""


# ── Helpers ──────────────────────────────────────────────────────────────────

def _local_from_iri(iri: str) -> str:
    return iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def _prefix_for_iri(iri: str, namespaces: dict[str, str]) -> str:
    for prefix, ns_uri in namespaces.items():
        if iri.startswith(ns_uri):
            return prefix
    return ""


def _datatype_props_by_table(
    model: dict,
    tables_by_iri: dict[str, dict],
) -> dict[str, list[dict]]:
    """Index the ontology's datatype properties by the tables they apply to.

    A property with a union domain appears on every table in the union.
    The keys are plan-side table names (not IRIs) so Reconciler validation
    can look columns up by the name the Builder emits.
    """
    by_table: dict[str, list[dict]] = {}
    for prop in model["datatype_properties"]:
        for class_iri in prop.get("domain_iris") or []:
            table = tables_by_iri.get(class_iri)
            if not table:
                continue
            by_table.setdefault(table["name"], []).append(prop)
    return by_table


# ── Module build loading ─────────────────────────────────────────────────────

def _load_module_builds(plan: dict, builds_dir: Path) -> dict[str, dict]:
    """Load every `module_*.json` build file referenced by the plan.

    Fails fast with a list of missing modules rather than silently merging
    a partial set — see §6 of NOTES_builder_contract.md.
    """
    if not builds_dir.exists():
        raise ReconcileError(
            f"Builds directory not found: {builds_dir}. Run the Builder first."
        )

    module_names = [m["name"] for m in plan["modules"]]
    loaded: dict[str, dict] = {}
    missing: list[str] = []

    for name in module_names:
        path = builds_dir / f"module_{name}.json"
        if not path.exists():
            missing.append(name)
            continue
        try:
            loaded[name] = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            raise ReconcileError(
                f"Module build '{path.name}' is not valid JSON: {e}"
            ) from e

    if missing:
        raise ReconcileError(
            f"Missing build files for {len(missing)} module(s): {missing}. "
            "The Reconciler refuses to merge a partial set — re-run the Builder "
            "for the missing modules."
        )

    return loaded


# ── Step 1–2: Entity tables from plan + module columns ──────────────────────

def _build_entity_tables(
    plan: dict,
    module_builds: dict[str, dict],
    datatype_props_by_table: dict[str, list[dict]],
) -> dict[str, dict]:
    """Assemble every non-junction table with its identity (from the
    plan) and its data columns (from the module build). Validates that
    every emitted column corresponds to a real datatype property.
    """
    module_for_table: dict[str, str] = {}
    for m in plan["modules"]:
        for t in m.get("tables") or []:
            module_for_table[t] = m["name"]

    tables_out: dict[str, dict] = {}
    for table in plan["tables"]:
        name = table["name"]
        module_name = module_for_table.get(name)
        if module_name is None:
            raise ReconcileError(
                f"Table '{name}' is not assigned to any module in the plan"
            )
        build = module_builds[module_name]
        build_tables = {bt["name"]: bt for bt in build.get("tables") or []}

        if name not in build_tables:
            raise ReconcileError(
                f"Module '{module_name}' build output is missing table '{name}'. "
                f"Expected tables: {sorted(t for t in module_for_table if module_for_table[t] == module_name)}"
            )

        raw_columns = build_tables[name].get("columns") or []
        accepted = {p["snake_name"] for p in datatype_props_by_table.get(name, [])}

        validated_cols: list[dict] = []
        for col in raw_columns:
            if "name" not in col or "type" not in col:
                raise ReconcileError(
                    f"Module '{module_name}', table '{name}': column missing name/type: {col!r}"
                )
            if col["type"] not in _LOGICAL_TYPES:
                raise ReconcileError(
                    f"Table '{name}', column '{col['name']}': unknown logical type "
                    f"'{col['type']}' (expected one of {sorted(_LOGICAL_TYPES)})"
                )
            if col["name"] not in accepted:
                raise ReconcileError(
                    f"Table '{name}', column '{col['name']}' is not in the ontology slice. "
                    f"Accepted column names for this table: {sorted(accepted)}. "
                    "The Builder likely hallucinated a column — re-run or fix the prompt."
                )
            validated_cols.append(col)

        tables_out[name] = {
            "name": name,
            "comment": table.get("comment", ""),
            "ontology_iri": table["ontology_iri"],
            "primary_key": table["primary_key"],
            "columns": validated_cols,
            "foreign_keys": [],
        }

    # Detect tables the Builder emitted that aren't in the plan (prompt drift).
    for module_name, build in module_builds.items():
        plan_tables = {
            t for t, m in module_for_table.items() if m == module_name
        }
        for bt in build.get("tables") or []:
            if bt["name"] not in plan_tables:
                raise ReconcileError(
                    f"Module '{module_name}' build output contains table "
                    f"'{bt['name']}' which is not declared in the plan for this module."
                )

    return tables_out


# ── Step 3: Inject pinned FK columns ─────────────────────────────────────────

def _inject_foreign_keys(entity_tables: dict[str, dict], plan: dict) -> None:
    """Append FK column records to each child table from the plan's FK
    relationships. Contract §4: every FK is not_null + required.

    Also enforces that no FK column name collides with a data column
    that the Builder already emitted for the same table.
    """
    for rel in plan["relationships"]:
        if rel["kind"] != "fk":
            continue
        child_name = rel["child_table"]
        parent_name = rel["parent_table"]
        child = entity_tables.get(child_name)
        parent = entity_tables.get(parent_name)
        if child is None or parent is None:
            raise ReconcileError(
                f"FK relationship {rel['iri']} references unknown table(s): "
                f"child={child_name}, parent={parent_name}"
            )

        fk_col = rel["fk_column"]
        existing_col_names = {c["name"] for c in child["columns"]}
        if fk_col in existing_col_names:
            raise ReconcileError(
                f"Table '{child_name}': FK column '{fk_col}' collides with a "
                "data column the Builder emitted. Rename the datatype property "
                "in the ontology or split the relationship."
            )

        child["foreign_keys"].append({
            "column": fk_col,
            "references_table": parent_name,
            "references_column": parent["primary_key"],
            "not_null": True,
            "required": True,
        })


# ── Step 4: Junction tables ──────────────────────────────────────────────────

def _build_junction_tables(plan: dict, entity_tables: dict[str, dict]) -> list[dict]:
    """One table per junction relationship in the plan. Two integer FKs,
    no data columns, auto-integer surrogate PK. schema.py treats any
    table with ≥2 FKs and no columns as a junction at DDL time.
    """
    junctions: list[dict] = []
    for rel in plan["relationships"]:
        if rel["kind"] != "junction":
            continue
        a_name = rel["endpoint_a"]
        b_name = rel["endpoint_b"]
        a = entity_tables.get(a_name)
        b = entity_tables.get(b_name)
        if a is None or b is None:
            raise ReconcileError(
                f"Junction relationship {rel['iri']} references unknown endpoint(s): "
                f"a={a_name}, b={b_name}"
            )

        junction_name = rel["junction_table"]
        junctions.append({
            "name": junction_name,
            "comment": f"Associates {a_name} with {b_name}.",
            "primary_key": f"{junction_name}_id",
            "columns": [],
            "foreign_keys": [
                {
                    "column": a["primary_key"],
                    "references_table": a_name,
                    "references_column": a["primary_key"],
                    "not_null": True,
                    "required": True,
                },
                {
                    "column": b["primary_key"],
                    "references_table": b_name,
                    "references_column": b["primary_key"],
                    "not_null": True,
                    "required": True,
                },
            ],
        })
    return junctions


# ── Step 5–6: Lookup tables + value-set column patching ──────────────────────

def _build_lookup_tables_and_patch(
    entity_tables: dict[str, dict],
    model: dict,
    tables_by_iri: dict[str, dict],
) -> list[dict]:
    """Generate a lookup table for every `owl:oneOf` value set and patch
    every datatype-property column whose range is a value set with a
    `references` + `allowed_values` pair. Ported (pure-Python) from the
    legacy `architect._inject_lookup_tables`, which was the one piece of
    the old architect worth keeping.
    """
    value_sets = model.get("value_sets") or []
    if not value_sets:
        return []

    namespaces = model.get("namespaces", {})

    lookup_tables: list[dict] = []
    lookup_name_by_iri: dict[str, str] = {}
    codes_by_iri: dict[str, list[str]] = {}

    for vs in value_sets:
        prefix = _prefix_for_iri(vs["iri"], namespaces) or vs["qname"].split(":", 1)[0]
        plural = to_table_name(vs["local_name"])
        table_name = f"{prefix}_{plural}" if prefix else plural
        lookup_name_by_iri[vs["iri"]] = table_name

        codes = [_local_from_iri(m) for m in vs["members"]]
        codes_by_iri[vs["iri"]] = codes

        lookup_tables.append({
            "name": table_name,
            "comment": vs["comment"] or f"Lookup table for {vs['qname']}",
            "ontology_iri": vs["iri"],
            "lookup_table": True,
            "primary_key": "code",
            "primary_key_type": "text",
            "columns": [
                {"name": "label",      "type": "string",  "not_null": True},
                {"name": "sort_order", "type": "integer", "not_null": True, "default": 0},
                {"name": "active",     "type": "boolean", "not_null": True, "default": True},
            ],
            "foreign_keys": [],
        })

    # Patch entity columns whose datatype property targets a value set.
    value_set_iris = {vs["iri"] for vs in value_sets}
    for dp in model["datatype_properties"]:
        rng = dp.get("range_iri")
        if rng not in value_set_iris:
            continue
        lookup_name = lookup_name_by_iri[rng]
        codes = codes_by_iri[rng]
        snake = dp["snake_name"]

        for class_iri in dp.get("domain_iris") or []:
            tbl = tables_by_iri.get(class_iri)
            if not tbl:
                continue
            entity = entity_tables.get(tbl["name"])
            if entity is None:
                continue
            for col in entity["columns"]:
                if col["name"] == snake:
                    col["references"] = {"table": lookup_name, "column": "code"}
                    col["allowed_values"] = list(codes)
                    break

    return lookup_tables


# ── Step 7: Creation order ───────────────────────────────────────────────────

def _build_creation_order(
    plan: dict,
    lookup_tables: list[dict],
    junction_tables: list[dict],
) -> list[str]:
    """Flatten the plan's layered creation_order into a single list,
    prepended with lookup tables and appended with junction tables.
    schema.py's render_ddl consumes a flat list of table names.
    """
    plan_levels = plan.get("creation_order") or []
    if not plan_levels:
        raise ReconcileError(
            "Plan is missing creation_order. The Reconciler cannot merge a "
            "plan that failed validation (cycles or other errors in the ontology)."
        )

    flat: list[str] = []
    for level in plan_levels:
        flat.extend(level.get("tables") or [])

    return (
        [t["name"] for t in lookup_tables]
        + flat
        + [j["name"] for j in junction_tables]
    )


# ── Step 8: Final validation ─────────────────────────────────────────────────

def _validate_merged_schema(schema: dict) -> None:
    """Structural check on the merged output before we write it to disk.

    Overlaps slightly with `architect._validate_schema` from the old
    architect — kept here so the Reconciler is self-contained.
    """
    names = {t["name"] for t in schema["tables"]}

    order_missing = [n for n in schema["creation_order"] if n not in names]
    if order_missing:
        raise ReconcileError(
            f"creation_order references unknown tables: {order_missing}"
        )

    order_extra = names - set(schema["creation_order"])
    if order_extra:
        raise ReconcileError(
            f"Tables not in creation_order: {sorted(order_extra)}"
        )

    for t in schema["tables"]:
        col_names = [c["name"] for c in t.get("columns") or []]
        if len(col_names) != len(set(col_names)):
            dupes = sorted({n for n in col_names if col_names.count(n) > 1})
            raise ReconcileError(
                f"Table '{t['name']}': duplicate column names {dupes}"
            )
        for col in t.get("columns") or []:
            if col["type"] not in _LOGICAL_TYPES:
                raise ReconcileError(
                    f"Table '{t['name']}', column '{col['name']}': "
                    f"unknown logical type '{col['type']}'"
                )
            ref = col.get("references")
            if ref and ref.get("table") not in names:
                raise ReconcileError(
                    f"Table '{t['name']}', column '{col['name']}': "
                    f"references unknown lookup table '{ref.get('table')}'"
                )
        for fk in t.get("foreign_keys") or []:
            if fk["references_table"] not in names:
                raise ReconcileError(
                    f"Table '{t['name']}': FK to unknown table '{fk['references_table']}'"
                )


# ── Public API ───────────────────────────────────────────────────────────────

def reconcile(
    domain: DomainConfig,
    plan: dict,
    model: dict | None = None,
    builds_dir: Path | None = None,
) -> tuple[dict, str]:
    """Merge a plan + per-module Builder outputs into a logical schema.

    Parameters default to the canonical locations: the ontology model
    from the domain's .ttl, and builds from
    `domain.generated_dir / _builds`. Tests pass explicit values to
    keep things hermetic.

    Returns (schema_dict, schema_filename). The caller updates the
    domain manifest.
    """
    if model is None:
        model = domain.ontology_model
    if builds_dir is None:
        builds_dir = domain.generated_dir / BUILDS_SUBDIR

    plan_version = plan.get("schema_version")
    if plan_version is not None and plan_version != _PLAN_SCHEMA_VERSION:
        raise ReconcileError(
            f"Plan schema_version={plan_version!r} does not match the "
            f"version this reconciler understands ({_PLAN_SCHEMA_VERSION}). "
            "Re-run design_plan to regenerate the plan."
        )
    if not plan.get("valid", True):
        raise ReconcileError(
            "Plan is marked valid: false. Fix the ontology or overrides "
            "and re-run design_plan before reconciling."
        )

    # Detect ontology drift between plan time and reconcile time. If the
    # .ttl has been edited since `design_plan` ran, the Builder ran against
    # a stale slice and any errors the Reconciler raises next would be
    # surfaced at confusing locations. Bail out loudly instead.
    plan_hash = plan.get("ontology_hash")
    if plan_hash:
        current_hash = ontology_hash(domain.ontology_text)
        if current_hash != plan_hash:
            raise ReconcileError(
                f"Ontology has changed since the plan was built.\n"
                f"    Plan hash    : {plan_hash[:16]}...\n"
                f"    Current hash : {current_hash[:16]}...\n"
                f"    Source       : {domain.ontology_path}\n"
                f"Re-run: python scripts/design_plan.py {domain.dir_name} "
                f"&& python scripts/build_schema.py {domain.dir_name}"
            )

    module_builds = _load_module_builds(plan, builds_dir)

    tables_by_iri = {t["ontology_iri"]: t for t in plan["tables"]}
    datatype_props_by_table = _datatype_props_by_table(model, tables_by_iri)

    entity_tables = _build_entity_tables(plan, module_builds, datatype_props_by_table)
    _inject_foreign_keys(entity_tables, plan)
    junction_tables = _build_junction_tables(plan, entity_tables)
    lookup_tables = _build_lookup_tables_and_patch(
        entity_tables, model, tables_by_iri
    )

    creation_order = _build_creation_order(plan, lookup_tables, junction_tables)

    schema: dict = {
        "schema_version": SCHEMA_VERSION,
        "tables": lookup_tables + list(entity_tables.values()) + junction_tables,
        "creation_order": creation_order,
    }

    _validate_merged_schema(schema)

    schema_file = f"{domain.dir_name}_schema.json"
    domain.generated_dir.mkdir(exist_ok=True)
    schema_path = domain.generated_dir / schema_file
    schema_path.write_text(
        json.dumps(schema, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"  Saved _generated/{schema_path.name}")
    return schema, schema_file
