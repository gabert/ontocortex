"""Schema Planner — Phase 1 of the scaled architect pipeline.

Deterministic, pure Python. Given an ontology model, emit a topology-only
schema plan: entity tables, FK relationships (with direction pinned),
junction tables, and module groupings.

No LLM call. The hard judgment calls — FK direction on ambiguous
object properties, junction-vs-FK, module boundaries — can be resolved
via an optional `schema_overrides.yaml` in the domain directory.

Output shape matches the Builder/Reconciler contract; see
NOTES_scaled_architect.md for the pipeline overview.
"""

from __future__ import annotations

import hashlib
import warnings

import yaml

from agentcore.domain import DomainConfig
from agentcore.domain.ontology import to_pk_name, to_snake_case, to_table_name


def ontology_hash(ontology_text: str) -> str:
    """Stable SHA-256 of the ontology source text.

    Used to detect stale plans/builds: the planner stamps this hash
    into the plan YAML; the reconciler re-reads the current .ttl and
    refuses to merge if the hash has drifted. Newlines are normalized
    so a git checkout that converts line endings doesn't invalidate
    an otherwise-identical ontology.
    """
    normalized = ontology_text.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

# Allowed top-level keys in schema_overrides.yaml. Unknown keys fail fast
# instead of silently no-opping — typos like `fk_parents` used to be
# invisible and caused hours of puzzlement.
_ALLOWED_OVERRIDE_KEYS = frozenset({
    "fk_parent",
    "junction_properties",
    "junction_relations",
    "ignore_classes",
    "modules",
})

# rdfs:comment keywords that signal a junction (many-to-many) relationship.
_JUNCTION_KEYWORDS = (
    "many-to-many",
    "many to many",
    "m:n",
    "m-to-n",
    "junction table",
    "join table",
    "must be represented with a junction",
)


def _load_overrides(domain: DomainConfig) -> dict:
    """Load schema_overrides.yaml from the domain directory if present.

    Returns an empty dict if the file doesn't exist. Fails fast on
    unknown top-level keys — silent typos are the #1 override footgun.
    Reference-existence checks against the ontology model happen later
    in `_validate_overrides` once the model is available.
    """
    override_path = domain.ontology_path.parent / "schema_overrides.yaml"
    if not override_path.exists():
        return {}
    data = yaml.safe_load(override_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise PlanValidationError(
            f"{override_path}: top-level must be a mapping, got {type(data).__name__}"
        )
    unknown = set(data) - _ALLOWED_OVERRIDE_KEYS
    if unknown:
        raise PlanValidationError(
            f"{override_path}: unknown top-level keys {sorted(unknown)}. "
            f"Allowed: {sorted(_ALLOWED_OVERRIDE_KEYS)}"
        )
    return data


def _validate_overrides(overrides: dict, model: dict, ontology_path: str = "") -> None:
    """Check that every class/property named in overrides actually exists
    in the ontology model.

    Dead references are a hard error, matching the strictness of
    `_load_overrides` on unknown top-level keys. They are almost
    always typos — failing the pipeline here saves the user from
    debugging phantom "my override is being ignored" issues later.
    """
    if not overrides:
        return

    class_keys: set[str] = set()
    for c in model["classes"]:
        class_keys.update(k for k in _keys_for(c) if k)
    prop_keys: set[str] = set()
    for p in model["object_properties"]:
        prop_keys.update(k for k in _keys_for(p) if k)

    dead: list[str] = []
    for k in (overrides.get("fk_parent") or {}):
        if k not in prop_keys:
            dead.append(f"fk_parent: {k!r}")
    for k in (overrides.get("junction_properties") or []):
        if k not in prop_keys:
            dead.append(f"junction_properties: {k!r}")
    for k in (overrides.get("ignore_classes") or []):
        if k not in class_keys:
            dead.append(f"ignore_classes: {k!r}")
    for _mod_name, entries in (overrides.get("modules") or {}).items():
        for entry in entries:
            if entry not in class_keys:
                dead.append(f"modules: {entry!r}")

    if dead:
        where = f" in {ontology_path}" if ontology_path else ""
        raise PlanValidationError(
            f"schema_overrides.yaml references entities that don't exist"
            f"{where}:\n    " + "\n    ".join(dead)
            + "\n\nDead references are almost always typos — fix them "
              "or remove the stale entries from schema_overrides.yaml."
        )


# ── IRI / name helpers ───────────────────────────────────────────────────────

def _prefix_for(iri: str, namespaces: dict[str, str]) -> str:
    """Find the namespace prefix that matches this IRI (e.g. 'sl', 'ins')."""
    for prefix, ns_uri in namespaces.items():
        if iri.startswith(ns_uri):
            return prefix
    return ""


def _table_name(cls: dict, namespaces: dict[str, str]) -> str:
    prefix = _prefix_for(cls["iri"], namespaces)
    base = to_table_name(cls["local_name"])
    return f"{prefix}_{base}" if prefix else base


def _primary_key(cls: dict, namespaces: dict[str, str]) -> str:
    prefix = _prefix_for(cls["iri"], namespaces)
    base = to_pk_name(cls["local_name"])
    return f"{prefix}_{base}" if prefix else base


def _fk_column_for(parent_cls: dict, namespaces: dict[str, str]) -> str:
    return _primary_key(parent_cls, namespaces)


# ── Override key resolution ──────────────────────────────────────────────────

def _keys_for(entity: dict) -> tuple[str, ...]:
    """Return the three acceptable override keys for a class or property:
    full IRI, qname (`prefix:LocalName`), and bare local_name.

    Override lookups try these in order so the user can pick whichever
    form is least ambiguous for their domain — bare name for single-namespace
    ontologies, qname for multi-namespace, full IRI as an escape hatch.
    """
    return (entity["iri"], entity.get("qname", ""), entity["local_name"])


def _lookup_in_dict(entity: dict, d: dict):
    for k in _keys_for(entity):
        if k and k in d:
            return d[k]
    return None


def _in_list(entity: dict, items: list) -> bool:
    if not items:
        return False
    keys = _keys_for(entity)
    return any(k and k in items for k in keys)


# ── Object-property classification ───────────────────────────────────────────

def _is_junction(prop: dict, overrides: dict) -> bool:
    """Return True if this object property should become a junction table."""
    if _in_list(prop, overrides.get("junction_properties") or []):
        return True
    forced = {tuple(x) for x in overrides.get("junction_relations") or []}
    if (prop.get("domain_iri"), prop.get("range_iri")) in forced:
        return True
    comment = (prop.get("comment") or "").lower()
    return any(k in comment for k in _JUNCTION_KEYWORDS)


def _fk_parent_side(prop: dict, overrides: dict) -> str:
    """Decide which side of an object property is the FK parent.

    Returns 'range' (default — FK lives on the domain/subject side,
    pointing at the range) or 'domain' (FK on the range side).

    The default is correct for reference-style properties
    (hasBorrower, issuedBy). Composition-style properties where the
    range is a dependent child (hasPayment) must be flipped via an
    explicit `fk_parent` override in schema_overrides.yaml.
    """
    explicit = _lookup_in_dict(prop, overrides.get("fk_parent") or {})
    if explicit in ("range", "domain"):
        return explicit
    return "range"


# ── Plan construction ────────────────────────────────────────────────────────

def _build_tables(
    classes: list[dict],
    namespaces: dict[str, str],
    ignore_list: list,
) -> tuple[list[dict], dict[str, dict], dict[str, dict]]:
    """Return (tables, iri → table map, iri → source class map) for
    every non-ignored class. The source-class map lets later stages
    resolve module assignments without re-walking the model.
    """
    tables: list[dict] = []
    by_iri: dict[str, dict] = {}
    cls_by_iri: dict[str, dict] = {}
    for cls in classes:
        if _in_list(cls, ignore_list):
            continue
        t = {
            "name": _table_name(cls, namespaces),
            "ontology_iri": cls["iri"],
            "primary_key": _primary_key(cls, namespaces),
            "comment": cls.get("comment", ""),
        }
        tables.append(t)
        by_iri[cls["iri"]] = t
        cls_by_iri[cls["iri"]] = cls
    return tables, by_iri, cls_by_iri


def _build_relationships(
    object_properties: list[dict],
    tables_by_iri: dict[str, dict],
    namespaces: dict[str, str],
    overrides: dict,
) -> list[dict]:
    rels: list[dict] = []
    used_junction_names: set[str] = set()

    for prop in object_properties:
        # Warn loudly on union domain/range — we drop these rather than
        # fan them out into multiple relationships, because the modeling
        # intent (one FK pointing at which parent?) is undefined.
        if len(prop.get("domain_iris") or []) > 1 or len(prop.get("range_iris") or []) > 1:
            warnings.warn(
                f"Object property {prop['qname']!r} declares an owl:unionOf "
                "domain/range. The planner does not yet expand unions into "
                "multiple relationships — the property is being dropped. "
                "Split it into separate properties in the ontology to keep it.",
                stacklevel=2,
            )
            continue

        domain_iri = prop.get("domain_iri")
        range_iri = prop.get("range_iri")
        if not domain_iri or not range_iri:
            continue
        domain_tbl = tables_by_iri.get(domain_iri)
        range_tbl = tables_by_iri.get(range_iri)
        if not domain_tbl or not range_tbl:
            continue  # points to an ignored class

        if _is_junction(prop, overrides):
            junction_name = _junction_name(
                prop, domain_iri, range_iri, namespaces, used_junction_names
            )
            used_junction_names.add(junction_name)
            rels.append({
                "iri": prop["iri"],
                "kind": "junction",
                "junction_table": junction_name,
                "endpoint_a": domain_tbl["name"],
                "endpoint_b": range_tbl["name"],
            })
            continue

        parent_side = _fk_parent_side(prop, overrides)
        if parent_side == "range":
            child, parent = domain_tbl, range_tbl
            parent_cls_iri = range_iri
        else:
            child, parent = range_tbl, domain_tbl
            parent_cls_iri = domain_iri

        fk_col = _primary_key(
            {"iri": parent_cls_iri, "local_name": _local_name(parent_cls_iri)},
            namespaces,
        )
        rels.append({
            "iri": prop["iri"],
            "kind": "fk",
            "child_table": child["name"],
            "parent_table": parent["name"],
            "fk_column": fk_col,
        })
    return rels


def _junction_name(
    prop: dict,
    domain_iri: str,
    range_iri: str,
    namespaces: dict[str, str],
    used: set[str],
) -> str:
    """Build a deterministic, collision-free junction table name.

    Base form: `{prefix}_{domain_singular}_{range_plural}`. If two M:N
    properties share the same class pair, the second one gets suffixed
    with the property's local_name in snake_case, so both land in the
    plan without silently clobbering each other.
    """
    prefix = _prefix_for(domain_iri, namespaces)
    domain_singular = to_snake_case(_local_name(domain_iri))
    range_plural = to_table_name(_local_name(range_iri))
    base = (
        f"{prefix}_{domain_singular}_{range_plural}"
        if prefix else f"{domain_singular}_{range_plural}"
    )
    if base not in used:
        return base
    suffix = to_snake_case(prop["local_name"])
    return f"{base}_{suffix}"


def _local_name(iri: str) -> str:
    return iri.split("#")[-1].split("/")[-1]


def _build_modules(
    tables: list[dict],
    cls_by_iri: dict[str, dict],
    namespaces: dict[str, str],
    overrides: dict,
) -> list[dict]:
    """Group entity tables into modules.

    v1 heuristic: one module per namespace prefix. Overrides may force
    explicit groupings via `modules: { name: [TableName, ...] }`, where
    each entry matches by physical table name, class local_name, or IRI.
    """
    forced: dict[str, str] = {}
    for mod_name, entries in (overrides.get("modules") or {}).items():
        for entry in entries:
            forced[entry] = mod_name

    grouped: dict[str, list[str]] = {}
    for t in tables:
        cls = cls_by_iri[t["ontology_iri"]]
        module = (
            forced.get(t["name"])
            or forced.get(cls["local_name"])
            or forced.get(cls.get("qname", ""))
            or forced.get(cls["iri"])
            or _prefix_for(cls["iri"], namespaces)
            or "core"
        )
        grouped.setdefault(module, []).append(t["name"])
    return [{"name": m, "tables": sorted(ts)} for m, ts in sorted(grouped.items())]


# ── Cycle detection ──────────────────────────────────────────────────────────

def _detect_cycles(relationships: list[dict], tables: list[dict]) -> list[dict]:
    """Find FK cycles via Tarjan's strongly-connected-components.

    Returns a list of cycle records. Self-references (length-1 SCCs with a
    self-edge) are classified as `kind: self` and treated as informational
    — they're valid modeling (employee.manager_id, parent pointers, etc.).
    Length-≥2 cycles are `kind: cycle` and block plan validation.

    Each cycle record includes the path so a reviewer can eyeball the fix.

    Implementation note: Tarjan's SCC is written iteratively with an
    explicit work stack so deep FK chains can't blow Python's recursion
    limit — the pipeline is designed for up to ~1000 tables.
    """
    graph: dict[str, list[tuple[str, str, str]]] = {t["name"]: [] for t in tables}
    for r in relationships:
        if r["kind"] == "fk":
            graph[r["child_table"]].append(
                (r["parent_table"], r["iri"], r["fk_column"])
            )

    index_counter = 0
    stack: list[str] = []
    index: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_stack: dict[str, bool] = {}
    sccs: list[list[str]] = []

    def _init_node(v: str) -> None:
        nonlocal index_counter
        index[v] = index_counter
        lowlink[v] = index_counter
        index_counter += 1
        stack.append(v)
        on_stack[v] = True

    def strongconnect(start: str) -> None:
        """Iterative Tarjan: each frame holds (node, successor_iterator).

        When we hit an unvisited successor we push a new frame and break
        out of the inner loop. When a frame's iterator is exhausted we
        pop it, close out its SCC if it's a root, and then propagate its
        lowlink up to the parent — the same step the recursive version
        performs right after `strongconnect(w)` returns.
        """
        _init_node(start)
        work: list[tuple[str, object]] = [(start, iter(graph.get(start, [])))]
        while work:
            v, it = work[-1]
            pushed = False
            for (w, _iri, _col) in it:  # type: ignore[assignment]
                if w not in index:
                    _init_node(w)
                    work.append((w, iter(graph.get(w, []))))
                    pushed = True
                    break
                if on_stack.get(w):
                    lowlink[v] = min(lowlink[v], index[w])
            if pushed:
                continue

            work.pop()
            if lowlink[v] == index[v]:
                component: list[str] = []
                while True:
                    w = stack.pop()
                    on_stack[w] = False
                    component.append(w)
                    if w == v:
                        break
                sccs.append(component)
            if work:
                parent = work[-1][0]
                lowlink[parent] = min(lowlink[parent], lowlink[v])

    for v in list(graph.keys()):
        if v not in index:
            strongconnect(v)

    cycles: list[dict] = []
    for comp in sccs:
        if len(comp) == 1:
            v = comp[0]
            self_edges = [
                {"via": col, "property": iri}
                for (w, iri, col) in graph[v] if w == v
            ]
            if self_edges:
                cycles.append({
                    "kind": "self",
                    "severity": "info",
                    "table": v,
                    "edges": self_edges,
                })
            continue

        comp_set = set(comp)
        path = _walk_cycle(comp[0], graph, comp_set)
        cycles.append({
            "kind": "cycle",
            "severity": "error",
            "length": len(comp),
            "tables": sorted(comp),
            "path": path,
            "hint": _cycle_hint(path),
        })

    return cycles


def _walk_cycle(
    start: str,
    graph: dict[str, list[tuple[str, str, str]]],
    scc: set[str],
) -> list[dict]:
    """DFS within an SCC until we loop back to `start`, recording edges.

    Each edge is a dict with `from`, `to`, `fk_column`, and `property`
    (the OWL object property IRI) so downstream error messages can
    point the user at the exact ontology entries to inspect.

    Iterative to match the iterative Tarjan above — a deep SCC must
    not blow the recursion limit while we're trying to explain it.
    """
    path: list[dict] = []
    visited: set[str] = {start}
    # Each frame is (node, successor_iterator). The edge leading into a
    # non-root frame is the current `path[-1]`; popping the frame and
    # popping `path` are paired so `path` stays in sync with `work`.
    work: list[tuple[str, object]] = [(start, iter(graph[start]))]

    while work:
        v, it = work[-1]
        descended = False
        for (w, iri, col) in it:  # type: ignore[assignment]
            if w not in scc:
                continue
            path.append({"from": v, "to": w, "fk_column": col, "property": iri})
            if w == start:
                return path
            if w in visited:
                path.pop()
                continue
            visited.add(w)
            work.append((w, iter(graph[w])))
            descended = True
            break
        if descended:
            continue

        work.pop()
        if work and path:
            # Drop the descent edge that brought us into this frame so
            # the parent can try its next successor cleanly.
            path.pop()

    return path


def _cycle_hint(path: list[dict]) -> str:
    """Resolution suggestions for a multi-table FK cycle.

    All cycles are ontology modeling errors — the root cause is two or
    more object properties whose domain/range together form a loop.
    The hint always starts with "fix the ontology" because that's the
    90%-correct answer; overrides are a second-resort workaround.
    """
    props = ", ".join(sorted({e["property"] for e in path}))
    if len(path) == 2:
        return (
            f"Root cause: two object properties ({props}) declare "
            "domain/range in opposite directions, creating a deadlock. "
            "Most likely fix: edit the ontology .ttl and delete whichever "
            "property is redundant - usually one side can be derived from "
            "the other (a boolean flag on the child, or an ORDER BY query). "
            "If both are genuinely needed, alternatives are: move one to a "
            "side/designator class, flip one direction via fk_parent "
            "override, or make one FK nullable in post-processing."
        )
    return (
        f"Root cause: {len(path)} object properties ({props}) chain "
        "together into a loop. Most likely fix: one of them has the "
        "wrong rdfs:domain/rdfs:range direction in the ontology - find "
        "and correct it in the .ttl. If the cycle is semantically real, "
        "break it with a side class or a nullable FK."
    )


# ── Topological sort ─────────────────────────────────────────────────────────

def _creation_order(relationships: list[dict], tables: list[dict]) -> list[dict]:
    """Kahn's algorithm in layered form.

    Returns a list of levels; every table in level N depends only on
    tables in levels < N, so each level is mutually independent and
    can be created (or seeded) in parallel.

    Self-edges are ignored — a self-FK resolves to "insert with NULL,
    UPDATE later", not a hard dependency on the same row.

    Junction tables are not included; they live outside `tables` and
    will be appended at the end by the Reconciler (they depend on both
    endpoints, so they always belong to the final level).
    """
    table_names = {t["name"] for t in tables}
    deps: dict[str, set[str]] = {n: set() for n in table_names}

    for r in relationships:
        if r["kind"] != "fk":
            continue
        child, parent = r["child_table"], r["parent_table"]
        if child == parent:
            continue  # self-reference, handled at insert time
        if child in deps and parent in table_names:
            deps[child].add(parent)

    levels: list[dict] = []
    placed: set[str] = set()
    remaining = dict(deps)

    while remaining:
        ready = sorted(n for n, d in remaining.items() if d.issubset(placed))
        if not ready:
            # Cycle prevents further progress; caller already reports cycles.
            break
        levels.append({"level": len(levels), "tables": ready})
        placed.update(ready)
        for n in ready:
            del remaining[n]

    return levels


# ── Validation ───────────────────────────────────────────────────────────────

class PlanValidationError(ValueError):
    """Raised when a plan fails structural or ontology-coverage checks."""


def _validate_plan(plan: dict, model: dict, ignore_list: list, ontology_path: str = "") -> None:
    errors: list[str] = []

    class_iris = {
        c["iri"] for c in model["classes"]
        if not _in_list(c, ignore_list)
    }
    table_iris = [t["ontology_iri"] for t in plan["tables"]]
    table_iri_set = set(table_iris)

    missing = class_iris - table_iri_set
    if missing:
        errors.append(f"Plan missing tables for {len(missing)} classes: {sorted(missing)[:5]}")

    if len(table_iris) != len(table_iri_set):
        errors.append("Plan has duplicate table IRIs")

    names = [t["name"] for t in plan["tables"]]
    if len(names) != len(set(names)):
        dupes = sorted({n for n in names if names.count(n) > 1})
        errors.append(f"Plan has duplicate table names: {dupes}")

    # Primary-key collisions across tables (two classes snake-casing to
    # the same singular stem produce the same PK name and break DDL).
    pk_names = [t["primary_key"] for t in plan["tables"]]
    if len(pk_names) != len(set(pk_names)):
        dupes = sorted({n for n in pk_names if pk_names.count(n) > 1})
        errors.append(f"Plan has duplicate primary-key names: {dupes}")

    # Junction-table name collisions against entity tables.
    entity_names = set(names)
    for r in plan["relationships"]:
        if r["kind"] == "junction" and r["junction_table"] in entity_names:
            errors.append(
                f"Junction table '{r['junction_table']}' collides with an entity table name"
            )

    # FK-column collisions on the same child table (two object properties
    # targeting the same parent class produce the same fk_column, which
    # SQL can't represent as two distinct FKs).
    fk_cols_by_child: dict[str, dict[str, list[str]]] = {}
    for r in plan["relationships"]:
        if r["kind"] != "fk":
            continue
        child_map = fk_cols_by_child.setdefault(r["child_table"], {})
        child_map.setdefault(r["fk_column"], []).append(r["iri"])
    for child, col_map in fk_cols_by_child.items():
        for col, iris in col_map.items():
            if len(iris) > 1:
                errors.append(
                    f"Child table '{child}': FK column '{col}' is claimed by "
                    f"{len(iris)} object properties ({sorted(iris)}). Two FKs to "
                    "the same parent need distinct column names — split the "
                    "relationship or rename one property."
                )

    # Module coverage
    module_tables: list[str] = []
    for m in plan["modules"]:
        module_tables.extend(m.get("tables") or [])
    missing_from_modules = set(names) - set(module_tables)
    if missing_from_modules:
        errors.append(f"Tables not assigned to any module: {sorted(missing_from_modules)}")

    # Relationship endpoints must be real tables
    for r in plan["relationships"]:
        if r["kind"] == "fk":
            for side in ("child_table", "parent_table"):
                if r[side] not in names:
                    errors.append(f"Relationship {r['iri']}: {side} '{r[side]}' not declared")
        elif r["kind"] == "junction":
            for side in ("endpoint_a", "endpoint_b"):
                if r[side] not in names:
                    errors.append(f"Relationship {r['iri']}: {side} '{r[side]}' not declared")

    # Every non-ignored object property should appear — except those
    # dropped because of unsupported union domain/range (warned already).
    def _is_expandable(p: dict) -> bool:
        if len(p.get("domain_iris") or []) > 1 or len(p.get("range_iris") or []) > 1:
            return False
        return p.get("domain_iri") in class_iris and p.get("range_iri") in class_iris

    op_iris = {p["iri"] for p in model["object_properties"] if _is_expandable(p)}
    rel_iris = {r["iri"] for r in plan["relationships"]}
    missing_rels = op_iris - rel_iris
    if missing_rels:
        errors.append(f"Plan missing relationships for: {sorted(missing_rels)[:5]}")

    # FK cycles — ontology modeling errors. Self-references are informational.
    real_cycles = [c for c in (plan.get("cycles") or []) if c.get("severity") == "error"]
    for c in real_cycles:
        path_str = " -> ".join(
            f"{e['from']}.{e['fk_column']} -> {e['to']}" for e in c["path"]
        )
        properties = sorted({e["property"] for e in c["path"]})
        errors.append(
            "ONTOLOGY ERROR - FK cycle detected.\n"
            f"    Tables involved : {c['tables']}\n"
            f"    Cycle path      : {path_str}\n"
            f"    OWL properties  : {properties}\n"
            + (f"    Source ontology : {ontology_path}\n" if ontology_path else "")
            + f"    Resolution      : {c['hint']}"
        )

    if errors:
        header = ""
        if real_cycles:
            header = (
                "Plan validation failed. The issues below are modeling errors "
                "in the ontology itself, not planner bugs. Edit the .ttl file "
                "(or schema_overrides.yaml as a last resort) and re-run.\n\n"
            )
        raise PlanValidationError(header + "\n".join(f"- {e}" for e in errors))


# ── Public API ───────────────────────────────────────────────────────────────

# Structural version of the plan YAML. Bump whenever the shape changes
# so downstream tools can detect stale artifacts instead of misparsing.
PLAN_SCHEMA_VERSION = 1


def design_plan(domain: DomainConfig) -> tuple[dict, str]:
    """Build a deterministic schema plan for the given domain.

    Returns (plan_dict, plan_filename). The caller updates the manifest.
    """
    model = domain.ontology_model
    overrides = _load_overrides(domain)
    _validate_overrides(overrides, model, str(domain.ontology_path))
    ignore_list = overrides.get("ignore_classes") or []
    namespaces = model["namespaces"]

    tables, by_iri, cls_by_iri = _build_tables(model["classes"], namespaces, ignore_list)
    relationships = _build_relationships(
        model["object_properties"], by_iri, namespaces, overrides
    )
    modules = _build_modules(tables, cls_by_iri, namespaces, overrides)
    cycles = _detect_cycles(relationships, tables)

    plan: dict = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "ontology_hash": ontology_hash(domain.ontology_text),
        "valid": True,  # flipped to False if validation fails below
        "modules": modules,
        "tables": tables,
        "relationships": relationships,
    }
    if cycles:
        plan["cycles"] = cycles
    # Topological sort only runs when there are no real cycles — otherwise
    # the result is incomplete and misleading. Self-references are fine.
    if not any(c.get("severity") == "error" for c in cycles):
        plan["creation_order"] = _creation_order(relationships, tables)

    plan_file = f"{domain.dir_name}_schema_plan.yaml"
    domain.generated_dir.mkdir(exist_ok=True)
    plan_path = domain.generated_dir / plan_file

    def _write(p: dict) -> None:
        plan_path.write_text(
            yaml.safe_dump(p, sort_keys=False, allow_unicode=True, width=120),
            encoding="utf-8",
            newline="\n",
        )

    # Write once speculatively so a reviewer can eyeball cycle paths and
    # other diagnostics even when validation later fails. If validation
    # passes, the file stays valid: true. If it fails, we rewrite with
    # valid: false + the error list before re-raising.
    _write(plan)
    print(f"  Saved _generated/{plan_path.name}")

    try:
        _validate_plan(plan, model, ignore_list, str(domain.ontology_path))
    except PlanValidationError as e:
        plan["valid"] = False
        plan["errors"] = str(e).splitlines()
        _write(plan)
        raise

    return plan, plan_file
