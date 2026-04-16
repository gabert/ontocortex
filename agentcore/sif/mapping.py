"""Mapping between ontology vocabulary and physical database schema.

Two entry points:

  build_schema_map_from_mapping(ontology, mapping_dict)
      → SchemaMap from a mapping dict (human-authored or auto-generated).

  generate_mapping_from_schema(ontology, schema_json)
      → mapping dict derived from schema.json (identity column mapping).

The architect pipeline generates a mapping.yaml alongside schema.json so
that SchemaMap is always built from the mapping — whether the schema was
designed by the architect or pre-existing.
"""

from agentcore.sif.schema_map import (
    JoinStep,
    RelationMap,
    SchemaMap,
    TableMap,
)


class MappingError(Exception):
    """Raised when a mapping file is invalid."""


def build_schema_map_from_mapping(
    ontology_model: dict, mapping: dict,
) -> SchemaMap:
    """Build a SchemaMap from an ontology model + mapping dict.

    The mapping dict has two top-level keys:

      tables:
        ClassName:
          iri: "https://..."           # ontology class IRI
          table: physical_table_name
          primary_key: physical_pk_column
          columns:
            ontology_prop:
              iri: "https://..."       # datatype property IRI
              column: physical_column
            ...

      relations:
        relationName:
          iri: "https://..."           # object property IRI
          type: direct | junction
          ...

    Column values may also be plain strings (``prop: physical_col``)
    for backward compatibility; the ``iri`` is then resolved from the
    ontology model by name.

    Returns a fully populated SchemaMap with column_map set on each
    TableMap and fk_index populated from the relation definitions.
    """
    classes_by_local = {c["local_name"]: c for c in ontology_model["classes"]}
    props_by_local = {
        p["local_name"]: p for p in ontology_model.get("object_properties", [])
    }

    smap = object.__new__(SchemaMap)
    smap.tables = {}
    smap.tables_by_iri = {}
    smap.relations = {}
    smap.junction_tables = set()
    smap.fk_index = {}
    smap._schema = {"tables": []}  # empty — not used for mapping path

    # ── Tables ───────────────────────────────────────────────────────────
    table_defs = mapping.get("tables") or {}
    for class_name, tdef in table_defs.items():
        # Resolve class — prefer IRI from mapping, fall back to name lookup.
        class_iri = tdef.get("iri")
        cls = classes_by_local.get(class_name)
        if cls is None:
            raise MappingError(
                f"Mapping references class '{class_name}' which is not in the ontology. "
                f"Available: {', '.join(sorted(classes_by_local))}"
            )
        if class_iri and class_iri != cls["iri"]:
            raise MappingError(
                f"Class '{class_name}' IRI mismatch: mapping says '{class_iri}', "
                f"ontology says '{cls['iri']}'"
            )

        table_name = tdef.get("table")
        primary_key = tdef.get("primary_key")
        if not table_name or not primary_key:
            raise MappingError(
                f"Table mapping for '{class_name}' must have 'table' and 'primary_key'"
            )

        col_defs = tdef.get("columns") or {}
        columns: list[str] = []
        column_map: dict[str, str] = {}
        for prop_name, cdef in col_defs.items():
            columns.append(prop_name)
            if isinstance(cdef, str):
                # Simple format: ontology_prop: physical_col
                column_map[prop_name] = cdef
            else:
                # Rich format: ontology_prop: {iri: ..., column: ...}
                column_map[prop_name] = cdef["column"]

        tmap = TableMap(
            class_name=class_name,
            class_iri=cls["iri"],
            table_name=table_name,
            primary_key=primary_key,
            columns=columns,
            column_map=column_map,
            comment=cls.get("comment") or "",
        )
        smap.tables[class_name] = tmap
        smap.tables_by_iri[cls["iri"]] = tmap

    # ── Relations ────────────────────────────────────────────────────────
    rel_defs = mapping.get("relations") or {}
    for rel_name, rdef in rel_defs.items():
        prop = props_by_local.get(rel_name)
        if not prop:
            raise MappingError(
                f"Mapping references relation '{rel_name}' which is not an "
                f"object property in the ontology. "
                f"Available: {', '.join(sorted(props_by_local))}"
            )

        rel_iri = rdef.get("iri")
        if rel_iri and rel_iri != prop["iri"]:
            raise MappingError(
                f"Relation '{rel_name}' IRI mismatch: mapping says '{rel_iri}', "
                f"ontology says '{prop['iri']}'"
            )

        from_iri = prop.get("domain_iri")
        to_iri = prop.get("range_iri")
        from_map = smap.tables_by_iri.get(from_iri)
        to_map = smap.tables_by_iri.get(to_iri)
        if not from_map or not to_map:
            raise MappingError(
                f"Relation '{rel_name}' endpoints not found in table mappings. "
                f"Domain IRI: {from_iri}, Range IRI: {to_iri}"
            )

        rel_type = rdef.get("type", "direct")

        if rel_type == "direct":
            step = JoinStep(
                fk_table=rdef["fk_table"],
                fk_column=rdef["fk_column"],
                ref_table=rdef["ref_table"],
                ref_column=rdef["ref_column"],
            )
            smap.relations[rel_name] = RelationMap(
                name=rel_name,
                iri=prop["iri"],
                from_class=from_map.class_name,
                to_class=to_map.class_name,
                steps=[step],
                junction_table=None,
            )
            # Populate fk_index
            smap.fk_index.setdefault(step.ref_table, []).append(
                (step.fk_table, step.fk_column)
            )

        elif rel_type == "junction":
            junction_table = rdef.get("junction_table")
            if not junction_table:
                raise MappingError(
                    f"Junction relation '{rel_name}' must have 'junction_table'"
                )
            steps = []
            for sdef in rdef.get("steps") or []:
                step = JoinStep(
                    fk_table=sdef["fk_table"],
                    fk_column=sdef["fk_column"],
                    ref_table=sdef["ref_table"],
                    ref_column=sdef["ref_column"],
                )
                steps.append(step)
                smap.fk_index.setdefault(step.ref_table, []).append(
                    (step.fk_table, step.fk_column)
                )

            smap.junction_tables.add(junction_table)
            smap.relations[rel_name] = RelationMap(
                name=rel_name,
                iri=prop["iri"],
                from_class=from_map.class_name,
                to_class=to_map.class_name,
                steps=steps,
                junction_table=junction_table,
            )

        else:
            raise MappingError(
                f"Relation '{rel_name}' has unknown type '{rel_type}'. "
                f"Must be 'direct' or 'junction'."
            )

    return smap


# ── Auto-generated mapping from schema.json ─────────────────────────────────


def generate_mapping_from_schema(
    ontology_model: dict, schema: dict,
) -> dict:
    """Derive a mapping dict from an ontology model + schema.json.

    For architect-generated schemas the column names in schema.json ARE
    the ontology property names (both snake_case), so the column mapping
    is an identity (``first_name: first_name``).  Making it explicit
    means the runtime always takes the same code path — mapping →
    SchemaMap — regardless of whether the DB was generated or
    pre-existing.

    Returns a dict in the same format that ``build_schema_map_from_mapping``
    consumes (``tables`` + ``relations``).
    """
    classes_by_iri = {c["iri"]: c for c in ontology_model["classes"]}

    # Index datatype properties by (class_iri, snake_name) → property IRI.
    _dp_iri: dict[tuple[str, str], str] = {}
    for dp in ontology_model.get("datatype_properties", []):
        for class_iri in dp.get("domain_iris") or []:
            _dp_iri[(class_iri, dp["snake_name"])] = dp["iri"]

    # ── Tables ───────────────────────────────────────────────────────
    tables: dict[str, dict] = {}
    table_phys_to_class: dict[str, str] = {}  # physical name → class name

    for t in schema["tables"]:
        if t.get("lookup_table"):
            continue

        iri = t.get("ontology_iri")
        cls = classes_by_iri.get(iri)
        if not cls:
            continue

        class_name = cls["local_name"]
        table_phys_to_class[t["name"]] = class_name

        # Identity column mapping with property IRIs.
        columns: dict[str, dict] = {}
        for col in t.get("columns", []):
            col_name = col["name"]
            prop_iri = _dp_iri.get((iri, col_name))
            columns[col_name] = {"iri": prop_iri, "column": col_name}

        tables[class_name] = {
            "iri": iri,
            "table": t["name"],
            "primary_key": t["primary_key"],
            "columns": columns,
        }

    # ── Detect junction tables (same heuristic as SchemaMap) ─────────
    _skip = {"created_at", "updated_at"}
    junction_tables: set[str] = set()
    for t in schema["tables"]:
        if t.get("lookup_table"):
            continue
        fks = t.get("foreign_keys") or []
        if len(fks) < 2:
            continue
        data_cols = [c for c in (t.get("columns") or []) if c["name"] not in _skip]
        if not data_cols:
            junction_tables.add(t["name"])

    tables_by_name = {t["name"]: t for t in schema["tables"]}

    # ── Relations ────────────────────────────────────────────────────
    relations: dict[str, dict] = {}

    for prop in ontology_model.get("object_properties", []):
        from_cls = classes_by_iri.get(prop.get("domain_iri"))
        to_cls = classes_by_iri.get(prop.get("range_iri"))
        if not from_cls or not to_cls:
            continue

        from_def = tables.get(from_cls["local_name"])
        to_def = tables.get(to_cls["local_name"])
        if not from_def or not to_def:
            continue

        from_phys = from_def["table"]
        to_phys = to_def["table"]

        # Try direct FK (either direction).
        direct = _find_direct_fk(tables_by_name, from_phys, to_phys)
        if not direct:
            direct = _find_direct_fk(tables_by_name, to_phys, from_phys)

        if direct:
            relations[prop["local_name"]] = {
                "iri": prop["iri"],
                "type": "direct",
                "fk_table": direct[0],
                "fk_column": direct[1],
                "ref_table": direct[2],
                "ref_column": direct[3],
            }
            continue

        # Try junction path.
        for jname in sorted(junction_tables):
            j = tables_by_name[jname]
            fk_from = next(
                (fk for fk in j.get("foreign_keys", [])
                 if fk["references_table"] == from_phys),
                None,
            )
            fk_to = next(
                (fk for fk in j.get("foreign_keys", [])
                 if fk["references_table"] == to_phys),
                None,
            )
            if fk_from and fk_to:
                relations[prop["local_name"]] = {
                    "iri": prop["iri"],
                    "type": "junction",
                    "junction_table": jname,
                    "steps": [
                        {
                            "fk_table": jname,
                            "fk_column": fk_from["column"],
                            "ref_table": from_phys,
                            "ref_column": fk_from["references_column"],
                        },
                        {
                            "fk_table": jname,
                            "fk_column": fk_to["column"],
                            "ref_table": to_phys,
                            "ref_column": fk_to["references_column"],
                        },
                    ],
                }
                break

    return {"tables": tables, "relations": relations}


def _find_direct_fk(
    tables_by_name: dict, child_name: str, parent_name: str,
) -> tuple[str, str, str, str] | None:
    """Return (fk_table, fk_col, ref_table, ref_col) if child→parent FK exists."""
    child = tables_by_name.get(child_name)
    if not child:
        return None
    for fk in child.get("foreign_keys", []):
        if fk["references_table"] == parent_name:
            return (child_name, fk["column"], parent_name, fk["references_column"])
    return None
