"""Ontology-to-physical schema mapping.

SchemaMap is the bridge between the ontology model (classes + object
properties) and the physical schema (tables + foreign keys). It is
built once per domain at pipeline startup and then consulted by every
validator, tool-schema builder, and backend translator.

The dataclasses here (TableMap, JoinStep, RelationMap) describe the
mapping in SQL-shaped terms — tables, primary keys, foreign keys — but
they carry no I/O. A different backend (GraphQL, REST) would wrap the
same data with a different executor without replacing this module.
"""

from dataclasses import dataclass, field


@dataclass
class TableMap:
    class_name: str      # Ontology local name: "Customer"
    class_iri: str       # Ontology IRI: "https://.../ontology#Customer"
    table_name: str      # Physical: "ins_customers"
    primary_key: str     # Physical: "ins_customer_id"
    columns: list[str]   # Ontology property names — what SIF / LLM uses
    column_map: dict[str, str] = field(default_factory=dict)  # ontology → physical
    comment: str = ""

    def physical_column(self, field_name: str) -> str:
        """Resolve an ontology field name to its physical column name.

        When column_map is empty (auto-generated schema), returns the
        name unchanged — ontology names ARE the physical names.
        """
        return self.column_map.get(field_name, field_name)


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
        self.fk_index: dict[str, list[tuple[str, str]]] = {}  # ref_table → [(fk_table, fk_column)]
        self._schema = schema
        self._build(ontology_model, schema)

    def _build(self, model: dict, schema: dict) -> None:
        self._build_tables(model, schema)
        self._build_fk_index(schema)
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

    # ── FK index ─────────────────────────────────────────────────────────

    def _build_fk_index(self, schema: dict) -> None:
        """Build a lookup of ref_table → [(fk_table, fk_column)].

        Used by OwnershipMap to find tables scoped to the identity entity,
        without reaching into the raw schema dict.
        """
        for t in schema.get("tables", []):
            for fk in t.get("foreign_keys", []):
                ref = fk["references_table"]
                self.fk_index.setdefault(ref, []).append(
                    (t["name"], fk["column"])
                )

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
