"""LLM-driven database schema design from OWL ontology.

Instead of mechanically parsing OWL into tables, this module sends the raw
ontology to Claude acting as a database architect.  Claude infers proper
normalisation (FK placement, CHECK constraints from value sets, etc.) and
returns a **database-agnostic logical schema** as JSON.

The logical schema uses generic types (string, integer, decimal, boolean,
date, datetime) that can be mapped to any concrete database dialect by a
separate DDL-generation step.  It also serves directly as context for the
business-logic agent's system prompt.

A second LLM call generates realistic seed data as SQL INSERTs.

Generated artefacts are saved next to the ontology and referenced from
domain.json so the rest of the framework can use them.
"""

import json
import re

from anthropic import Anthropic
from agentcore.config import AppConfig
from agentcore.domain import DomainConfig

_SCHEMA_MODEL     = "claude-opus-4-6"   # Opus for complex normalization reasoning
_SEED_MODEL       = "claude-sonnet-4-20250514"  # Sonnet sufficient for mechanical data gen
_SCHEMA_MAX_TOKENS = 16000  # total cap (thinking + response)
_SEED_MAX_TOKENS   = 16384

# ── System prompts ────────────────────────────────────────────────────────────

_SCHEMA_SYSTEM_PROMPT = """\
You are a senior database architect.  Your task is to design a relational \
schema in Third Normal Form (3NF) from an OWL/Turtle ontology.

3NF means: every non-key attribute depends on the primary key, the whole \
key, and nothing but the key.  Concretely:
- No repeating groups or multi-valued columns (1NF).
- No partial dependencies on a composite key (2NF).
- No transitive dependencies — non-key columns must not determine other \
non-key columns (3NF).

The output must be a **database-agnostic logical schema** — no vendor-specific \
SQL types.  A separate step will map it to concrete DDL for PostgreSQL, \
SQLite, or any other backend.

Read the ontology carefully.  Identify every class, datatype property, \
object property, and value set (owl:oneOf).  Then produce a 3NF schema \
following the rules below.

## Rules

0. **Namespaces.**
   The ontology begins with a NAMESPACES section listing every domain prefix
   and its full URI, e.g. `ins: <https://insurance.example.org/ontology#>`.
   All class and property names in the compact ontology are written as
   `prefix:LocalName`.  Use the prefix to drive naming in the schema:
   - Table names and primary keys are prefixed with the namespace short name.
   - Column names use only the local part (no prefix).
   - When a foreign key crosses namespaces (e.g. `pol:Policy -> crm:Customer`),
     the FK column name is `{target_prefix}_{target_singular}_id`.

1. **Classes → Tables.**
   Each owl:Class that is NOT a value set (owl:oneOf) becomes a table.
   - Table name: `{prefix}_{plural_snake_case}` \
(ins:Customer → ins_customers, ins:Policy → ins_policies, \
crm:Agent → crm_agents).
   - Primary key: `{prefix}_{singular}_id`, auto-increment integer \
(ins:Customer → ins_customer_id).
   - Every table automatically receives a created_at datetime column — \
do NOT include either the primary key or created_at in the columns array.

2. **Datatype properties → Columns.**
   Each owl:DatatypeProperty becomes a column on the table(s) indicated by
   rdfs:domain.  If rdfs:domain uses owl:unionOf, the column appears on
   every listed class's table.
   - Column name: snake_case of the **local part only** (strip the prefix): \
ins:agency_name → agency_name, crm:firstName → first_name.
   - Use these logical types ONLY:
       string   — short text (names, codes, identifiers)
       text     — long / unbounded text (descriptions, notes)
       integer  — whole numbers
       decimal  — fixed-point numbers (money, rates, measurements)
       boolean  — true / false
       date     — calendar date
       datetime — date + time

3. **Object properties → Foreign keys.**
   Each owl:ObjectProperty expresses a relationship.  Infer which side
   carries the FK from the semantics of the property:
   - "ins:Customer ins:hasPolicy ins:Policy" → ins_policies carries ins_customer_id.
   - "ins:Policy ins:managedBy ins:Agent"    → ins_policies carries ins_agent_id.
   - "pol:Policy pol:heldBy crm:Customer"    → pol_policies carries crm_customer_id.
   The dependent / child side carries the FK.
   - FK column: `{target_prefix}_{target_singular}_id`, type integer.
   - FK columns go ONLY in the foreign_keys array, NOT in columns.

4. **Value sets → CHECK constraints.**
   Classes defined with owl:oneOf are enumerations, NOT tables.
   Find the datatype property whose values correspond to each value set
   (match by name, comment, or range) and add an "allowed_values" list
   to that column definition.
   - allowed_values entries use the **local part only** (strip the prefix): \
ins:active → "active", ins:claim_pending → "claim_pending".

5. **Constraints.**
   - not_null  : the field must always have a value (identifiers, dates,
     amounts, mandatory flags, parent-entity FKs).
   - unique    : natural keys / identifiers (policy numbers, licence
     numbers, VINs, emails).
   - required  : *business-level* — the agent must collect this value from
     the user before creating a record.  This is a subset of not_null;
     auto-generated or defaulted fields are NOT required.
   - default   : sensible defaults where appropriate.  Use plain JSON
     values: true/false for booleans, plain strings for text (no SQL
     quoting).

## Output

Return ONLY a JSON object — no markdown fences, no commentary.

{
  "tables": [
    {
      "name": "ins_policies",
      "comment": "What this table represents",
      "primary_key": "ins_policy_id",
      "columns": [
        {
          "name": "col",
          "type": "string",
          "not_null": true,
          "unique": true,
          "required": true,
          "default": "some_value",
          "allowed_values": ["a", "b", "c"]
        }
      ],
      "foreign_keys": [
        {
          "column": "ins_customer_id",
          "references_table": "ins_customers",
          "references_column": "ins_customer_id",
          "not_null": true,
          "required": true
        }
      ]
    }
  ],
  "creation_order": ["ins_independent_table", "ins_dependent_table"]
}

JSON rules:
- Omit optional fields when they are false or null (do NOT write "unique": false).
- creation_order lists ALL table names in topological order: every table
  appears AFTER the tables it references via foreign keys.
- Use only the seven logical types listed above.
- Defaults are plain JSON values (true, false, "active") — no SQL syntax.
"""

_SEED_DATA_SYSTEM_PROMPT = """\
You are a test-data generator.  Given a logical database schema (JSON) and \
the original OWL ontology for domain context, generate realistic seed data \
as SQL INSERT statements.

## Rules

1. Generate 5–10 records per table — enough to demonstrate all
   relationships and cover every allowed_values / enum value at least once.
2. Insert in dependency order (tables with no FKs first).
3. The database is freshly created: auto-increment ids start at 1 and
   increment sequentially.  Use these predictable ids for FK references
   (first insert → 1, second → 2, …).
4. Use realistic, domain-appropriate values (plausible names, dates,
   amounts, etc.).
5. Referential integrity: every FK value must match an already-inserted
   record's id.
6. Respect allowed_values, NOT NULL, and UNIQUE constraints.
7. Date format: 'YYYY-MM-DD'.  Strings in single quotes.
8. Do NOT insert the primary key (auto-generated) or created_at (has
   a default).
9. List column names explicitly in every INSERT.
10. Use standard SQL syntax — no vendor extensions.

## Output

Return ONLY valid SQL.  No markdown fences, no prose.
Use -- comments for section headers (e.g. -- agents).
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

_LOGICAL_TYPES = {"string", "text", "integer", "decimal", "boolean", "date", "datetime"}


def _extract_json(text: str) -> dict:
    """Extract a JSON object from LLM response text."""
    # markdown fences
    m = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    if m:
        return json.loads(m.group(1))
    # raw JSON
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        return json.loads(m.group(0))
    raise ValueError("No JSON object found in LLM response")


def _extract_sql(text: str) -> str:
    """Extract SQL from LLM response text."""
    m = re.search(r"```(?:sql)?\s*\n(.*?)\n```", text, re.DOTALL)
    if m:
        return m.group(1).strip()
    return text.strip()


def _check_stop_reason(response, label: str) -> None:
    """Raise a clear error if the response was truncated by the token limit."""
    if response.stop_reason == "max_tokens":
        usage = response.usage
        raise RuntimeError(
            f"{label} was truncated — max_tokens limit hit.\n"
            f"  Tokens used: input={usage.input_tokens}, output={usage.output_tokens}\n"
            f"  Increase _SCHEMA_MAX_TOKENS in architect.py (currently {_SCHEMA_MAX_TOKENS}) and retry."
        )


def _call_schema_llm(client: Anthropic, system: str, user_message: str) -> str:
    """Call Opus with adaptive thinking for schema design."""
    response = client.messages.create(
        model=_SCHEMA_MODEL,
        max_tokens=_SCHEMA_MAX_TOKENS,
        thinking={"type": "adaptive"},
        system=system,
        messages=[{"role": "user", "content": user_message}],
    )
    _check_stop_reason(response, "Schema design response")
    # thinking blocks have .thinking not .text — filter them out
    return next((b.text for b in response.content if hasattr(b, "text")), "")


def _call_seed_llm(client: Anthropic, system: str, user_message: str) -> str:
    """Call Sonnet (no thinking) for mechanical seed data generation."""
    response = client.messages.create(
        model=_SEED_MODEL,
        max_tokens=_SEED_MAX_TOKENS,
        system=system,
        messages=[{"role": "user", "content": user_message}],
    )
    _check_stop_reason(response, "Seed data response")
    return next((b.text for b in response.content if hasattr(b, "text")), "")


def _validate_schema(schema: dict) -> None:
    """Catch obvious structural errors before saving."""
    if "tables" not in schema:
        raise ValueError("Schema missing 'tables' key")
    if "creation_order" not in schema:
        raise ValueError("Schema missing 'creation_order' key")

    table_names = {t["name"] for t in schema["tables"]}

    for name in schema["creation_order"]:
        if name not in table_names:
            raise ValueError(f"creation_order references unknown table: {name}")

    for t in schema["tables"]:
        for col in t.get("columns", []):
            if col["type"] not in _LOGICAL_TYPES:
                raise ValueError(
                    f"Table '{t['name']}', column '{col['name']}': "
                    f"unknown logical type '{col['type']}' "
                    f"(expected one of {sorted(_LOGICAL_TYPES)})"
                )
        for fk in t.get("foreign_keys", []):
            ref = fk["references_table"]
            if ref not in table_names:
                raise ValueError(
                    f"Table '{t['name']}' has FK to unknown table: '{ref}'"
                )


# ── Compact ontology ─────────────────────────────────────────────────────────

_SQL_TO_LOGICAL: dict[str, str] = {
    "VARCHAR(255)":   "string",
    "TEXT":           "text",
    "INTEGER":        "integer",
    "DECIMAL(12, 2)": "decimal",
    "BOOLEAN":        "boolean",
    "DATE":           "date",
    "TIMESTAMP":      "datetime",
}


def build_compact_ontology(ontology_text: str) -> str:
    """Deterministic, lossless transformation of an OWL/Turtle ontology
    into a compact, human- and LLM-readable text format.

    Preserves the four OWL building blocks:
      1. Classes — entities with comments
      2. Datatype properties — attributes with types and comments
      3. Object properties — relationships with comments
      4. Value sets — owl:oneOf enumerations

    All names are written as prefix:LocalName using the @prefix bindings
    from the source TTL file.  This keeps the output unambiguous when an
    ontology spans more than one domain namespace.

    No schema decisions are made here — the compact form faithfully
    represents the domain model for downstream consumers (LLM architect,
    seed data generator, business agent).
    """
    from agentcore.ontology import (
        _INFRA_PREFIXES, XSD_TO_PG, is_domain_uri, local_name, to_snake_case,
        is_value_set, union_classes,
    )
    from rdflib import Graph, OWL, RDF, RDFS
    from rdflib.collection import Collection

    g = Graph()
    g.parse(data=ontology_text, format="turtle")

    # Build reverse map: full_namespace_uri -> short_prefix
    # Only for domain (non-infra) namespaces with a non-empty prefix
    # that are actually referenced by at least one URI in the graph.
    _candidate_ns: dict[str, str] = {
        str(uri): prefix
        for prefix, uri in g.namespaces()
        if prefix and not any(str(uri).startswith(p) for p in _INFRA_PREFIXES)
    }
    _used_ns: set[str] = set()
    for s, _p, o in g:
        for node in (s, o):
            if is_domain_uri(node):
                node_str = str(node)
                for ns_uri in _candidate_ns:
                    if node_str.startswith(ns_uri):
                        _used_ns.add(ns_uri)
                        break
    ns_map: dict[str, str] = {
        uri: prefix for uri, prefix in _candidate_ns.items() if uri in _used_ns
    }

    def pfx(uri) -> str:
        """Return prefix:LocalName, falling back to bare local_name()."""
        s = str(uri)
        for ns_uri, ns_prefix in ns_map.items():
            if s.startswith(ns_uri):
                return f"{ns_prefix}:{s[len(ns_uri):]}"
        return local_name(uri)

    def pfx_snake(uri) -> str:
        """Return prefix:snake_local_name for a property URI."""
        s = str(uri)
        for ns_uri, ns_prefix in ns_map.items():
            if s.startswith(ns_uri):
                return f"{ns_prefix}:{to_snake_case(s[len(ns_uri):])}"
        return to_snake_case(local_name(uri))

    lines: list[str] = []

    # ── 0. Namespaces ────────────────────────────────────────────────────
    lines.append("NAMESPACES")
    for ns_uri, ns_prefix in sorted(ns_map.items(), key=lambda x: x[1]):
        lines.append(f"  {ns_prefix}: <{ns_uri}>")
    lines.append("")

    # ── 1. Classes ───────────────────────────────────────────────────────
    lines.append("CLASSES")
    for cls in sorted(g.subjects(RDF.type, OWL.Class), key=str):
        if not is_domain_uri(cls):
            continue
        if is_value_set(g, cls):
            continue
        comment = str(g.value(cls, RDFS.comment) or "")
        lines.append(f"  {pfx(cls)} -- {comment}" if comment else f"  {pfx(cls)}")
    lines.append("")

    # ── 2. Datatype properties ───────────────────────────────────────────
    lines.append("PROPERTIES")
    for prop in sorted(g.subjects(RDF.type, OWL.DatatypeProperty), key=str):
        if not is_domain_uri(prop):
            continue
        label = str(g.value(prop, RDFS.label) or local_name(prop))
        col_name = pfx_snake(prop)
        comment = str(g.value(prop, RDFS.comment) or "")
        xsd_type = g.value(prop, RDFS.range)
        sql_type = XSD_TO_PG.get(xsd_type, "TEXT")
        logical_type = _SQL_TO_LOGICAL.get(sql_type, "string")

        domain_node = g.value(prop, RDFS.domain)
        classes = [pfx(c) for c in union_classes(g, domain_node)] if domain_node else []

        on_str = f" on {'/'.join(classes)}" if classes else ""
        comment_str = f" -- {comment}" if comment else ""
        lines.append(f"  {col_name}: {logical_type}{on_str}{comment_str}")
    lines.append("")

    # ── 3. Object properties (relationships) ─────────────────────────────
    lines.append("RELATIONSHIPS")
    for prop in sorted(g.subjects(RDF.type, OWL.ObjectProperty), key=str):
        if not is_domain_uri(prop):
            continue
        comment = str(g.value(prop, RDFS.comment) or "")
        domain_cls = g.value(prop, RDFS.domain)
        range_cls = g.value(prop, RDFS.range)

        from_name = pfx(domain_cls) if domain_cls else "?"
        to_name = pfx(range_cls) if range_cls else "?"

        parts = [f"  {pfx(prop)}: {from_name} -> {to_name}"]
        if comment:
            parts.append(f"-- {comment}")
        lines.append("  ".join(parts))
    lines.append("")

    # ── 4. Value sets (owl:oneOf) ────────────────────────────────────────
    lines.append("VALUE SETS")
    for cls in sorted(g.subjects(RDF.type, OWL.Class), key=str):
        if not is_domain_uri(cls):
            continue
        for one_of_node in g.objects(cls, OWL.oneOf):
            values = [pfx(v) for v in Collection(g, one_of_node)]
            if values:
                lines.append(f"  {pfx(cls)}: {', '.join(values)}")

    return "\n".join(lines).strip()


# ── Public API ────────────────────────────────────────────────────────────────

def generate_compact_ontology(domain: DomainConfig) -> tuple[str, str]:
    """Build a compact ontology glossary, save it, return (compact_text, filename)."""
    compact = build_compact_ontology(domain.ontology_text)

    compact_file = f"{domain.dir_name}_ontology_compact.owl"
    domain.generated_dir.mkdir(exist_ok=True)
    compact_path = domain.generated_dir / compact_file
    compact_path.write_text(compact + "\n", encoding="utf-8")
    print(f"  Saved _generated/{compact_path.name}")
    return compact, compact_file


def design_schema(config: AppConfig, domain: DomainConfig, compact_ontology: str) -> tuple[dict, str]:
    """Send compact ontology to Claude, receive a normalised logical schema, save as JSON."""
    client = Anthropic(api_key=config.api_key)
    raw = _call_schema_llm(client, _SCHEMA_SYSTEM_PROMPT, compact_ontology)
    schema = _extract_json(raw)
    _validate_schema(schema)

    schema_file = f"{domain.dir_name}_schema.json"
    domain.generated_dir.mkdir(exist_ok=True)
    schema_path = domain.generated_dir / schema_file
    schema_path.write_text(
        json.dumps(schema, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"  Saved _generated/{schema_path.name}")
    return schema, schema_file


def design_seed_data(config: AppConfig, domain: DomainConfig, schema: dict, compact_ontology: str) -> tuple[str, str]:
    """Send schema + compact ontology context to Claude, receive SQL INSERTs, save."""
    client = Anthropic(api_key=config.api_key)
    user_message = (
        "## Database Schema\n\n"
        f"{json.dumps(schema, indent=2)}\n\n"
        "## Domain Ontology (for context)\n\n"
        f"{compact_ontology}"
    )
    sql = _extract_sql(_call_seed_llm(client, _SEED_DATA_SYSTEM_PROMPT, user_message))

    seed_file = f"{domain.dir_name}_seed_data.sql"
    domain.generated_dir.mkdir(exist_ok=True)
    seed_path = domain.generated_dir / seed_file
    seed_path.write_text(sql + "\n", encoding="utf-8")
    print(f"  Saved _generated/{seed_path.name}")
    return sql, seed_file


def fix_seed_data(
    config: AppConfig,
    domain: DomainConfig,
    schema: dict,
    compact_ontology: str,
    sql: str,
    errors: list[str],
) -> str:
    """Ask the LLM to fix seed SQL that failed validation.  Returns corrected SQL."""
    client = Anthropic(api_key=config.api_key)
    error_block = "\n".join(f"  - {e}" for e in errors)
    user_message = (
        "## Database Schema\n\n"
        f"{json.dumps(schema, indent=2)}\n\n"
        "## Domain Ontology (for context)\n\n"
        f"{compact_ontology}\n\n"
        "## Current SQL (contains errors)\n\n"
        f"{sql}\n\n"
        "## Validation Errors\n\n"
        f"{error_block}\n\n"
        "Fix the SQL to resolve all validation errors. Return only valid SQL."
    )
    return _extract_sql(_call_seed_llm(client, _SEED_DATA_SYSTEM_PROMPT, user_message))


def update_domain_manifest(domain: DomainConfig, **generated_keys: str) -> None:
    """Merge one or more keys into the 'generated' section of domain.json.

    Call after each pipeline step with just the key(s) produced by that step:
        update_domain_manifest(domain, ontology_compact=compact_file)
        update_domain_manifest(domain, schema=schema_file)
        update_domain_manifest(domain, seed_data=seed_file)
    """
    manifest_path = domain.ontology_path.parent / "domain.json"
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    manifest.setdefault("generated", {}).update(generated_keys)

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
        f.write("\n")
