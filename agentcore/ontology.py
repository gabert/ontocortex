"""
Domain-agnostic utilities for parsing OWL/Turtle ontologies.

The domain namespace is auto-detected from the owl:Ontology declaration,
so the same code works for any domain (insurance, vet clinic, etc.).

Provides helpers used by the planner and runtime schema loaders.
"""

import re

from rdflib import XSD, BNode, Graph, Namespace, OWL, RDF, RDFS, URIRef
from rdflib.collection import Collection

# ── Namespaces ────────────────────────────────────────────────────────────────

# Namespaces that are infrastructure, not domain content.
# Anything NOT in this set is considered a domain namespace.
_INFRA_PREFIXES: tuple[str, ...] = (
    str(RDF),
    str(RDFS),
    str(OWL),
    str(XSD),
    "http://www.w3.org/1999/02/22-rdf-syntax-ns",
    "http://www.w3.org/2000/01/rdf-schema",
    "http://www.w3.org/2002/07/owl",
    "http://www.w3.org/2001/XMLSchema",
)


def is_domain_uri(uri) -> bool:
    """Return True if the URI belongs to a domain namespace (not RDF/OWL/XSD infra)."""
    if isinstance(uri, BNode):
        return False
    s = str(uri)
    return not any(s.startswith(p) for p in _INFRA_PREFIXES)


def detect_domain_namespace(g: Graph) -> Namespace:
    """Auto-detect the domain namespace from the owl:Ontology declaration.

    Every valid domain ontology has exactly one ``<uri> a owl:Ontology`` triple.
    The namespace is that URI + '#'.
    """
    for ont in g.subjects(RDF.type, OWL.Ontology):
        return Namespace(str(ont) + "#")
    raise ValueError("No owl:Ontology declaration found in the graph")

# ── XSD → PostgreSQL type mapping ────────────────────────────────────────────

XSD_TO_PG: dict[URIRef, str] = {
    XSD.string:   "VARCHAR(255)",
    XSD.integer:  "INTEGER",
    XSD.decimal:  "DECIMAL(12, 2)",
    XSD.boolean:  "BOOLEAN",
    XSD.date:     "DATE",
    XSD.dateTime: "TIMESTAMP",
}

# ── Name helpers ──────────────────────────────────────────────────────────────

def local_name(uri: URIRef) -> str:
    return str(uri).split("#")[-1].split("/")[-1]


def to_snake_case(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def to_table_name(class_name: str) -> str:
    """CamelCase class name → plural snake_case table name."""
    s = to_snake_case(class_name)
    if s.endswith("s"):
        return s  # already plural (species, status, etc.)
    if s.endswith("y"):
        return s[:-1] + "ies"
    return s + "s"


def to_pk_name(class_name: str) -> str:
    return to_snake_case(class_name) + "_id"


def union_classes(g: Graph, domain_node) -> list[URIRef]:
    """Return the list of classes from a domain node (handles owl:unionOf)."""
    if isinstance(domain_node, BNode):
        union_list = g.value(domain_node, OWL.unionOf)
        if union_list:
            return [c for c in Collection(g, union_list) if isinstance(c, URIRef)]
    if isinstance(domain_node, URIRef):
        return [domain_node]
    return []


def is_value_set(g: Graph, cls: URIRef) -> bool:
    return (cls, OWL.oneOf, None) in g


# ── Structured ontology model ────────────────────────────────────────────────

class OntologyParseError(ValueError):
    """Raised when an ontology .ttl file cannot be parsed."""


def build_ontology_model(ontology_text: str, source_path=None) -> dict:
    """Parse an OWL/Turtle ontology into a structured dict.

    Unlike `build_compact_ontology` (which emits human/LLM-readable text),
    this returns a machine-readable model keyed by full IRIs. It is the
    source of truth for code that needs to map ontology concepts to
    physical schema elements without re-parsing text.

    `source_path` is used only to produce a helpful error message if the
    parse fails; it has no semantic effect on the result.
    """
    g = Graph()
    try:
        g.parse(data=ontology_text, format="turtle")
    except Exception as e:
        where = f" ({source_path})" if source_path else ""
        raise OntologyParseError(
            f"Failed to parse ontology{where}: {e}"
        ) from e

    # Namespace map: domain prefixes only
    candidate_ns = {
        str(uri): prefix
        for prefix, uri in g.namespaces()
        if prefix and not any(str(uri).startswith(p) for p in _INFRA_PREFIXES)
    }
    used_ns: set[str] = set()
    for s, _p, o in g:
        for node in (s, o):
            if is_domain_uri(node):
                node_str = str(node)
                for ns_uri in candidate_ns:
                    if node_str.startswith(ns_uri):
                        used_ns.add(ns_uri)
                        break
    ns_map = {uri: prefix for uri, prefix in candidate_ns.items() if uri in used_ns}

    def qname(uri) -> str:
        s = str(uri)
        for ns_uri, prefix in ns_map.items():
            if s.startswith(ns_uri):
                return f"{prefix}:{s[len(ns_uri):]}"
        return local_name(uri)

    classes: list[dict] = []
    value_sets: list[dict] = []
    for cls in sorted(g.subjects(RDF.type, OWL.Class), key=str):
        if not is_domain_uri(cls):
            continue
        comment = str(g.value(cls, RDFS.comment) or "")
        if is_value_set(g, cls):
            members = []
            for one_of_node in g.objects(cls, OWL.oneOf):
                members = [str(v) for v in Collection(g, one_of_node) if isinstance(v, URIRef)]
                break
            value_sets.append({
                "iri": str(cls),
                "qname": qname(cls),
                "local_name": local_name(cls),
                "comment": comment,
                "members": members,
            })
            continue
        classes.append({
            "iri": str(cls),
            "qname": qname(cls),
            "local_name": local_name(cls),
            "comment": comment,
        })

    object_properties: list[dict] = []
    for prop in sorted(g.subjects(RDF.type, OWL.ObjectProperty), key=str):
        if not is_domain_uri(prop):
            continue
        domain_node = g.value(prop, RDFS.domain)
        range_node = g.value(prop, RDFS.range)
        domain_iris = [str(c) for c in union_classes(g, domain_node)] if domain_node else []
        range_iris = [str(c) for c in union_classes(g, range_node)] if range_node else []
        object_properties.append({
            "iri": str(prop),
            "qname": qname(prop),
            "local_name": local_name(prop),
            # Legacy single-valued fields retained for callers that only
            # handle the common case; cleared to None when the declared
            # domain/range is a union so downstream code can branch on it.
            "domain_iri": domain_iris[0] if len(domain_iris) == 1 else None,
            "range_iri": range_iris[0] if len(range_iris) == 1 else None,
            "domain_iris": domain_iris,
            "range_iris": range_iris,
            "comment": str(g.value(prop, RDFS.comment) or ""),
        })

    datatype_properties: list[dict] = []
    for prop in sorted(g.subjects(RDF.type, OWL.DatatypeProperty), key=str):
        if not is_domain_uri(prop):
            continue
        domain_node = g.value(prop, RDFS.domain)
        domain_iris = [str(c) for c in union_classes(g, domain_node)] if domain_node else []
        datatype_properties.append({
            "iri": str(prop),
            "qname": qname(prop),
            "local_name": local_name(prop),
            "snake_name": to_snake_case(local_name(prop)),
            "domain_iris": domain_iris,
            "range_iri": str(g.value(prop, RDFS.range)) if g.value(prop, RDFS.range) else None,
            "comment": str(g.value(prop, RDFS.comment) or ""),
        })

    return {
        "namespaces": {prefix: uri for uri, prefix in ns_map.items()},
        "classes": classes,
        "object_properties": object_properties,
        "datatype_properties": datatype_properties,
        "value_sets": value_sets,
    }