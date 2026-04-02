"""
Domain-agnostic utilities for parsing OWL/Turtle ontologies.

The domain namespace is auto-detected from the owl:Ontology declaration,
so the same code works for any domain (insurance, vet clinic, etc.).

Provides helpers used by the compact-ontology builder (architect.py).
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