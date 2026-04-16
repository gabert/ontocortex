package com.ontocore.engine.schema;

import java.util.Map;

/**
 * Contract for mapping an ontology to any physical storage backend.
 *
 * <p>Every backend (relational, document, graph, flat file) must provide
 * at minimum: which ontology classes map to which physical entities,
 * IRI-based lookups, and how object properties translate to physical
 * relations.</p>
 *
 * <p>Backend-specific details (bridge entities for relational, embedded
 * paths for document stores, edge types for graphs) live on the concrete
 * implementation — not here.</p>
 */
public interface OntologyMapping {

    /** Ontology class local name → entity mapping. */
    Map<String, EntityMapping> entities();

    /** Ontology class IRI → entity mapping. */
    Map<String, EntityMapping> entitiesByIri();

    /** Ontology object property local name → relation mapping. */
    Map<String, RelationMap> relations();
}
