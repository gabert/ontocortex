package com.ontocore.engine.ontology;

/**
 * A named OWL class from the domain ontology.
 */
public record OntologyClass(
        String iri,
        String qname,
        String localName,
        String comment
) {}
