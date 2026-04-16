package com.ontocore.engine.ontology;

import java.util.List;

/**
 * An OWL class defined via owl:oneOf — a closed set of allowed values.
 */
public record ValueSet(
        String iri,
        String qname,
        String localName,
        String comment,
        List<String> members
) {}
