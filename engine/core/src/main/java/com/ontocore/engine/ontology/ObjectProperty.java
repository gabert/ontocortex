package com.ontocore.engine.ontology;

import java.util.List;

/**
 * An OWL object property (relationship between two classes).
 */
public record ObjectProperty(
        String iri,
        String qname,
        String localName,
        String domainIri,
        String rangeIri,
        List<String> domainIris,
        List<String> rangeIris,
        String comment
) {}
