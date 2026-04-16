package com.ontocore.engine.ontology;

import java.util.List;

/**
 * An OWL datatype property (attribute of a class with an XSD range).
 */
public record DatatypeProperty(
        String iri,
        String qname,
        String localName,
        String snakeName,
        List<String> domainIris,
        String rangeIri,
        String comment
) {}
