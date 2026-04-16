package com.ontocore.engine.ontology;

import java.util.List;
import java.util.Map;

/**
 * Structured representation of a parsed OWL/Turtle ontology.
 *
 * <p>This is the machine-readable model that the planner, schema mapping,
 * and validation layers consume. Built once per domain by
 * {@link OntologyParser}.</p>
 */
public record OntologyModel(
        Map<String, String> namespaces,
        List<OntologyClass> classes,
        List<ObjectProperty> objectProperties,
        List<DatatypeProperty> datatypeProperties,
        List<ValueSet> valueSets
) {}
