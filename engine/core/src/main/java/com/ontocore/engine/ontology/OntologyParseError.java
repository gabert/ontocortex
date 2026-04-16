package com.ontocore.engine.ontology;

/**
 * Raised when an ontology .ttl file cannot be parsed.
 */
public class OntologyParseError extends RuntimeException {
    public OntologyParseError(String message) {
        super(message);
    }

    public OntologyParseError(String message, Throwable cause) {
        super(message, cause);
    }
}
