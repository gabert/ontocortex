package com.ontocore.engine.schema;

/**
 * Raised when a mapping file is invalid or references unknown ontology terms.
 */
public class MappingError extends RuntimeException {
    public MappingError(String message) {
        super(message);
    }
}
