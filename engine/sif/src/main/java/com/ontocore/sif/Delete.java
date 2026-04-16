package com.ontocore.sif;

import java.util.Map;

/**
 * Delete rows from an entity matching the given filters.
 */
public record Delete(
        String entity,
        Map<String, Object> filters
) implements SifOperation {

    @Override public String op() { return "delete"; }
}
