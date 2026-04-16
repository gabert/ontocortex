package com.ontocore.sif;

import java.util.Map;

/**
 * Update existing rows in an entity matching the given filters.
 */
public record Update(
        String entity,
        Map<String, Object> data,
        Map<String, Object> filters
) implements SifOperation {

    @Override public String op() { return "update"; }
}
