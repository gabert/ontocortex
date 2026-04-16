package com.ontocore.sif;

import java.util.Map;

/**
 * Insert a new row into an entity, optionally resolving FK references.
 */
public record Create(
        String entity,
        Map<String, Object> data,
        Map<String, Object> resolve
) implements SifOperation {

    @Override public String op() { return "create"; }
}
