package com.ontocore.sif;

import java.util.Map;

/**
 * Remove an association between two entity instances via a named relation.
 */
public record Unlink(
        String relation,
        Map<String, Object> from,
        Map<String, Object> to
) implements SifOperation {

    @Override public String op() { return "unlink"; }
}
