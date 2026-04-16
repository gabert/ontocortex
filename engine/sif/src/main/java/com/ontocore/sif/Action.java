package com.ontocore.sif;

import java.util.Map;

/**
 * Invoke a named domain action (e.g. calculate premium, send notification).
 */
public record Action(
        String action,
        Map<String, Object> params
) implements SifOperation {

    @Override public String op() { return "action"; }
}
