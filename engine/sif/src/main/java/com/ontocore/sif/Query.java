package com.ontocore.sif;

import java.util.List;
import java.util.Map;

/**
 * Read data from an entity, optionally with filters, relations, sorting,
 * aggregation, and pagination.
 */
public record Query(
        String entity,
        List<String> fields,
        Map<String, Object> filters,
        List<Map<String, Object>> relations,
        Map<String, Object> resolve,
        Map<String, Object> aggregate,
        Map<String, Object> sort,
        Integer limit
) implements SifOperation {

    @Override public String op() { return "query"; }
}
