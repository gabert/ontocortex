package com.ontocore.engine.schema;

import java.util.List;
import java.util.Map;

/**
 * Maps one ontology class to its physical storage entity (table, collection,
 * sheet, etc.).
 *
 * <p>The {@code fieldMap} bridges ontology property names to physical field
 * names. When empty (auto-generated schema), ontology names ARE the physical
 * names and {@link #physicalField} returns the input unchanged.</p>
 */
public record EntityMapping(
        String className,
        String classIri,
        String entityName,
        String primaryKey,
        List<String> fields,
        Map<String, String> fieldMap,
        String comment
) {
    /**
     * Resolve an ontology property name to its physical field name.
     */
    public String physicalField(String fieldName) {
        return fieldMap.getOrDefault(fieldName, fieldName);
    }
}
