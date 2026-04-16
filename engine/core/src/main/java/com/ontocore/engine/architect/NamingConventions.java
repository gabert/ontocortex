package com.ontocore.engine.architect;

import java.util.regex.Pattern;

/**
 * Deterministic name conversions matching the Python implementation exactly.
 *
 * <p>All methods are pure functions with no side effects.</p>
 */
public final class NamingConventions {

    private static final Pattern CAMEL_BOUNDARY = Pattern.compile("(?<!^)(?=[A-Z])");

    private NamingConventions() {}

    /**
     * Convert CamelCase to snake_case.
     * <pre>
     *   "LoanType"   → "loan_type"
     *   "firstName"  → "first_name"
     *   "SSNNumber"  → "s_s_n_number"
     * </pre>
     */
    public static String toSnakeCase(String name) {
        return CAMEL_BOUNDARY.matcher(name).replaceAll("_").toLowerCase();
    }

    /**
     * CamelCase class name to plural snake_case table name.
     * <pre>
     *   "LoanType"  → "loan_types"
     *   "Policy"    → "policies"
     *   "Species"   → "species"
     * </pre>
     */
    public static String toTableName(String className) {
        String s = toSnakeCase(className);
        if (s.endsWith("s")) {
            return s;  // already plural (species, status, etc.)
        }
        if (s.endsWith("y")) {
            return s.substring(0, s.length() - 1) + "ies";
        }
        return s + "s";
    }

    /**
     * CamelCase class name to primary key column name.
     * <pre>
     *   "LoanType" → "loan_type_id"
     * </pre>
     */
    public static String toPkName(String className) {
        return toSnakeCase(className) + "_id";
    }

    /**
     * Extract local name from a full IRI.
     * <pre>
     *   "https://example.org/ontology#Customer" → "Customer"
     *   "https://example.org/ontology/Customer" → "Customer"
     * </pre>
     */
    public static String localName(String iri) {
        int hash = iri.lastIndexOf('#');
        if (hash >= 0) {
            return iri.substring(hash + 1);
        }
        int slash = iri.lastIndexOf('/');
        if (slash >= 0) {
            return iri.substring(slash + 1);
        }
        return iri;
    }
}
