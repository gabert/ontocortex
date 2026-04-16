package com.ontocore.sif;

import java.util.List;
import java.util.Map;

/**
 * Parses raw JSON maps into typed {@link SifOperation} instances.
 *
 * <p>This is pure structural parsing — no validation against a schema.
 * It checks only that the JSON shape matches one of the 7 operation types
 * and extracts the fields into the appropriate record.</p>
 */
public final class SifParser {

    private SifParser() {}

    /**
     * Parse a list of raw JSON operation maps into typed SIF operations.
     *
     * @throws SifParseError if any operation has an invalid or missing {@code op} field
     */
    public static List<SifOperation> parseAll(List<Map<String, Object>> raw) {
        return raw.stream().map(SifParser::parseOne).toList();
    }

    /**
     * Parse a single raw JSON map into a typed SIF operation.
     *
     * @throws SifParseError if the {@code op} field is missing or unrecognised
     */
    @SuppressWarnings("unchecked")
    public static SifOperation parseOne(Map<String, Object> raw) {
        String op = (String) raw.get("op");
        if (op == null || op.isBlank()) {
            throw new SifParseError("Missing 'op' field in SIF operation.");
        }

        return switch (op) {
            case "query" -> new Query(
                    getString(raw, "entity"),
                    getStringList(raw, "fields"),
                    getMap(raw, "filters"),
                    getMapList(raw, "relations"),
                    getMap(raw, "resolve"),
                    getMap(raw, "aggregate"),
                    getMap(raw, "sort"),
                    getInteger(raw, "limit")
            );
            case "create" -> new Create(
                    getString(raw, "entity"),
                    getMap(raw, "data"),
                    getMap(raw, "resolve")
            );
            case "update" -> new Update(
                    getString(raw, "entity"),
                    getMap(raw, "data"),
                    getMap(raw, "filters")
            );
            case "delete" -> new Delete(
                    getString(raw, "entity"),
                    getMap(raw, "filters")
            );
            case "action" -> new Action(
                    getString(raw, "action"),
                    getMap(raw, "params")
            );
            case "link" -> new Link(
                    getString(raw, "relation"),
                    getMap(raw, "from"),
                    getMap(raw, "to")
            );
            case "unlink" -> new Unlink(
                    getString(raw, "relation"),
                    getMap(raw, "from"),
                    getMap(raw, "to")
            );
            default -> throw new SifParseError(
                    "Unknown op '%s'. Must be one of: query, create, update, delete, action, link, unlink."
                            .formatted(op));
        };
    }

    // ── Extraction helpers ─────────────────────────────────────────────

    private static String getString(Map<String, Object> raw, String key) {
        Object v = raw.get(key);
        return v instanceof String s ? s : null;
    }

    private static Integer getInteger(Map<String, Object> raw, String key) {
        Object v = raw.get(key);
        if (v instanceof Number n) return n.intValue();
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getMap(Map<String, Object> raw, String key) {
        Object v = raw.get(key);
        return v instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
    }

    @SuppressWarnings("unchecked")
    private static List<String> getStringList(Map<String, Object> raw, String key) {
        Object v = raw.get(key);
        return v instanceof List<?> l ? (List<String>) (List<?>) l : null;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getMapList(Map<String, Object> raw, String key) {
        Object v = raw.get(key);
        return v instanceof List<?> l ? (List<Map<String, Object>>) (List<?>) l : null;
    }
}
