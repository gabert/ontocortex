package com.ontocore.sif;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ontocore.engine.schema.OntologyMapping;

/**
 * Builds the {@code submit_sif} tool definition for the LLM.
 *
 * <p>Loads a base SIF JSON schema and injects domain-specific enums for
 * entities, relations, and actions. The resulting tool definition constrains
 * the model at generation time — bad names are prevented rather than
 * caught after execution.</p>
 */
public final class ToolSchemaBuilder {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, Object> BASE_SCHEMA;

    static {
        try (InputStream is = ToolSchemaBuilder.class.getResourceAsStream("/sif_schema.json")) {
            if (is == null) throw new IllegalStateException("sif_schema.json not found on classpath");
            BASE_SCHEMA = MAPPER.readValue(is, new TypeReference<>() {});
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load sif_schema.json", e);
        }
    }

    private ToolSchemaBuilder() {}

    /**
     * Build the submit_sif tool definition with domain-specific enums.
     *
     * @param mapping the ontology mapping (provides entity and relation names)
     * @param actions registered domain action names
     * @return the complete tool definition map ({@code name}, {@code description}, {@code input_schema})
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> buildToolSchema(OntologyMapping mapping, Set<String> actions) {
        Map<String, Object> schema = deepCopy(BASE_SCHEMA);

        var opProps = (Map<String, Object>)
                ((Map<String, Object>) ((Map<String, Object>) schema.get("properties"))
                        .get("operations")).get("items");
        opProps = (Map<String, Object>) opProps.get("properties");

        List<String> classNames = mapping.entities().keySet().stream().sorted().toList();
        List<String> relNames = mapping.relations().keySet().stream().sorted().toList();
        List<String> actionNames = actions.stream().sorted().toList();

        if (!classNames.isEmpty()) {
            putEnum(opProps, "entity", classNames);

            var relItemProps = (Map<String, Object>)
                    ((Map<String, Object>) ((Map<String, Object>) opProps.get("relations"))
                            .get("items")).get("properties");
            putEnum(relItemProps, "entity", classNames);

            putNestedEnum(opProps, "from", "entity", classNames);
            putNestedEnum(opProps, "to", "entity", classNames);
        }

        if (!relNames.isEmpty()) {
            var relItemProps = (Map<String, Object>)
                    ((Map<String, Object>) ((Map<String, Object>) opProps.get("relations"))
                            .get("items")).get("properties");
            putEnum(relItemProps, "rel", relNames);

            ((Map<String, Object>) opProps.get("resolve"))
                    .put("propertyNames", Map.of("enum", relNames));

            putEnum(opProps, "relation", relNames);
        }

        if (!actionNames.isEmpty()) {
            putEnum(opProps, "action", actionNames);
        }

        return Map.of(
                "name", "submit_sif",
                "description",
                "Submit structured operations against the domain model. "
                        + "Use ontology class names for entities and ontology property names for fields. "
                        + "Call this whenever the user asks you to look up, create, update, or delete information.",
                "input_schema", schema
        );
    }

    @SuppressWarnings("unchecked")
    private static void putEnum(Map<String, Object> props, String key, List<String> values) {
        ((Map<String, Object>) props.get(key)).put("enum", values);
    }

    @SuppressWarnings("unchecked")
    private static void putNestedEnum(Map<String, Object> props, String outerKey, String innerKey, List<String> values) {
        var outer = (Map<String, Object>) props.get(outerKey);
        var innerProps = (Map<String, Object>) outer.get("properties");
        ((Map<String, Object>) innerProps.get(innerKey)).put("enum", values);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> deepCopy(Map<String, Object> original) {
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(original);
            return MAPPER.readValue(bytes, new TypeReference<>() {});
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deep-copy schema", e);
        }
    }
}
