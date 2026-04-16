package com.ontocore.engine.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ontocore.engine.ontology.OntologyModel;
import com.ontocore.engine.ontology.ObjectProperty;
import com.ontocore.engine.ontology.OntologyClass;

/**
 * Reference-based ontology mapping for relational and key-based backends.
 *
 * <p>Entities reference each other via keys (foreign keys in SQL, reference
 * fields in document stores). Many-to-many relationships go through bridge
 * entities. This is the natural mapping for SQL databases, but also works
 * for any backend where entities point at each other by key.</p>
 *
 * <p>Two construction paths:</p>
 * <ul>
 *   <li>{@link #fromSchema} — from ontology model + schema.json (auto-generated)</li>
 *   <li>{@link MappingLoader#buildFromMapping} — from ontology model + mapping.yaml</li>
 * </ul>
 */
public final class ReferenceMapping implements OntologyMapping {

    private final Map<String, EntityMapping> entities = new LinkedHashMap<>();
    private final Map<String, EntityMapping> entitiesByIri = new HashMap<>();
    private final Map<String, RelationMap> relations = new LinkedHashMap<>();
    private final Set<String> bridgeEntities = new HashSet<>();
    private final Map<String, List<InboundRef>> inboundIndex = new HashMap<>();

    /**
     * An inbound reference: another entity's key points at the indexed entity.
     */
    public record InboundRef(String entity, String key) {}

    // ── OntologyMapping interface ───────────────────────────────────────

    @Override public Map<String, EntityMapping> entities() { return entities; }
    @Override public Map<String, EntityMapping> entitiesByIri() { return entitiesByIri; }
    @Override public Map<String, RelationMap> relations() { return relations; }

    // ── Reference-model-specific accessors ───────────────────────────────

    /** Entities that serve only as M:N bridges (junction tables in SQL). */
    public Set<String> bridgeEntities() { return bridgeEntities; }

    /** Target entity → list of (source entity, source key) that reference it. */
    public Map<String, List<InboundRef>> inboundIndex() { return inboundIndex; }

    // ── Construction from schema.json ───────────────────────────────────

    @SuppressWarnings("unchecked")
    public static ReferenceMapping fromSchema(OntologyModel ontology, Map<String, Object> schema) {
        var mapping = new ReferenceMapping();
        mapping.buildEntities(ontology, schema);
        mapping.buildInboundIndex(schema);
        mapping.detectBridgeEntities(schema);
        mapping.buildRelations(ontology, schema);
        return mapping;
    }

    // ── Package-private for MappingLoader ───────────────────────────────

    static ReferenceMapping empty() {
        return new ReferenceMapping();
    }

    void putEntity(String className, EntityMapping entity) {
        entities.put(className, entity);
        entitiesByIri.put(entity.classIri(), entity);
    }

    void putRelation(String name, RelationMap rmap) {
        relations.put(name, rmap);
    }

    void addBridgeEntity(String name) {
        bridgeEntities.add(name);
    }

    void addInboundRef(String targetEntity, String sourceEntity, String sourceKey) {
        inboundIndex.computeIfAbsent(targetEntity, k -> new ArrayList<>())
                .add(new InboundRef(sourceEntity, sourceKey));
    }

    // ── Private: build from schema.json ─────────────────────────────────

    @SuppressWarnings("unchecked")
    private void buildEntities(OntologyModel ontology, Map<String, Object> schema) {
        var tablesList = (List<Map<String, Object>>) schema.get("tables");

        Map<String, Map<String, Object>> tablesByComment = new HashMap<>();
        for (var t : tablesList) {
            String comment = stringOrEmpty(t.get("comment")).trim();
            if (!comment.isEmpty()) {
                tablesByComment.putIfAbsent(comment, t);
            }
        }

        for (OntologyClass cls : ontology.classes()) {
            String iri = cls.iri();
            String local = cls.localName();
            String comment = (cls.comment() != null ? cls.comment() : "").trim();

            Map<String, Object> table = null;
            for (var t : tablesList) {
                if (iri.equals(t.get("ontology_iri"))) {
                    table = t;
                    break;
                }
            }
            if (table == null && !comment.isEmpty()) {
                table = tablesByComment.get(comment);
            }
            if (table == null) continue;

            var cols = (List<Map<String, Object>>) table.getOrDefault("columns", List.of());
            List<String> fieldNames = cols.stream().map(c -> (String) c.get("name")).toList();

            var entity = new EntityMapping(
                    local, iri,
                    (String) table.get("name"),
                    (String) table.get("primary_key"),
                    fieldNames,
                    Map.of(),
                    stringOrEmpty(table.get("comment"))
            );
            entities.put(local, entity);
            entitiesByIri.put(iri, entity);
        }
    }

    @SuppressWarnings("unchecked")
    private void buildInboundIndex(Map<String, Object> schema) {
        var tablesList = (List<Map<String, Object>>) schema.get("tables");
        for (var t : tablesList) {
            var fks = (List<Map<String, Object>>) t.getOrDefault("foreign_keys", List.of());
            for (var fk : fks) {
                String refTable = (String) fk.get("references_table");
                addInboundRef(refTable, (String) t.get("name"), (String) fk.get("column"));
            }
        }
    }

    private static final Set<String> SKIP_FIELD_NAMES = Set.of("created_at", "updated_at");

    @SuppressWarnings("unchecked")
    private void detectBridgeEntities(Map<String, Object> schema) {
        var tablesList = (List<Map<String, Object>>) schema.get("tables");
        for (var t : tablesList) {
            if (Boolean.TRUE.equals(t.get("lookup_table"))) continue;
            var fks = (List<Map<String, Object>>) t.getOrDefault("foreign_keys", List.of());
            if (fks.size() < 2) continue;
            var cols = (List<Map<String, Object>>) t.getOrDefault("columns", List.of());
            boolean hasDataFields = cols.stream()
                    .anyMatch(c -> !SKIP_FIELD_NAMES.contains(c.get("name")));
            if (!hasDataFields) {
                bridgeEntities.add((String) t.get("name"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void buildRelations(OntologyModel ontology, Map<String, Object> schema) {
        var tablesList = (List<Map<String, Object>>) schema.get("tables");
        Map<String, Map<String, Object>> tablesByName = new HashMap<>();
        for (var t : tablesList) {
            tablesByName.put((String) t.get("name"), t);
        }

        for (ObjectProperty prop : ontology.objectProperties()) {
            String fromIri = prop.domainIri();
            String toIri = prop.rangeIri();
            if (fromIri == null || toIri == null) continue;

            EntityMapping fromMap = entitiesByIri.get(fromIri);
            EntityMapping toMap = entitiesByIri.get(toIri);
            if (fromMap == null || toMap == null) continue;

            var pathResult = findRelationPath(tablesByName, fromMap, toMap);
            if (pathResult.steps.isEmpty()) continue;

            relations.put(prop.localName(), new RelationMap(
                    prop.localName(), prop.iri(),
                    fromMap.className(), toMap.className(),
                    pathResult.steps, pathResult.bridge
            ));
        }
    }

    private record PathResult(List<RelationStep> steps, String bridge) {}

    private PathResult findRelationPath(
            Map<String, Map<String, Object>> tablesByName,
            EntityMapping fromMap, EntityMapping toMap
    ) {
        RelationStep direct = directRef(tablesByName, fromMap.entityName(), toMap.entityName());
        if (direct != null) return new PathResult(List.of(direct), null);

        direct = directRef(tablesByName, toMap.entityName(), fromMap.entityName());
        if (direct != null) return new PathResult(List.of(direct), null);

        for (String bridgeName : bridgeEntities) {
            var bridge = tablesByName.get(bridgeName);
            @SuppressWarnings("unchecked")
            var bridgeRefs = (List<Map<String, Object>>) bridge.getOrDefault("foreign_keys", List.of());

            Map<String, Object> refToFrom = null, refToTo = null;
            for (var fk : bridgeRefs) {
                if (fromMap.entityName().equals(fk.get("references_table"))) refToFrom = fk;
                if (toMap.entityName().equals(fk.get("references_table"))) refToTo = fk;
            }

            if (refToFrom != null && refToTo != null) {
                var step1 = new RelationStep(
                        bridgeName, (String) refToFrom.get("column"),
                        fromMap.entityName(), (String) refToFrom.get("references_column")
                );
                var step2 = new RelationStep(
                        bridgeName, (String) refToTo.get("column"),
                        toMap.entityName(), (String) refToTo.get("references_column")
                );
                return new PathResult(List.of(step1, step2), bridgeName);
            }
        }

        return new PathResult(List.of(), null);
    }

    @SuppressWarnings("unchecked")
    private static RelationStep directRef(
            Map<String, Map<String, Object>> tablesByName,
            String childName, String parentName
    ) {
        var child = tablesByName.get(childName);
        if (child == null) return null;
        var fks = (List<Map<String, Object>>) child.getOrDefault("foreign_keys", List.of());
        for (var fk : fks) {
            if (parentName.equals(fk.get("references_table"))) {
                return new RelationStep(
                        childName, (String) fk.get("column"),
                        parentName, (String) fk.get("references_column")
                );
            }
        }
        return null;
    }

    private static String stringOrEmpty(Object o) {
        return o != null ? o.toString() : "";
    }
}
