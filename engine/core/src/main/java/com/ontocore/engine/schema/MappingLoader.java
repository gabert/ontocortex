package com.ontocore.engine.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.ontocore.engine.ontology.OntologyModel;
import com.ontocore.engine.ontology.OntologyClass;
import com.ontocore.engine.ontology.ObjectProperty;

/**
 * Builds a {@link ReferenceMapping} from an ontology model + mapping dict
 * (typically loaded from mapping.yaml).
 *
 * <p>The mapping file uses backend-specific vocabulary (e.g. {@code fk_table},
 * {@code fk_column} for SQL). This loader translates that into the generic
 * {@link RelationStep} / {@link RelationMap} types.</p>
 */
public final class MappingLoader {

    private MappingLoader() {}

    @SuppressWarnings("unchecked")
    public static ReferenceMapping buildFromMapping(OntologyModel ontology, Map<String, Object> mapping) {
        Map<String, OntologyClass> classesByLocal = new HashMap<>();
        for (OntologyClass cls : ontology.classes()) {
            classesByLocal.put(cls.localName(), cls);
        }
        Map<String, ObjectProperty> propsByLocal = new HashMap<>();
        for (ObjectProperty p : ontology.objectProperties()) {
            propsByLocal.put(p.localName(), p);
        }

        ReferenceMapping refMap = ReferenceMapping.empty();

        // ── Entities ────────────────────────────────────────────────────
        var tableDefs = (Map<String, Map<String, Object>>) mapping.getOrDefault("tables", Map.of());

        for (var entry : tableDefs.entrySet()) {
            String className = entry.getKey();
            Map<String, Object> tdef = entry.getValue();

            OntologyClass cls = classesByLocal.get(className);
            if (cls == null) {
                throw new MappingError(
                        "Mapping references class '%s' which is not in the ontology. Available: %s"
                                .formatted(className, sorted(classesByLocal.keySet())));
            }

            String classIri = (String) tdef.get("iri");
            if (classIri != null && !classIri.equals(cls.iri())) {
                throw new MappingError(
                        "Class '%s' IRI mismatch: mapping says '%s', ontology says '%s'"
                                .formatted(className, classIri, cls.iri()));
            }

            String entityName = (String) tdef.get("table");
            String primaryKey = (String) tdef.get("primary_key");
            if (entityName == null || primaryKey == null) {
                throw new MappingError(
                        "Table mapping for '%s' must have 'table' and 'primary_key'".formatted(className));
            }

            var colDefs = (Map<String, Object>) tdef.getOrDefault("columns", Map.of());
            List<String> fields = new ArrayList<>();
            Map<String, String> fieldMap = new LinkedHashMap<>();

            for (var colEntry : colDefs.entrySet()) {
                String propName = colEntry.getKey();
                fields.add(propName);
                Object cdef = colEntry.getValue();
                if (cdef instanceof String s) {
                    fieldMap.put(propName, s);
                } else if (cdef instanceof Map<?, ?> m) {
                    fieldMap.put(propName, (String) m.get("column"));
                }
            }

            refMap.putEntity(className, new EntityMapping(
                    className,
                    cls.iri(),
                    entityName,
                    primaryKey,
                    fields,
                    Map.copyOf(fieldMap),
                    cls.comment() != null ? cls.comment() : ""
            ));
        }

        // ── Relations ───────────────────────────────────────────────────
        var relDefs = (Map<String, Map<String, Object>>) mapping.getOrDefault("relations", Map.of());

        for (var entry : relDefs.entrySet()) {
            String relName = entry.getKey();
            Map<String, Object> rdef = entry.getValue();

            ObjectProperty prop = propsByLocal.get(relName);
            if (prop == null) {
                throw new MappingError(
                        "Mapping references relation '%s' which is not an object property in the ontology. Available: %s"
                                .formatted(relName, sorted(propsByLocal.keySet())));
            }

            String relIri = (String) rdef.get("iri");
            if (relIri != null && !relIri.equals(prop.iri())) {
                throw new MappingError(
                        "Relation '%s' IRI mismatch: mapping says '%s', ontology says '%s'"
                                .formatted(relName, relIri, prop.iri()));
            }

            String fromIri = prop.domainIri();
            String toIri = prop.rangeIri();
            EntityMapping fromMap = refMap.entitiesByIri().get(fromIri);
            EntityMapping toMap = refMap.entitiesByIri().get(toIri);
            if (fromMap == null || toMap == null) {
                throw new MappingError(
                        "Relation '%s' endpoints not found in entity mappings. Domain IRI: %s, Range IRI: %s"
                                .formatted(relName, fromIri, toIri));
            }

            String relType = (String) rdef.getOrDefault("type", "direct");

            switch (relType) {
                case "direct" -> {
                    var step = new RelationStep(
                            (String) rdef.get("fk_table"),
                            (String) rdef.get("fk_column"),
                            (String) rdef.get("ref_table"),
                            (String) rdef.get("ref_column")
                    );
                    refMap.putRelation(relName, new RelationMap(
                            relName, prop.iri(),
                            fromMap.className(), toMap.className(),
                            List.of(step), null
                    ));
                    refMap.addInboundRef(step.targetEntity(), step.sourceEntity(), step.sourceKey());
                }
                case "junction" -> {
                    String bridgeEntity = (String) rdef.get("junction_table");
                    if (bridgeEntity == null) {
                        throw new MappingError(
                                "Junction relation '%s' must have 'junction_table'".formatted(relName));
                    }
                    var stepDefs = (List<Map<String, Object>>) rdef.getOrDefault("steps", List.of());
                    List<RelationStep> steps = new ArrayList<>();
                    for (var sdef : stepDefs) {
                        var step = new RelationStep(
                                (String) sdef.get("fk_table"),
                                (String) sdef.get("fk_column"),
                                (String) sdef.get("ref_table"),
                                (String) sdef.get("ref_column")
                        );
                        steps.add(step);
                        refMap.addInboundRef(step.targetEntity(), step.sourceEntity(), step.sourceKey());
                    }
                    refMap.addBridgeEntity(bridgeEntity);
                    refMap.putRelation(relName, new RelationMap(
                            relName, prop.iri(),
                            fromMap.className(), toMap.className(),
                            List.copyOf(steps), bridgeEntity
                    ));
                }
                default -> throw new MappingError(
                        "Relation '%s' has unknown type '%s'. Must be 'direct' or 'junction'."
                                .formatted(relName, relType));
            }
        }

        return refMap;
    }

    private static String sorted(java.util.Collection<String> keys) {
        return String.join(", ", keys.stream().sorted().toList());
    }
}
