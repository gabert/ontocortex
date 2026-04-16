package com.ontocore.engine.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ontocore.engine.ontology.DatatypeProperty;
import com.ontocore.engine.ontology.ObjectProperty;
import com.ontocore.engine.ontology.OntologyClass;
import com.ontocore.engine.ontology.OntologyModel;

/**
 * Derives a mapping dict from an ontology model + schema.json.
 *
 * <p>For architect-generated schemas the column names in schema.json ARE
 * the ontology property names (both snake_case), so the column mapping
 * is an identity ({@code first_name: first_name}). Making it explicit
 * means the runtime always takes the same code path — mapping →
 * SchemaMap — regardless of whether the DB was generated or
 * pre-existing.</p>
 *
 * <p>Returns a dict in the same format that {@link MappingLoader#buildFromMapping}
 * consumes ({@code tables} + {@code relations}).</p>
 */
public final class MappingGenerator {

    private MappingGenerator() {}

    @SuppressWarnings("unchecked")
    public static Map<String, Object> generateFromSchema(OntologyModel ontology, Map<String, Object> schema) {
        Map<String, OntologyClass> classesByIri = new HashMap<>();
        for (OntologyClass cls : ontology.classes()) {
            classesByIri.put(cls.iri(), cls);
        }

        // Index datatype properties by (classIri, snakeName) → property IRI
        Map<String, String> dpIri = new HashMap<>();  // key: classIri + "|" + snakeName
        for (DatatypeProperty dp : ontology.datatypeProperties()) {
            for (String classIri : dp.domainIris()) {
                dpIri.put(classIri + "|" + dp.snakeName(), dp.iri());
            }
        }

        var tablesList = (List<Map<String, Object>>) schema.get("tables");

        // ── Tables ──────────────────────────────────────────────────────
        Map<String, Map<String, Object>> tables = new LinkedHashMap<>();
        Map<String, String> tablePhysToClass = new HashMap<>();

        for (var t : tablesList) {
            if (Boolean.TRUE.equals(t.get("lookup_table"))) continue;

            String iri = (String) t.get("ontology_iri");
            OntologyClass cls = classesByIri.get(iri);
            if (cls == null) continue;

            String className = cls.localName();
            tablePhysToClass.put((String) t.get("name"), className);

            // Identity column mapping with property IRIs
            Map<String, Map<String, Object>> columns = new LinkedHashMap<>();
            var cols = (List<Map<String, Object>>) t.getOrDefault("columns", List.of());
            for (var col : cols) {
                String colName = (String) col.get("name");
                String propIri = dpIri.get(iri + "|" + colName);
                columns.put(colName, Map.of(
                        "iri", propIri != null ? propIri : "",
                        "column", colName
                ));
            }

            tables.put(className, Map.of(
                    "iri", iri,
                    "table", t.get("name"),
                    "primary_key", t.get("primary_key"),
                    "columns", columns
            ));
        }

        // ── Detect junction tables ──────────────────────────────────────
        Set<String> skipCols = Set.of("created_at", "updated_at");
        Set<String> junctionTables = new HashSet<>();
        for (var t : tablesList) {
            if (Boolean.TRUE.equals(t.get("lookup_table"))) continue;
            var fks = (List<Map<String, Object>>) t.getOrDefault("foreign_keys", List.of());
            if (fks.size() < 2) continue;
            var cols = (List<Map<String, Object>>) t.getOrDefault("columns", List.of());
            boolean hasDataCols = cols.stream()
                    .anyMatch(c -> !skipCols.contains(c.get("name")));
            if (!hasDataCols) {
                junctionTables.add((String) t.get("name"));
            }
        }

        Map<String, Map<String, Object>> tablesByName = new HashMap<>();
        for (var t : tablesList) {
            tablesByName.put((String) t.get("name"), t);
        }

        // ── Relations ───────────────────────────────────────────────────
        Map<String, Object> relations = new LinkedHashMap<>();

        for (ObjectProperty prop : ontology.objectProperties()) {
            OntologyClass fromCls = classesByIri.get(prop.domainIri());
            OntologyClass toCls = classesByIri.get(prop.rangeIri());
            if (fromCls == null || toCls == null) continue;

            var fromDef = tables.get(fromCls.localName());
            var toDef = tables.get(toCls.localName());
            if (fromDef == null || toDef == null) continue;

            String fromPhys = (String) fromDef.get("table");
            String toPhys = (String) toDef.get("table");

            // Try direct FK (either direction)
            String[] direct = findDirectFk(tablesByName, fromPhys, toPhys);
            if (direct == null) {
                direct = findDirectFk(tablesByName, toPhys, fromPhys);
            }

            if (direct != null) {
                relations.put(prop.localName(), Map.of(
                        "iri", prop.iri(),
                        "type", "direct",
                        "fk_table", direct[0],
                        "fk_column", direct[1],
                        "ref_table", direct[2],
                        "ref_column", direct[3]
                ));
                continue;
            }

            // Try junction path
            for (String jname : junctionTables.stream().sorted().toList()) {
                var j = tablesByName.get(jname);
                var jFks = (List<Map<String, Object>>) j.getOrDefault("foreign_keys", List.of());

                Map<String, Object> fkFrom = null, fkTo = null;
                for (var fk : jFks) {
                    if (fromPhys.equals(fk.get("references_table"))) fkFrom = fk;
                    if (toPhys.equals(fk.get("references_table"))) fkTo = fk;
                }

                if (fkFrom != null && fkTo != null) {
                    relations.put(prop.localName(), Map.of(
                            "iri", prop.iri(),
                            "type", "junction",
                            "junction_table", jname,
                            "steps", List.of(
                                    Map.of(
                                            "fk_table", jname,
                                            "fk_column", fkFrom.get("column"),
                                            "ref_table", fromPhys,
                                            "ref_column", fkFrom.get("references_column")
                                    ),
                                    Map.of(
                                            "fk_table", jname,
                                            "fk_column", fkTo.get("column"),
                                            "ref_table", toPhys,
                                            "ref_column", fkTo.get("references_column")
                                    )
                            )
                    ));
                    break;
                }
            }
        }

        return Map.of("tables", tables, "relations", relations);
    }

    @SuppressWarnings("unchecked")
    private static String[] findDirectFk(
            Map<String, Map<String, Object>> tablesByName,
            String childName, String parentName
    ) {
        var child = tablesByName.get(childName);
        if (child == null) return null;
        var fks = (List<Map<String, Object>>) child.getOrDefault("foreign_keys", List.of());
        for (var fk : fks) {
            if (parentName.equals(fk.get("references_table"))) {
                return new String[]{
                        childName, (String) fk.get("column"),
                        parentName, (String) fk.get("references_column")
                };
            }
        }
        return null;
    }
}
