package com.ontocore.sif;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ontocore.engine.schema.EntityMapping;
import com.ontocore.engine.schema.OntologyMapping;
import com.ontocore.engine.schema.RelationMap;

/**
 * Validates SIF operations against an {@link OntologyMapping}.
 *
 * <p>Returns actionable error messages the LLM can read and correct.
 * Empty list means valid. No exceptions, no partial execution — the
 * entire batch must pass before any backend sees it.</p>
 *
 * <p>This module never touches a database. It only reads the mapping,
 * so it works for any backend.</p>
 */
public final class SifValidator {

    private static final Set<String> VALID_AGG_FNS = Set.of("count", "sum", "avg", "min", "max");
    private static final Set<String> VALID_SORT_DIRS = Set.of("asc", "desc");

    private SifValidator() {}

    /**
     * Validate a batch of SIF operations.
     *
     * @param operations parsed SIF operations
     * @param mapping    the ontology mapping to validate against
     * @param actions    registered action names (may be empty)
     * @return list of error messages — empty means valid
     */
    public static List<String> validate(
            List<SifOperation> operations,
            OntologyMapping mapping,
            Set<String> actions
    ) {
        List<String> errors = new ArrayList<>();
        for (int i = 0; i < operations.size(); i++) {
            String prefix = operations.size() > 1 ? "Operation " + (i + 1) : "Operation";
            errors.addAll(validateOne(operations.get(i), mapping, actions, prefix));
        }
        return errors;
    }

    // ── Single operation dispatch ──────────────────────────────────────

    private static List<String> validateOne(
            SifOperation op, OntologyMapping mapping, Set<String> actions, String prefix
    ) {
        return switch (op) {
            case Action a -> validateAction(a, actions, prefix);
            case Link l -> validateLinkOp(l.op(), l.relation(), l.from(), l.to(), mapping, prefix);
            case Unlink u -> validateLinkOp(u.op(), u.relation(), u.from(), u.to(), mapping, prefix);
            case Query q -> validateCrud(q.entity(), q.fields(), q.filters(), q.relations(),
                    q.resolve(), q.aggregate(), q.sort(), mapping, prefix);
            case Create c -> validateCrud(c.entity(), null, null, null,
                    c.resolve(), null, null, mapping, prefix,
                    c.data(), "data field");
            case Update u -> validateCrud(u.entity(), null, u.filters(), null,
                    null, null, null, mapping, prefix,
                    u.data(), "data field");
            case Delete d -> validateCrud(d.entity(), null, d.filters(), null,
                    null, null, null, mapping, prefix);
        };
    }

    // ── CRUD validation ────────────────────────────────────────────────

    private static List<String> validateCrud(
            String entity, List<String> fields, Map<String, Object> filters,
            List<Map<String, Object>> relations, Map<String, Object> resolve,
            Map<String, Object> aggregate, Map<String, Object> sort,
            OntologyMapping mapping, String prefix
    ) {
        return validateCrud(entity, fields, filters, relations, resolve, aggregate, sort,
                mapping, prefix, null, null);
    }

    private static List<String> validateCrud(
            String entity, List<String> fields, Map<String, Object> filters,
            List<Map<String, Object>> relations, Map<String, Object> resolve,
            Map<String, Object> aggregate, Map<String, Object> sort,
            OntologyMapping mapping, String prefix,
            Map<String, Object> data, String dataLabel
    ) {
        List<String> errors = new ArrayList<>();

        if (entity == null || entity.isBlank()) {
            return List.of(prefix + ": missing 'entity' field.");
        }

        EntityMapping entityMap = mapping.entities().get(entity);
        if (entityMap == null) {
            String available = sorted(mapping.entities().keySet());
            return List.of(prefix + ": unknown entity '" + entity + "'. Valid entities: " + available);
        }

        errors.addAll(validateFieldNames(fields, entityMap, prefix, "field"));
        errors.addAll(validateFilterFields(filters, entityMap, prefix));
        if (data != null) {
            errors.addAll(validateFieldNames(data.keySet(), entityMap, prefix, dataLabel));
        }
        errors.addAll(validateRelations(relations, mapping, prefix));
        errors.addAll(validateResolve(resolve, entityMap, mapping, prefix));
        errors.addAll(validateAggregate(aggregate, entityMap, prefix));
        errors.addAll(validateSort(sort, entityMap, prefix));

        return errors;
    }

    // ── Field validation ───────────────────────────────────────────────

    private static List<String> validateFieldNames(
            Collection<String> names, EntityMapping entityMap, String prefix, String label
    ) {
        if (names == null || names.isEmpty()) return List.of();

        Set<String> valid = Set.copyOf(entityMap.fields());
        String validStr = sorted(valid);
        List<String> errors = new ArrayList<>();

        for (String f : names) {
            if (!valid.contains(f)) {
                errors.add(prefix + ": unknown " + label + " '" + f + "' on " + entityMap.className()
                        + ". Valid fields: " + validStr);
            }
        }
        return errors;
    }

    private static List<String> validateFilterFields(
            Map<String, Object> filters, EntityMapping entityMap, String prefix
    ) {
        if (filters == null || filters.isEmpty()) return List.of();
        return validateFieldNames(filters.keySet(), entityMap, prefix, "filter field");
    }

    // ── Relation validation (query) ────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static List<String> validateRelations(
            List<Map<String, Object>> relations, OntologyMapping mapping, String prefix
    ) {
        if (relations == null || relations.isEmpty()) return List.of();
        List<String> errors = new ArrayList<>();

        for (int j = 0; j < relations.size(); j++) {
            Map<String, Object> rel = relations.get(j);
            String tag = prefix + ", relation " + (j + 1);

            String relName = (String) rel.get("rel");
            if (relName == null || relName.isBlank()) {
                errors.add(tag + ": missing 'rel' field.");
                continue;
            }
            if (!mapping.relations().containsKey(relName)) {
                errors.add(tag + ": unknown relation '" + relName + "'. Valid relations: "
                        + sorted(mapping.relations().keySet()));
                continue;
            }

            String relEntity = (String) rel.get("entity");
            if (relEntity == null || relEntity.isBlank()) {
                errors.add(tag + ": missing 'entity' field.");
                continue;
            }
            EntityMapping relEntityMap = mapping.entities().get(relEntity);
            if (relEntityMap == null) {
                errors.add(tag + ": unknown entity '" + relEntity + "'. Valid entities: "
                        + sorted(mapping.entities().keySet()));
                continue;
            }

            var relFilters = (Map<String, Object>) rel.get("filters");
            if (relFilters != null) {
                Set<String> relFields = Set.copyOf(relEntityMap.fields());
                for (String f : relFilters.keySet()) {
                    if (!relFields.contains(f)) {
                        errors.add(tag + ": unknown filter field '" + f + "' on " + relEntity
                                + ". Valid fields: " + sorted(relFields));
                    }
                }
            }
        }
        return errors;
    }

    // ── Resolve validation (create) ────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static List<String> validateResolve(
            Map<String, Object> resolve, EntityMapping entityMap,
            OntologyMapping mapping, String prefix
    ) {
        if (resolve == null || resolve.isEmpty()) return List.of();
        List<String> errors = new ArrayList<>();

        for (var entry : resolve.entrySet()) {
            String relName = entry.getKey();
            String tag = prefix + ", resolve '" + relName + "'";

            RelationMap relMap = mapping.relations().get(relName);
            if (relMap == null) {
                errors.add(prefix + ", resolve: unknown relation '" + relName
                        + "'. Valid relations: " + sorted(mapping.relations().keySet()));
                continue;
            }
            if (!relMap.isDirect()) {
                errors.add(prefix + ", resolve: relation '" + relName
                        + "' cannot be resolved in a create (it traverses a bridge entity — use a 'link' op after the create).");
                continue;
            }
            if (!relMap.directSourceEntity().equals(entityMap.entityName())) {
                errors.add(prefix + ", resolve: relation '" + relName
                        + "' cannot be resolved on " + entityMap.className() + " (FK is on a different entity).");
            }

            if (!(entry.getValue() instanceof Map<?, ?> resolveSpec)) continue;

            String resolveEntity = (String) ((Map<String, Object>) resolveSpec).get("entity");
            if (resolveEntity == null) continue;

            EntityMapping resolveEntityMap = mapping.entities().get(resolveEntity);
            if (resolveEntityMap == null) {
                errors.add(tag + ": unknown entity '" + resolveEntity + "'. Valid entities: "
                        + sorted(mapping.entities().keySet()));
            } else {
                var resolveFilters = (Map<String, Object>) ((Map<String, Object>) resolveSpec).get("filters");
                if (resolveFilters != null) {
                    Set<String> resolveFields = Set.copyOf(resolveEntityMap.fields());
                    for (String f : resolveFilters.keySet()) {
                        if (!resolveFields.contains(f)) {
                            errors.add(tag + ": unknown filter field '" + f + "' on " + resolveEntity
                                    + ". Valid fields: " + sorted(resolveFields));
                        }
                    }
                }
            }
        }
        return errors;
    }

    // ── Aggregate + sort validation (query) ────────────────────────────

    private static List<String> validateAggregate(
            Map<String, Object> aggregate, EntityMapping entityMap, String prefix
    ) {
        if (aggregate == null || aggregate.isEmpty()) return List.of();
        List<String> errors = new ArrayList<>();

        String fn = (String) aggregate.get("fn");
        if (!VALID_AGG_FNS.contains(fn)) {
            errors.add(prefix + ": invalid aggregate fn '" + fn + "'. Must be one of: "
                    + sorted(VALID_AGG_FNS));
        }
        String field = (String) aggregate.get("field");
        if (field != null && !Set.copyOf(entityMap.fields()).contains(field)) {
            errors.add(prefix + ": unknown aggregate field '" + field + "' on " + entityMap.className()
                    + ". Valid fields: " + sorted(entityMap.fields()));
        }
        return errors;
    }

    private static List<String> validateSort(
            Map<String, Object> sort, EntityMapping entityMap, String prefix
    ) {
        if (sort == null || sort.isEmpty()) return List.of();
        List<String> errors = new ArrayList<>();

        String field = (String) sort.get("field");
        if (field != null && !Set.copyOf(entityMap.fields()).contains(field)) {
            errors.add(prefix + ": unknown sort field '" + field + "' on " + entityMap.className()
                    + ". Valid fields: " + sorted(entityMap.fields()));
        }
        String dir = (String) sort.getOrDefault("dir", "asc");
        if (!VALID_SORT_DIRS.contains(dir)) {
            errors.add(prefix + ": invalid sort dir '" + dir + "'. Must be 'asc' or 'desc'.");
        }
        return errors;
    }

    // ── Action validation ──────────────────────────────────────────────

    private static List<String> validateAction(Action a, Set<String> actions, String prefix) {
        if (a.action() == null || a.action().isBlank()) {
            return List.of(prefix + ": action op requires 'action' field with the action name.");
        }
        if (!actions.contains(a.action())) {
            String available = actions.isEmpty() ? "(none registered)" : sorted(actions);
            return List.of(prefix + ": unknown action '" + a.action() + "'. Available actions: " + available);
        }
        return List.of();
    }

    // ── Link / unlink validation ───────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static List<String> validateLinkOp(
            String opType, String relation,
            Map<String, Object> from, Map<String, Object> to,
            OntologyMapping mapping, String prefix
    ) {
        if (relation == null || relation.isBlank()) {
            return List.of(prefix + ": " + opType + " op requires 'relation' field.");
        }
        RelationMap relMap = mapping.relations().get(relation);
        if (relMap == null) {
            return List.of(prefix + ": unknown relation '" + relation + "'. Valid relations: "
                    + sorted(mapping.relations().keySet()));
        }
        if (relMap.isDirect()) {
            return List.of(prefix + ": relation '" + relation
                    + "' is a direct FK — use create/update with 'resolve' instead of " + opType + ".");
        }

        if (from == null || from.isEmpty() || to == null || to.isEmpty()) {
            return List.of(prefix + ": " + opType + " op requires 'from' and 'to' with {entity, filters}.");
        }

        String fromEntity = (String) from.get("entity");
        String toEntity = (String) to.get("entity");
        if (fromEntity == null || toEntity == null) {
            return List.of(prefix + ": " + opType + " op requires 'entity' on both 'from' and 'to'.");
        }

        var allowedPair = List.of(relMap.fromClass(), relMap.toClass()).stream().sorted().toList();
        var givenPair = List.of(fromEntity, toEntity).stream().sorted().toList();
        if (!givenPair.equals(allowedPair)) {
            return List.of(prefix + ": " + opType + " endpoints must be " + allowedPair
                    + " for relation '" + relation + "', got " + givenPair + ".");
        }

        List<String> errors = new ArrayList<>();
        for (var side : List.of(Map.entry("from", from), Map.entry("to", to))) {
            String ent = (String) side.getValue().get("entity");
            EntityMapping entityMap = mapping.entities().get(ent);
            if (entityMap == null) {
                errors.add(prefix + ": unknown entity '" + ent + "' on " + opType + "." + side.getKey() + ".");
                continue;
            }
            var filters = (Map<String, Object>) side.getValue().get("filters");
            if (filters == null || filters.isEmpty()) {
                errors.add(prefix + ": " + opType + "." + side.getKey()
                        + " requires 'filters' to locate the " + ent + " row.");
                continue;
            }
            Set<String> validCols = Set.copyOf(entityMap.fields());
            for (String f : filters.keySet()) {
                if (!validCols.contains(f)) {
                    errors.add(prefix + ": " + opType + "." + side.getKey()
                            + " unknown filter field '" + f + "' on " + ent
                            + ". Valid fields: " + sorted(validCols));
                }
            }
        }
        return errors;
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private static String sorted(Collection<String> items) {
        return String.join(", ", items.stream().sorted().toList());
    }
}
