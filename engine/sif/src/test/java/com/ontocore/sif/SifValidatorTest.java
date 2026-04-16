package com.ontocore.sif;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ontocore.engine.schema.EntityMapping;
import com.ontocore.engine.schema.OntologyMapping;
import com.ontocore.engine.schema.RelationMap;
import com.ontocore.engine.schema.RelationStep;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for SIF validation — mirrors Python validation.py test cases.
 */
class SifValidatorTest {

    static OntologyMapping mapping;
    static Set<String> actions;

    @BeforeAll
    static void setUp() {
        // Build a simple mapping: Owner, Pet, Species
        // ownsPet: direct FK (animals.client_id → clients.client_id)
        // hasTreatment: junction (owner_treatments bridge)
        var ownerEntity = new EntityMapping(
                "Owner", "v#Owner", "clients", "client_id",
                List.of("first_name", "last_name", "email"),
                Map.of(), "owners"
        );
        var petEntity = new EntityMapping(
                "Pet", "v#Pet", "animals", "animal_id",
                List.of("pet_name", "breed", "weight_kg"),
                Map.of(), "pets"
        );
        var speciesEntity = new EntityMapping(
                "Species", "v#Species", "species", "species_id",
                List.of("species_name"),
                Map.of(), "species"
        );

        var ownsPet = new RelationMap("ownsPet", "v#ownsPet", "Owner", "Pet",
                List.of(new RelationStep("animals", "client_id", "clients", "client_id")),
                null);

        var hasTreatment = new RelationMap("hasTreatment", "v#hasTreatment", "Owner", "Species",
                List.of(
                        new RelationStep("owner_treatments", "owner_id", "clients", "client_id"),
                        new RelationStep("owner_treatments", "species_id", "species", "species_id")
                ),
                "owner_treatments");

        mapping = new OntologyMapping() {
            private final Map<String, EntityMapping> entities = Map.of(
                    "Owner", ownerEntity, "Pet", petEntity, "Species", speciesEntity);
            private final Map<String, EntityMapping> byIri = Map.of(
                    "v#Owner", ownerEntity, "v#Pet", petEntity, "v#Species", speciesEntity);
            private final Map<String, RelationMap> relations = Map.of(
                    "ownsPet", ownsPet, "hasTreatment", hasTreatment);

            @Override public Map<String, EntityMapping> entities() { return entities; }
            @Override public Map<String, EntityMapping> entitiesByIri() { return byIri; }
            @Override public Map<String, RelationMap> relations() { return relations; }
        };

        actions = Set.of("calculate_premium");
    }

    // ── Valid operations ───────────────────────────────────────────────

    @Test
    void validQueryPasses() {
        var ops = List.<SifOperation>of(new Query(
                "Owner", List.of("first_name", "email"),
                Map.of("last_name", "Smith"), null, null, null, null, null));

        assertThat(SifValidator.validate(ops, mapping, actions)).isEmpty();
    }

    @Test
    void validCreatePasses() {
        var ops = List.<SifOperation>of(new Create(
                "Owner", Map.of("first_name", "Alice", "email", "a@b.com"), null));

        assertThat(SifValidator.validate(ops, mapping, actions)).isEmpty();
    }

    @Test
    void validUpdatePasses() {
        var ops = List.<SifOperation>of(new Update(
                "Pet", Map.of("breed", "Labrador"), Map.of("pet_name", "Rex")));

        assertThat(SifValidator.validate(ops, mapping, actions)).isEmpty();
    }

    @Test
    void validDeletePasses() {
        var ops = List.<SifOperation>of(new Delete("Pet", Map.of("pet_name", "Rex")));

        assertThat(SifValidator.validate(ops, mapping, actions)).isEmpty();
    }

    @Test
    void validActionPasses() {
        var ops = List.<SifOperation>of(new Action("calculate_premium", Map.of("policy_id", 1)));

        assertThat(SifValidator.validate(ops, mapping, actions)).isEmpty();
    }

    @Test
    void validLinkPasses() {
        var ops = List.<SifOperation>of(new Link("hasTreatment",
                Map.of("entity", "Owner", "filters", Map.of("first_name", "Alice")),
                Map.of("entity", "Species", "filters", Map.of("species_name", "Dog"))));

        assertThat(SifValidator.validate(ops, mapping, actions)).isEmpty();
    }

    // ── Entity errors ──────────────────────────────────────────────────

    @Test
    void unknownEntityReturnsError() {
        var ops = List.<SifOperation>of(new Query("Alien", null, null, null, null, null, null, null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("unknown entity 'Alien'").contains("Valid entities:");
    }

    @Test
    void missingEntityReturnsError() {
        var ops = List.<SifOperation>of(new Query(null, null, null, null, null, null, null, null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("missing 'entity'");
    }

    // ── Field errors ───────────────────────────────────────────────────

    @Test
    void unknownFieldReturnsError() {
        var ops = List.<SifOperation>of(new Query(
                "Owner", List.of("first_name", "nonexistent"), null, null, null, null, null, null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("unknown field 'nonexistent'");
    }

    @Test
    void unknownFilterFieldReturnsError() {
        var ops = List.<SifOperation>of(new Query(
                "Owner", null, Map.of("bad_field", "x"), null, null, null, null, null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("unknown filter field 'bad_field'");
    }

    @Test
    void unknownDataFieldReturnsError() {
        var ops = List.<SifOperation>of(new Create("Owner", Map.of("nonexistent", "val"), null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("unknown data field 'nonexistent'");
    }

    // ── Relation errors ────────────────────────────────────────────────

    @Test
    void unknownRelationReturnsError() {
        var ops = List.<SifOperation>of(new Query(
                "Owner", null, null,
                List.of(Map.<String, Object>of("rel", "badRel", "entity", "Pet")),
                null, null, null, null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("unknown relation 'badRel'");
    }

    // ── Aggregate errors ───────────────────────────────────────────────

    @Test
    void invalidAggregateFnReturnsError() {
        var ops = List.<SifOperation>of(new Query(
                "Owner", null, null, null, null,
                Map.of("fn", "median"), null, null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("invalid aggregate fn 'median'");
    }

    // ── Sort errors ────────────────────────────────────────────────────

    @Test
    void unknownSortFieldReturnsError() {
        var ops = List.<SifOperation>of(new Query(
                "Owner", null, null, null, null, null,
                Map.of("field", "nonexistent"), null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("unknown sort field 'nonexistent'");
    }

    @Test
    void invalidSortDirReturnsError() {
        var ops = List.<SifOperation>of(new Query(
                "Owner", null, null, null, null, null,
                Map.of("field", "first_name", "dir", "sideways"), null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("invalid sort dir 'sideways'");
    }

    // ── Action errors ──────────────────────────────────────────────────

    @Test
    void unknownActionReturnsError() {
        var ops = List.<SifOperation>of(new Action("nuke_database", null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("unknown action 'nuke_database'");
    }

    @Test
    void missingActionNameReturnsError() {
        var ops = List.<SifOperation>of(new Action(null, null));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("requires 'action' field");
    }

    // ── Link errors ────────────────────────────────────────────────────

    @Test
    void linkOnDirectRelationReturnsError() {
        var ops = List.<SifOperation>of(new Link("ownsPet",
                Map.of("entity", "Owner", "filters", Map.of("first_name", "Alice")),
                Map.of("entity", "Pet", "filters", Map.of("pet_name", "Rex"))));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("direct FK").contains("resolve");
    }

    @Test
    void linkWithWrongEndpointsReturnsError() {
        var ops = List.<SifOperation>of(new Link("hasTreatment",
                Map.of("entity", "Pet", "filters", Map.of("pet_name", "Rex")),
                Map.of("entity", "Species", "filters", Map.of("species_name", "Dog"))));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).contains("endpoints must be");
    }

    @Test
    void linkMissingFiltersReturnsError() {
        var ops = List.<SifOperation>of(new Link("hasTreatment",
                Map.of("entity", "Owner"),
                Map.of("entity", "Species", "filters", Map.of("species_name", "Dog"))));

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).anyMatch(e -> e.contains("requires 'filters'"));
    }

    // ── Batch numbering ────────────────────────────────────────────────

    @Test
    void batchErrorsIncludeOperationNumber() {
        var ops = List.<SifOperation>of(
                new Query("Owner", null, null, null, null, null, null, null),
                new Query("Alien", null, null, null, null, null, null, null)
        );

        var errors = SifValidator.validate(ops, mapping, actions);
        assertThat(errors).hasSize(1);
        assertThat(errors.getFirst()).startsWith("Operation 2:");
    }
}
