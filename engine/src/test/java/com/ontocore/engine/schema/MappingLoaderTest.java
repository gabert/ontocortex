package com.ontocore.engine.schema;

import java.util.List;
import java.util.Map;

import com.ontocore.engine.ontology.DatatypeProperty;
import com.ontocore.engine.ontology.ObjectProperty;
import com.ontocore.engine.ontology.OntologyClass;
import com.ontocore.engine.ontology.OntologyModel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the mapping layer — mirrors Python test_mapping.py.
 *
 * <p>Validates that {@link MappingLoader} produces a correct
 * {@link ReferenceMapping} from an ontology + mapping dict.</p>
 */
class MappingLoaderTest {

    static OntologyModel ontology;
    static Map<String, Object> mapping;

    @BeforeAll
    static void setUp() {
        ontology = new OntologyModel(
                Map.of("v", "v#"),
                List.of(
                        new OntologyClass("v#Owner", "v:Owner", "Owner", "owners"),
                        new OntologyClass("v#Pet", "v:Pet", "Pet", "pets"),
                        new OntologyClass("v#Species", "v:Species", "Species", "species")
                ),
                List.of(
                        new ObjectProperty("v#ownsPet", "v:ownsPet", "ownsPet",
                                "v#Owner", "v#Pet", List.of("v#Owner"), List.of("v#Pet"), ""),
                        new ObjectProperty("v#isSpecies", "v:isSpecies", "isSpecies",
                                "v#Pet", "v#Species", List.of("v#Pet"), List.of("v#Species"), "")
                ),
                List.of(
                        new DatatypeProperty("v#firstName", "v:firstName", "firstName", "first_name", List.of("v#Owner"), null, ""),
                        new DatatypeProperty("v#lastName", "v:lastName", "lastName", "last_name", List.of("v#Owner"), null, ""),
                        new DatatypeProperty("v#email", "v:email", "email", "email", List.of("v#Owner"), null, ""),
                        new DatatypeProperty("v#petName", "v:petName", "petName", "pet_name", List.of("v#Pet"), null, ""),
                        new DatatypeProperty("v#breed", "v:breed", "breed", "breed", List.of("v#Pet"), null, ""),
                        new DatatypeProperty("v#weightKg", "v:weightKg", "weightKg", "weight_kg", List.of("v#Pet"), null, ""),
                        new DatatypeProperty("v#speciesName", "v:speciesName", "speciesName", "species_name", List.of("v#Species"), null, "")
                ),
                List.of()
        );

        // Mapping that renames everything — ontology names ≠ physical names
        mapping = Map.of(
                "tables", Map.of(
                        "Owner", Map.of(
                                "table", "clients",
                                "primary_key", "client_id",
                                "columns", Map.of(
                                        "first_name", "fname",
                                        "last_name", "lname",
                                        "email", "email_addr"
                                )
                        ),
                        "Pet", Map.of(
                                "table", "animals",
                                "primary_key", "animal_id",
                                "columns", Map.of(
                                        "pet_name", "name",
                                        "breed", "breed",
                                        "weight_kg", "weight"
                                )
                        ),
                        "Species", Map.of(
                                "table", "species",
                                "primary_key", "species_id",
                                "columns", Map.of(
                                        "species_name", "species_name"
                                )
                        )
                ),
                "relations", Map.of(
                        "ownsPet", Map.of(
                                "type", "direct",
                                "fk_table", "animals",
                                "fk_column", "client_id",
                                "ref_table", "clients",
                                "ref_column", "client_id"
                        ),
                        "isSpecies", Map.of(
                                "type", "direct",
                                "fk_table", "animals",
                                "fk_column", "species_id",
                                "ref_table", "species",
                                "ref_column", "species_id"
                        )
                )
        );
    }

    @Test
    void tablesPopulatedFromMapping() {
        ReferenceMapping smap = MappingLoader.buildFromMapping(ontology, mapping);

        assertThat(smap.entities()).hasSize(3);
        assertThat(smap.entities()).containsKeys("Owner", "Pet", "Species");

        EntityMapping owner = smap.entities().get("Owner");
        assertThat(owner.entityName()).isEqualTo("clients");
        assertThat(owner.primaryKey()).isEqualTo("client_id");
        assertThat(owner.fields()).containsExactlyInAnyOrder("first_name", "last_name", "email");
    }

    @Test
    void physicalFieldResolvesRenames() {
        ReferenceMapping smap = MappingLoader.buildFromMapping(ontology, mapping);
        EntityMapping owner = smap.entities().get("Owner");

        assertThat(owner.physicalField("first_name")).isEqualTo("fname");
        assertThat(owner.physicalField("last_name")).isEqualTo("lname");
        assertThat(owner.physicalField("email")).isEqualTo("email_addr");
    }

    @Test
    void physicalFieldFallsBackToIdentity() {
        ReferenceMapping smap = MappingLoader.buildFromMapping(ontology, mapping);
        EntityMapping pet = smap.entities().get("Pet");

        // "breed" maps to "breed" — same name
        assertThat(pet.physicalField("breed")).isEqualTo("breed");
    }

    @Test
    void relationsPopulated() {
        ReferenceMapping smap = MappingLoader.buildFromMapping(ontology, mapping);

        assertThat(smap.relations()).hasSize(2);
        assertThat(smap.relations()).containsKeys("ownsPet", "isSpecies");

        RelationMap ownsPet = smap.relations().get("ownsPet");
        assertThat(ownsPet.isDirect()).isTrue();
        assertThat(ownsPet.directSourceEntity()).isEqualTo("animals");
        assertThat(ownsPet.directSourceKey()).isEqualTo("client_id");
        assertThat(ownsPet.fromClass()).isEqualTo("Owner");
        assertThat(ownsPet.toClass()).isEqualTo("Pet");
    }

    @Test
    void inboundIndexPopulated() {
        ReferenceMapping smap = MappingLoader.buildFromMapping(ontology, mapping);

        assertThat(smap.inboundIndex()).containsKey("clients");
        var refs = smap.inboundIndex().get("clients");
        assertThat(refs).hasSize(1);
        assertThat(refs.getFirst().entity()).isEqualTo("animals");
        assertThat(refs.getFirst().key()).isEqualTo("client_id");
    }

    @Test
    void tablesByIriPopulated() {
        ReferenceMapping smap = MappingLoader.buildFromMapping(ontology, mapping);

        assertThat(smap.entitiesByIri()).containsKey("v#Owner");
        assertThat(smap.entitiesByIri().get("v#Owner").entityName()).isEqualTo("clients");
    }

    @Test
    void unknownClassThrowsMappingError() {
        Map<String, Object> badMapping = Map.of(
                "tables", Map.of(
                        "Alien", Map.of("table", "aliens", "primary_key", "alien_id")
                ),
                "relations", Map.of()
        );
        assertThatThrownBy(() -> MappingLoader.buildFromMapping(ontology, badMapping))
                .isInstanceOf(MappingError.class)
                .hasMessageContaining("Alien")
                .hasMessageContaining("not in the ontology");
    }

    @Test
    void missingTableFieldThrowsMappingError() {
        Map<String, Object> badMapping = Map.of(
                "tables", Map.of(
                        "Owner", Map.of("primary_key", "id")
                ),
                "relations", Map.of()
        );
        assertThatThrownBy(() -> MappingLoader.buildFromMapping(ontology, badMapping))
                .isInstanceOf(MappingError.class)
                .hasMessageContaining("must have 'table'");
    }

    @Test
    void unknownRelationThrowsMappingError() {
        Map<String, Object> badMapping = Map.of(
                "tables", Map.of(
                        "Owner", Map.of("table", "clients", "primary_key", "client_id"),
                        "Pet", Map.of("table", "animals", "primary_key", "animal_id"),
                        "Species", Map.of("table", "species", "primary_key", "species_id")
                ),
                "relations", Map.of(
                        "unknownRel", Map.of(
                                "type", "direct",
                                "fk_table", "x", "fk_column", "y",
                                "ref_table", "z", "ref_column", "w"
                        )
                )
        );
        assertThatThrownBy(() -> MappingLoader.buildFromMapping(ontology, badMapping))
                .isInstanceOf(MappingError.class)
                .hasMessageContaining("unknownRel")
                .hasMessageContaining("not an object property");
    }

    @Test
    void badRelationTypeThrowsMappingError() {
        Map<String, Object> badMapping = Map.of(
                "tables", Map.of(
                        "Owner", Map.of("table", "clients", "primary_key", "client_id"),
                        "Pet", Map.of("table", "animals", "primary_key", "animal_id"),
                        "Species", Map.of("table", "species", "primary_key", "species_id")
                ),
                "relations", Map.of(
                        "ownsPet", Map.of("type", "graphql")
                )
        );
        assertThatThrownBy(() -> MappingLoader.buildFromMapping(ontology, badMapping))
                .isInstanceOf(MappingError.class)
                .hasMessageContaining("unknown type 'graphql'");
    }
}
