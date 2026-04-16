package com.ontocore.engine.schema;

import java.util.List;

/**
 * Maps one ontology object property to its physical relation path.
 *
 * <p>A direct relation has a single {@link RelationStep}; a many-to-many
 * relation passes through a bridge entity with two steps.</p>
 */
public record RelationMap(
        String name,
        String iri,
        String fromClass,
        String toClass,
        List<RelationStep> steps,
        String bridgeEntity
) {
    public boolean isDirect() {
        return steps.size() == 1;
    }

    /**
     * The entity holding the reference in a direct (single-step) relation.
     * @throws IllegalStateException if the relation goes through a bridge
     */
    public String directSourceEntity() {
        requireDirect();
        return steps.getFirst().sourceEntity();
    }

    /**
     * The key in the source entity for a direct (single-step) relation.
     * @throws IllegalStateException if the relation goes through a bridge
     */
    public String directSourceKey() {
        requireDirect();
        return steps.getFirst().sourceKey();
    }

    private void requireDirect() {
        if (!isDirect()) {
            throw new IllegalStateException(
                    "Relation '%s' is not direct (it goes through bridge entity '%s')"
                            .formatted(name, bridgeEntity));
        }
    }
}
