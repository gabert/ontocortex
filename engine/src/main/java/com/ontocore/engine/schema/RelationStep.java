package com.ontocore.engine.schema;

/**
 * A single hop in a relation path between two physical storage entities.
 *
 * <p>Backend-agnostic: in SQL, source/target are tables and keys are columns.
 * In MongoDB, they might be collections and field paths. The mapping file
 * (e.g. mapping.yaml) translates backend-specific vocabulary into these
 * generic terms.</p>
 *
 * @param sourceEntity the entity holding the reference (e.g. child table in SQL)
 * @param sourceKey    the key in the source that points outward (e.g. FK column)
 * @param targetEntity the entity being referenced (e.g. parent table in SQL)
 * @param targetKey    the key being referenced (e.g. PK column)
 */
public record RelationStep(
        String sourceEntity,
        String sourceKey,
        String targetEntity,
        String targetKey
) {}
