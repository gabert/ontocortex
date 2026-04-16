package com.ontocore.engine.domain;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Immutable snapshot of a domain's configuration and file paths.
 *
 * <p>Each domain is effectively a tenant — it owns an isolated ontology,
 * data source, schema, business rules, and persona. We keep the name
 * "domain" to avoid collision with OWL's domain/range vocabulary.</p>
 *
 * <p>All paths are resolved at construction time by {@link DomainLoader}.</p>
 */
public record DomainConfig(
        String dirName,
        String name,
        String description,
        String dataSource,
        String store,
        Path ontologyPath,
        Path rulesPath,
        Path promptPath,
        Path sourceDir,
        Path schemaPath,
        Path seedDataPath,
        Path ontologyCompactPath,
        Path schemaPlanPath,
        Path mappingPath,
        String identityEntity
) {

    public Path domainDir() {
        return ontologyPath.getParent();
    }

    public Path generatedDir() {
        return sourceDir.resolve("_generated");
    }

    public Path overridesPath() {
        return sourceDir.resolve("overrides.yaml");
    }

    public String ontologyText() throws IOException {
        return Files.readString(ontologyPath, StandardCharsets.UTF_8);
    }

    public String rulesText() throws IOException {
        return Files.readString(rulesPath, StandardCharsets.UTF_8);
    }

    public String promptText() throws IOException {
        return Files.readString(promptPath, StandardCharsets.UTF_8);
    }

    public boolean hasMapping() {
        return mappingPath != null && Files.exists(mappingPath);
    }

    public boolean hasDesignedSchema() {
        return schemaPath != null && Files.exists(schemaPath);
    }
}
