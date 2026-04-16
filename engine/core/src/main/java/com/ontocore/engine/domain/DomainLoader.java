package com.ontocore.engine.domain;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Loads domain configuration from {@code <domains_dir>/<name>/domain.json}.
 *
 * <p>Mirrors the Python {@code load_domain()} function. Resolves data
 * source selection, file paths, and generated artifact locations.</p>
 */
public final class DomainLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DomainLoader() {}

    @SuppressWarnings("unchecked")
    public static DomainConfig load(String domainName, Path domainsDir) throws IOException {
        return load(domainName, domainsDir, null);
    }

    @SuppressWarnings("unchecked")
    public static DomainConfig load(String domainName, Path domainsDir, String dataSourceName) throws IOException {
        Path domainDir = domainsDir.resolve(domainName);
        Path manifestPath = domainDir.resolve("domain.json");

        if (!Files.exists(manifestPath)) {
            List<String> available = listDomains(domainsDir);
            throw new IOException(
                    "Domain '%s' not found at %s. Available: %s"
                            .formatted(domainName, manifestPath, String.join(", ", available)));
        }

        String json = Files.readString(manifestPath, StandardCharsets.UTF_8);
        Map<String, Object> manifest = MAPPER.readValue(json, Map.class);

        // Resolve data source
        String dsName;
        String store;
        String sourceDirRel;
        String mappingFile;

        Map<String, Map<String, Object>> dataSources =
                (Map<String, Map<String, Object>>) manifest.get("data_sources");

        if (dataSources != null && !dataSources.isEmpty()) {
            dsName = dataSourceName != null ? dataSourceName : dataSources.keySet().iterator().next();
            Map<String, Object> ds = dataSources.get(dsName);
            if (ds == null) {
                throw new IllegalArgumentException(
                        "Data source '%s' not found in domain '%s'. Available: %s"
                                .formatted(dsName, domainName, dataSources.keySet()));
            }
            store = (String) ds.get("store");
            sourceDirRel = (String) ds.getOrDefault("source_dir", "data_sources/" + dsName);
            mappingFile = (String) ds.getOrDefault("mapping", "mapping.yaml");
        } else {
            // Legacy format
            dsName = (String) manifest.getOrDefault("database_name", domainName);
            store = (String) manifest.get("database_name");
            sourceDirRel = "data_sources/" + dsName;
            mappingFile = "mapping.yaml";
        }

        Path sourceDir = domainDir.resolve(sourceDirRel);
        Path generatedDir = sourceDir.resolve("_generated");

        Map<String, String> generated = (Map<String, String>) manifest.getOrDefault("generated", Map.of());

        return new DomainConfig(
                domainName,
                (String) manifest.get("name"),
                (String) manifest.get("description"),
                dsName,
                store,
                domainDir.resolve((String) manifest.get("ontology")),
                domainDir.resolve((String) manifest.get("business_rules")),
                domainDir.resolve((String) manifest.get("system_prompt")),
                sourceDir,
                generated.containsKey("schema") ? generatedDir.resolve(generated.get("schema")) : null,
                generated.containsKey("seed_data") ? generatedDir.resolve(generated.get("seed_data")) : null,
                generated.containsKey("ontology_compact") ? generatedDir.resolve(generated.get("ontology_compact")) : null,
                generated.containsKey("schema_plan") ? generatedDir.resolve(generated.get("schema_plan")) : null,
                mappingFile != null ? sourceDir.resolve(mappingFile) : null,
                (String) manifest.get("identity_entity")
        );
    }

    /**
     * Return available domain names found in the given directory.
     */
    public static List<String> listDomains(Path domainsDir) throws IOException {
        if (!Files.isDirectory(domainsDir)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.list(domainsDir)) {
            return entries
                    .filter(Files::isDirectory)
                    .filter(d -> Files.exists(d.resolve("domain.json")))
                    .map(d -> d.getFileName().toString())
                    .sorted()
                    .toList();
        }
    }
}
