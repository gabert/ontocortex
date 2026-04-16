package com.ontocore.engine.config;

import java.nio.file.Path;
import java.util.Map;

/**
 * Top-level application configuration. Aggregates all config sections.
 */
public record AppConfig(
        String apiKey,
        ModelConfig models,
        ChatConfig chat,
        ArchitectConfig architect,
        Map<String, DatabaseConfig> dataSources,
        Path domainsDir
) {}
