package com.ontocore.engine.config;

/**
 * Schema builder pipeline settings.
 */
public record ArchitectConfig(
        int maxTokens,
        int maxConcurrency,
        int sdkMaxRetries,
        int maxValidationAttempts,
        int rowsPerTable,
        int junctionRows
) {}
