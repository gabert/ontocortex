package com.ontocore.engine.config;

/**
 * LLM model selection for different pipeline roles.
 */
public record ModelConfig(
        String chat,
        String seedData,
        String analyzer
) {}
