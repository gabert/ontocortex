package com.ontocore.engine.config;

/**
 * Conversation agent settings.
 */
public record ChatConfig(
        int maxTokens,
        int maxRetries,
        int retryDelay,
        int maxIterations
) {}
