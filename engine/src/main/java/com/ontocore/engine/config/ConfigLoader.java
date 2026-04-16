package com.ontocore.engine.config;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Converts Spring-managed {@link EngineProperties} into the immutable
 * {@link AppConfig} record used throughout the application.
 */
@Configuration
public class ConfigLoader {

    @Bean
    public AppConfig appConfig(EngineProperties props) {
        Map<String, DatabaseConfig> dataSources = new LinkedHashMap<>();
        if (props.getDataSources() != null) {
            props.getDataSources().forEach((name, entry) ->
                    dataSources.put(name, entry.toDatabaseConfig()));
        }

        Path domainsDir = Path.of(props.getDomainsDir());

        // ModelConfig, ChatConfig, ArchitectConfig are only needed by the
        // Python LLM service. The Java engine doesn't call the Anthropic API
        // directly, so we use placeholder values here.
        var models = new ModelConfig("", "", "");
        var chat = new ChatConfig(0, 0, 0, 0);
        var architect = new ArchitectConfig(0, 0, 0, 0, 0, 0);

        return new AppConfig(
                "",  // apiKey — not needed in the Java engine
                models,
                chat,
                architect,
                Map.copyOf(dataSources),
                domainsDir
        );
    }
}
