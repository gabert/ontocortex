package com.ontocore.engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.ontocore.engine.config.EngineProperties;

@SpringBootApplication
@EnableConfigurationProperties(EngineProperties.class)
public class EngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(EngineApplication.class, args);
    }
}
