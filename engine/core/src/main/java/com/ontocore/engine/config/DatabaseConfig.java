package com.ontocore.engine.config;

/**
 * Connection credentials for a single PostgreSQL data source.
 */
public record DatabaseConfig(
        String dbname,
        String user,
        String password,
        String host,
        int port
) {
    public String jdbcUrl() {
        return "jdbc:postgresql://%s:%d/%s".formatted(host, port, dbname);
    }
}
