package com.ontocore.engine.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Creates and caches HikariCP {@link DataSource} instances per data source name.
 *
 * <p>Thread-safe. Each data source is created lazily on first access and
 * reused for subsequent requests.</p>
 */
public class DataSourceFactory {

    private final Map<String, DatabaseConfig> configs;
    private final ConcurrentHashMap<String, HikariDataSource> pool = new ConcurrentHashMap<>();

    public DataSourceFactory(Map<String, DatabaseConfig> configs) {
        this.configs = configs;
    }

    public DataSource get(String name) {
        return pool.computeIfAbsent(name, this::create);
    }

    public DataSource get(DatabaseConfig config) {
        return pool.computeIfAbsent(config.jdbcUrl(), key -> createFromConfig(config));
    }

    private HikariDataSource create(String name) {
        var config = configs.get(name);
        if (config == null) {
            throw new IllegalArgumentException(
                    "Unknown data source: '%s'. Available: %s".formatted(name, configs.keySet()));
        }
        return createFromConfig(config);
    }

    private HikariDataSource createFromConfig(DatabaseConfig dbConfig) {
        var hikari = new HikariConfig();
        hikari.setJdbcUrl(dbConfig.jdbcUrl());
        hikari.setUsername(dbConfig.user());
        hikari.setPassword(dbConfig.password());
        hikari.setMaximumPoolSize(5);
        hikari.setConnectionTestQuery("SELECT 1");
        return new HikariDataSource(hikari);
    }

    public void close() {
        pool.values().forEach(HikariDataSource::close);
        pool.clear();
    }
}
