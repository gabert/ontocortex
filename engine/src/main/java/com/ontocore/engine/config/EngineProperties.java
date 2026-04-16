package com.ontocore.engine.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds the {@code engine.*} section of application.yaml to typed Java objects.
 *
 * <p>Spring Boot populates this automatically from YAML. The nested maps
 * use relaxed binding (kebab-case in YAML maps to camelCase fields).</p>
 */
@ConfigurationProperties(prefix = "engine")
public class EngineProperties {

    private String domainsDir;
    private Map<String, DataSourceEntry> dataSources;

    public String getDomainsDir() { return domainsDir; }
    public void setDomainsDir(String domainsDir) { this.domainsDir = domainsDir; }

    public Map<String, DataSourceEntry> getDataSources() { return dataSources; }
    public void setDataSources(Map<String, DataSourceEntry> dataSources) { this.dataSources = dataSources; }

    /**
     * A single data source entry matching {@code [data_source:NAME]} from config.ini.
     */
    public static class DataSourceEntry {
        private String dbname;
        private String user;
        private String password;
        private String host;
        private int port;

        public String getDbname() { return dbname; }
        public void setDbname(String dbname) { this.dbname = dbname; }
        public String getUser() { return user; }
        public void setUser(String user) { this.user = user; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }

        public DatabaseConfig toDatabaseConfig() {
            return new DatabaseConfig(dbname, user, password, host, port);
        }
    }
}
