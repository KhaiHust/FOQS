package project.khaihust.foqs.core.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import config.AppConfig;
import config.ShardConfig;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class DatasourceManager implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(DatasourceManager.class.getName());
    private final Map<Integer, HikariDataSource> shardDatasources = new HashMap<>();

    public DatasourceManager(List<ShardConfig> shardConfigs) {
        for (ShardConfig shardConfig : shardConfigs) {
            var dataSource = createShardDataSource(shardConfig);
            shardDatasources.put(shardConfig.getShardId(), dataSource);
            logger.info("Initialized datasource for shard: " + shardConfig.getShardId());
        }
    }

    private HikariDataSource createShardDataSource(ShardConfig shardConfig) {
        var hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(shardConfig.getJdbcUrl());
        hikariConfig.setUsername(shardConfig.getUsername());
        hikariConfig.setPassword(shardConfig.getPassword());

        int maxPoolSize = AppConfig.getIntProperty("foqs.datasource.max-pool-size", 20);
        int minIdle = AppConfig.getIntProperty("foqs.datasource.min-idle", 5);
        long idleTimeout = AppConfig.getIntProperty("foqs.datasource.idle-timeout-ms", 30000);
        long connectionTimeout = AppConfig.getIntProperty("foqs.datasource.connection-timeout-ms", 2000);

        hikariConfig.setMaximumPoolSize(maxPoolSize);
        hikariConfig.setMinimumIdle(minIdle);
        hikariConfig.setIdleTimeout(idleTimeout);
        hikariConfig.setConnectionTimeout(connectionTimeout);
        hikariConfig.setMaxLifetime(600000);

        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");
        hikariConfig.addDataSourceProperty("useLocalSessionState", "true");
        hikariConfig.addDataSourceProperty("rewriteBatchedStatements", "true");
        hikariConfig.addDataSourceProperty("cacheResultSetMetadata", "true");
        hikariConfig.addDataSourceProperty("cacheServerConfiguration", "true");
        hikariConfig.addDataSourceProperty("elideSetAutoCommits", "true");
        hikariConfig.addDataSourceProperty("maintainTimeStats", "false");

        return new HikariDataSource(hikariConfig);

    }

    public DataSource  getDataSource(int shardId) {
        var ds = shardDatasources.get(shardId);
        if(ds == null) {
            throw new IllegalStateException("No shard config found for shard: " + shardId);
        }
        return ds;
    }

    public Map<Integer, DataSource> getAllDatasources() {
        return Collections.unmodifiableMap(shardDatasources);
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<Integer, HikariDataSource> entry : shardDatasources.entrySet()) {
            try {
                entry.getValue().close();
                logger.info("Closed datasource for shard: " + entry.getKey());
            } catch (Exception e) {
                logger.severe("Error closing datasource for shard: " + entry.getKey() + ". Error: " + e.getMessage());
            }
        }
    }
}
