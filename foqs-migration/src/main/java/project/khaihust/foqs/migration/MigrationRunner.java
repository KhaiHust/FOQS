package project.khaihust.foqs.migration;

import config.AppConfig;
import config.ShardConfig;
import liquibase.Liquibase;
import liquibase.database.Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.logging.Logger;

public class MigrationRunner {
    private static final Logger logger = Logger.getLogger(MigrationRunner.class.getName());
    private static final String CHANGELOG = "db/changelogs/db.changelog-master.xml";

    public static void main(String[] args) {
        logger.info("Starting Liquibase migration");
        List<ShardConfig> shardConfigs = AppConfig.getShardConfigs();
        for (ShardConfig shardConfig : shardConfigs) {
            MigrationRunner.migration(shardConfig);
        }
    }

    public static void migration(ShardConfig shardConfig) {
        try {
            Connection conn = DriverManager.getConnection(
                    shardConfig.getJdbcUrl(),
                    shardConfig.getUsername(),
                    shardConfig.getPassword()
            );

            Database database = liquibase.database.DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new liquibase.database.jvm.JdbcConnection(conn));

            Liquibase liquibase = new Liquibase(CHANGELOG, new liquibase.resource.ClassLoaderResourceAccessor(), database);
            liquibase.update("");
            logger.info("Database migration completed successfully.");
        } catch (Exception e) {
            throw new RuntimeException("Error connecting to shard: " + shardConfig.getShardId(), e);
        }
    }
}
