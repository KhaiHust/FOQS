package project.khaihust.foqs.core.config;

import com.zaxxer.hikari.HikariDataSource;
import config.ShardConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatasourceManagerTest {

    @Test
    @DisplayName("Should initialize datasources for all configured shards and allow obtaining connections")
    void testInitializationAndGetDataSource() throws Exception {
        List<ShardConfig> shardConfigs = List.of(
                ShardConfig.builder()
                        .shardId(0)
                        .jdbcUrl("jdbc:h2:mem:shard_ds_0;DB_CLOSE_DELAY=-1")
                        .username("sa")
                        .password("")
                        .build(),
                ShardConfig.builder()
                        .shardId(1)
                        .jdbcUrl("jdbc:h2:mem:shard_ds_1;DB_CLOSE_DELAY=-1")
                        .username("sa")
                        .password("")
                        .build()
        );

        try (DatasourceManager manager = new DatasourceManager(shardConfigs)) {
            DataSource ds0 = manager.getDataSource(0);
            DataSource ds1 = manager.getDataSource(1);

            assertThat(ds0).isNotNull();
            assertThat(ds1).isNotNull();

            // Verify connections can be acquired
            try (Connection conn0 = ds0.getConnection();
                 Connection conn1 = ds1.getConnection()) {
                assertThat(conn0.isValid(1)).isTrue();
                assertThat(conn1.isValid(1)).isTrue();
            }
        }
    }

    @Test
    @DisplayName("Should throw IllegalStateException when requesting non-existent shard")
    void testGetDataSource_NonExistentShard() throws Exception {
        List<ShardConfig> shardConfigs = List.of(
                ShardConfig.builder()
                        .shardId(0)
                        .jdbcUrl("jdbc:h2:mem:shard_ds_single;DB_CLOSE_DELAY=-1")
                        .username("sa")
                        .password("")
                        .build()
        );

        try (DatasourceManager manager = new DatasourceManager(shardConfigs)) {
            assertThatThrownBy(() -> manager.getDataSource(99))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("No shard config found for shard: 99");
        }
    }

    @Test
    @DisplayName("Should return unmodifiable map of all datasources")
    void testGetAllDatasources() throws Exception {
        List<ShardConfig> shardConfigs = List.of(
                ShardConfig.builder()
                        .shardId(0)
                        .jdbcUrl("jdbc:h2:mem:shard_ds_unmod;DB_CLOSE_DELAY=-1")
                        .username("sa")
                        .password("")
                        .build()
        );

        try (DatasourceManager manager = new DatasourceManager(shardConfigs)) {
            Map<Integer, DataSource> allDs = manager.getAllDatasources();
            assertThat(allDs).hasSize(1);
            assertThat(allDs.containsKey(0)).isTrue();

            assertThatThrownBy(() -> allDs.remove(0))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    @DisplayName("Should close all underlying Hikari datasources upon close()")
    void testClose() throws Exception {
        List<ShardConfig> shardConfigs = List.of(
                ShardConfig.builder()
                        .shardId(0)
                        .jdbcUrl("jdbc:h2:mem:shard_ds_close;DB_CLOSE_DELAY=-1")
                        .username("sa")
                        .password("")
                        .build()
        );

        DatasourceManager manager = new DatasourceManager(shardConfigs);
        HikariDataSource ds0 = (HikariDataSource) manager.getDataSource(0);
        assertThat(ds0.isClosed()).isFalse();

        manager.close();
        assertThat(ds0.isClosed()).isTrue();
    }
}
