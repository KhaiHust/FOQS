package config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ShardConfigTest {

    @Test
    @DisplayName("Should build ShardConfig correctly using builder")
    void testBuilderAndGetters() {
        ShardConfig config = ShardConfig.builder()
                .shardId(0)
                .jdbcUrl("jdbc:mysql://localhost:3306/foqs_shard_0")
                .username("root")
                .password("secret")
                .build();

        assertThat(config.getShardId()).isEqualTo(0);
        assertThat(config.getJdbcUrl()).isEqualTo("jdbc:mysql://localhost:3306/foqs_shard_0");
        assertThat(config.getUsername()).isEqualTo("root");
        assertThat(config.getPassword()).isEqualTo("secret");
    }

    @Test
    @DisplayName("Should verify equals, hashCode, and toString")
    void testEqualsAndHashCode() {
        ShardConfig config1 = ShardConfig.builder()
                .shardId(1)
                .jdbcUrl("jdbc:mysql://localhost:3307/foqs_shard_1")
                .username("admin")
                .password("pass123")
                .build();

        ShardConfig config2 = ShardConfig.builder()
                .shardId(1)
                .jdbcUrl("jdbc:mysql://localhost:3307/foqs_shard_1")
                .username("admin")
                .password("pass123")
                .build();

        ShardConfig config3 = ShardConfig.builder()
                .shardId(2)
                .jdbcUrl("jdbc:mysql://localhost:3308/foqs_shard_2")
                .username("admin")
                .password("pass123")
                .build();

        assertThat(config1).isEqualTo(config2);
        assertThat(config1.hashCode()).isEqualTo(config2.hashCode());
        assertThat(config1).isNotEqualTo(config3);
        assertThat(config1.toString()).contains("shardId=1", "jdbc:mysql://localhost:3307/foqs_shard_1");
    }
}
