package config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AppConfigTest {

    @Test
    @DisplayName("Should return property value when key exists")
    void testGetProperty_ExistingKey() {
        String value = AppConfig.getProperty("foqs.test.key", "default-val");
        assertThat(value).isEqualTo("foqs-test-value");
    }

    @Test
    @DisplayName("Should return default value when key does not exist")
    void testGetProperty_NonExistingKey() {
        String value = AppConfig.getProperty("non.existing.key", "fallback");
        assertThat(value).isEqualTo("fallback");
    }

    @Test
    @DisplayName("Should return integer property when key exists and is valid")
    void testGetIntProperty_ExistingKey() {
        int value = AppConfig.getIntProperty("foqs.test.number", 0);
        assertThat(value).isEqualTo(42);
    }

    @Test
    @DisplayName("Should return default value when int property key does not exist")
    void testGetIntProperty_NonExistingKey() {
        int value = AppConfig.getIntProperty("non.existing.int.key", 99);
        assertThat(value).isEqualTo(99);
    }

    @Test
    @DisplayName("Should throw NumberFormatException when int property has invalid format")
    void testGetIntProperty_InvalidNumberFormat() throws Exception {
        Field propertiesField = AppConfig.class.getDeclaredField("properties");
        propertiesField.setAccessible(true);
        Properties props = (Properties) propertiesField.get(null);
        props.setProperty("foqs.invalid.number", "not-a-number");

        try {
            assertThatThrownBy(() -> AppConfig.getIntProperty("foqs.invalid.number", 10))
                    .isInstanceOf(NumberFormatException.class);
        } finally {
            props.remove("foqs.invalid.number");
        }
    }

    @Test
    @DisplayName("Should successfully load shard configurations")
    void testGetShardConfigs_Success() {
        List<ShardConfig> shardConfigs = AppConfig.getShardConfigs();

        assertThat(shardConfigs).isNotNull();
        assertThat(shardConfigs).hasSize(2);

        ShardConfig shard0 = shardConfigs.get(0);
        assertThat(shard0.getShardId()).isEqualTo(0);
        assertThat(shard0.getJdbcUrl()).isEqualTo("jdbc:h2:mem:shard0;DB_CLOSE_DELAY=-1");
        assertThat(shard0.getUsername()).isEqualTo("sa");
        assertThat(shard0.getPassword()).isEqualTo("");

        ShardConfig shard1 = shardConfigs.get(1);
        assertThat(shard1.getShardId()).isEqualTo(1);
        assertThat(shard1.getJdbcUrl()).isEqualTo("jdbc:h2:mem:shard1;DB_CLOSE_DELAY=-1");
        assertThat(shard1.getUsername()).isEqualTo("sa");
        assertThat(shard1.getPassword()).isEqualTo("");
    }

    @Test
    @DisplayName("Should throw IllegalStateException when shard URL is missing")
    void testGetShardConfigs_MissingShardUrl() throws Exception {
        Field propertiesField = AppConfig.class.getDeclaredField("properties");
        propertiesField.setAccessible(true);
        Properties props = (Properties) propertiesField.get(null);

        // Temporarily increase shard count without providing URL for shard 2
        props.setProperty("foqs.shards.count", "3");

        try {
            assertThatThrownBy(AppConfig::getShardConfigs)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Missing JDBC URL config for shard index: 2");
        } finally {
            props.setProperty("foqs.shards.count", "2");
        }
    }
}
