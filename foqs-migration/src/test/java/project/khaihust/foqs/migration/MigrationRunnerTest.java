package project.khaihust.foqs.migration;

import config.ShardConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MigrationRunnerTest {

    @Test
    @DisplayName("Should successfully apply Liquibase changelog to database")
    void testMigration_Success() throws Exception {
        String dbUrl = "jdbc:h2:mem:migration_success_db;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";
        ShardConfig shardConfig = ShardConfig.builder()
                .shardId(0)
                .jdbcUrl(dbUrl)
                .username("sa")
                .password("")
                .build();

        MigrationRunner.migration(shardConfig);

        // Verify table was created and accessible
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM queue_messages")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("Should execute main method across all configured shards")
    void testMainMethod_Success() {
        MigrationRunner.main(new String[]{});

        // Verify that tables were created on configured shard
        String dbUrl = "jdbc:h2:mem:foqs_shard_0;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM queue_messages")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("Should throw RuntimeException when connection to shard fails")
    void testMigration_ConnectionFailure() {
        ShardConfig invalidShard = ShardConfig.builder()
                .shardId(9)
                .jdbcUrl("jdbc:invalid_protocol://localhost:99999/none")
                .username("invalid")
                .password("invalid")
                .build();

        assertThatThrownBy(() -> MigrationRunner.migration(invalidShard))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Error connecting to shard: 9");
    }
}
