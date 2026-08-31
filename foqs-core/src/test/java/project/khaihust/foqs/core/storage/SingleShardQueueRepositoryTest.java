package project.khaihust.foqs.core.storage;

import com.fasterxml.uuid.impl.UUIDUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import project.khaihust.foqs.core.models.EnqueueRequest;
import project.khaihust.foqs.core.models.EnqueueTask;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SingleShardQueueRepositoryTest {

    private HikariDataSource dataSource;
    private SingleShardQueueRepository repository;

    @BeforeEach
    void setUp() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:test_shard;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS queue_messages");
            stmt.execute("""
                CREATE TABLE queue_messages (
                    id BINARY(16) NOT NULL,
                    topic VARCHAR(64) NOT NULL,
                    priority INT NOT NULL DEFAULT 0,
                    payload BLOB NOT NULL,
                    status TINYINT NOT NULL DEFAULT 0,
                    deliver_after TIMESTAMP(3) NOT NULL,
                    lease_until TIMESTAMP(3) NULL,
                    retry_count INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
                    PRIMARY KEY (id)
                )
            """);
        }

        repository = new SingleShardQueueRepository(dataSource);
    }

    @AfterEach
    void tearDown() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    private EnqueueTask createTask(String topic, int priority, byte[] payload, long deliverAfter) {
        EnqueueRequest request = EnqueueRequest.builder()
                .topic(topic)
                .priority(priority)
                .payload(payload)
                .deliverAfter(deliverAfter)
                .build();
        return new EnqueueTask(request);
    }

    @Test
    @DisplayName("Should successfully enqueue single task and persist all fields in database")
    void testEnqueueBatch_Success() throws Exception {
        long deliverAfter = System.currentTimeMillis();
        byte[] payload = "test-payload-bytes".getBytes(StandardCharsets.UTF_8);
        EnqueueTask task = createTask("orders", 10, payload, deliverAfter);

        repository.enqueueBatch(List.of(task));

        // Assert future completion
        assertThat(task.getFuture()).isCompletedWithValue(task.getMessageId());

        // Assert database row contents
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT * FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(task.getMessageId()));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getBytes("id")).isEqualTo(UUIDUtil.asByteArray(task.getMessageId()));
                assertThat(UUIDUtil.uuid(rs.getBytes("id"))).isEqualTo(task.getMessageId());
                assertThat(rs.getString("topic")).isEqualTo("orders");
                assertThat(rs.getInt("priority")).isEqualTo(10);
                assertThat(rs.getBytes("payload")).isEqualTo(payload);
                assertThat(rs.getInt("status")).isEqualTo(0);
                assertThat(rs.getInt("retry_count")).isEqualTo(0);
                assertThat(rs.getTimestamp("deliver_after").getTime()).isEqualTo(deliverAfter);
                assertThat(rs.getTimestamp("lease_until")).isNull();
                assertThat(rs.getTimestamp("created_at")).isNotNull();
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("Should successfully enqueue multiple tasks in a batch and persist all rows")
    void testEnqueueBatch_MultipleTasks() throws Exception {
        int taskCount = 5;
        List<EnqueueTask> tasks = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (int i = 0; i < taskCount; i++) {
            byte[] payload = ("payload-" + i).getBytes(StandardCharsets.UTF_8);
            tasks.add(createTask("topic-" + i, i * 2, payload, now + (i * 1000L)));
        }

        repository.enqueueBatch(tasks);

        // Verify all futures completed with correct message IDs
        for (EnqueueTask task : tasks) {
            assertThat(task.getFuture()).isCompletedWithValue(task.getMessageId());
        }

        // Verify database total row count
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM queue_messages")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(taskCount);
        }

        // Verify each individual record in DB
        for (EnqueueTask task : tasks) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement("SELECT * FROM queue_messages WHERE id = ?")) {
                ps.setBytes(1, UUIDUtil.asByteArray(task.getMessageId()));
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getString("topic")).isEqualTo(task.getEnqueueRequest().getTopic());
                    assertThat(rs.getInt("priority")).isEqualTo(task.getEnqueueRequest().getPriority());
                    assertThat(rs.getBytes("payload")).isEqualTo(task.getEnqueueRequest().getPayload());
                    assertThat(rs.getInt("status")).isEqualTo(0);
                    assertThat(rs.getInt("retry_count")).isEqualTo(0);
                    assertThat(rs.getTimestamp("deliver_after").getTime()).isEqualTo(task.getEnqueueRequest().getDeliverAfter());
                }
            }
        }
    }

    @Test
    @DisplayName("Should handle empty task list gracefully without executing insertions")
    void testEnqueueBatch_EmptyList() throws Exception {
        assertThatCode(() -> repository.enqueueBatch(Collections.emptyList()))
                .doesNotThrowAnyException();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM queue_messages")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("Should throw SQLException when database operation fails")
    void testEnqueueBatch_SqlException() throws Exception {
        // Drop table to trigger SQLException on insert
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE queue_messages");
        }

        EnqueueTask task = createTask("failing-topic", 1, "data".getBytes(StandardCharsets.UTF_8), System.currentTimeMillis());

        assertThatThrownBy(() -> repository.enqueueBatch(List.of(task)))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Failed to enqueue batch messages");

        assertThat(task.getFuture().isDone()).isFalse();
    }
}
