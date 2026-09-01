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
import java.util.UUID;

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

    @Test
    @DisplayName("Should lease ready messages in ascending priority order and update status to LEASED")
    void testLeaseMessages_Success() throws Exception {
        long now = System.currentTimeMillis();
        EnqueueTask taskP10 = createTask("orders", 10, "p10".getBytes(StandardCharsets.UTF_8), now - 1000);
        EnqueueTask taskP1 = createTask("orders", 1, "p1".getBytes(StandardCharsets.UTF_8), now - 1000);
        EnqueueTask taskP5 = createTask("orders", 5, "p5".getBytes(StandardCharsets.UTF_8), now - 1000);

        repository.enqueueBatch(List.of(taskP10, taskP1, taskP5));

        var leased = repository.leaseMessages("orders", 2, java.time.Duration.ofSeconds(30));

        assertThat(leased).hasSize(2);
        assertThat(leased.get(0).getPriority()).isEqualTo(1);
        assertThat(leased.get(0).getId()).isEqualTo(taskP1.getMessageId());
        assertThat(leased.get(1).getPriority()).isEqualTo(5);
        assertThat(leased.get(1).getId()).isEqualTo(taskP5.getMessageId());

        // Verify status in DB: 2 leased (status=1), 1 remaining ready (status=0)
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT status, COUNT(*) FROM queue_messages GROUP BY status")) {
                int status0Count = 0;
                int status1Count = 0;
                while (rs.next()) {
                    int status = rs.getInt(1);
                    int count = rs.getInt(2);
                    if (status == 0) status0Count = count;
                    if (status == 1) status1Count = count;
                }
                assertThat(status0Count).isEqualTo(1);
                assertThat(status1Count).isEqualTo(2);
            }
        }
    }

    @Test
    @DisplayName("Should not lease messages whose deliver_after is in the future")
    void testLeaseMessages_SkipFutureDeliverAfter() throws Exception {
        long futureTime = System.currentTimeMillis() + 60000;
        EnqueueTask futureTask = createTask("delayed", 1, "future".getBytes(StandardCharsets.UTF_8), futureTime);

        repository.enqueueBatch(List.of(futureTask));

        var leased = repository.leaseMessages("delayed", 5, java.time.Duration.ofSeconds(30));
        assertThat(leased).isEmpty();
    }

    @Test
    @DisplayName("Should return empty list when no messages are available to lease")
    void testLeaseMessages_NoAvailableMessages() throws SQLException {
        var leased = repository.leaseMessages("empty-topic", 10, java.time.Duration.ofSeconds(30));
        assertThat(leased).isEmpty();
    }

    @Test
    @DisplayName("Should reclaim expired leases, resetting status to READY and incrementing retry count")
    void testReclaimExpiredLeases() throws Exception {
        long now = System.currentTimeMillis();
        EnqueueTask task = createTask("reclaim-topic", 1, "reclaim-data".getBytes(StandardCharsets.UTF_8), now - 5000);
        repository.enqueueBatch(List.of(task));

        // Lease with 1 second duration
        var leased = repository.leaseMessages("reclaim-topic", 1, java.time.Duration.ofSeconds(1));
        assertThat(leased).hasSize(1);

        // Manually update lease_until to past timestamp to simulate expiration
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE queue_messages SET lease_until = ? WHERE id = ?")) {
            ps.setTimestamp(1, new java.sql.Timestamp(now - 1000));
            ps.setBytes(2, UUIDUtil.asByteArray(task.getMessageId()));
            ps.executeUpdate();
        }

        int reclaimedCount = repository.reclaimExpiredLeases();
        assertThat(reclaimedCount).isEqualTo(1);

        // Verify message is now READY (status=0), lease_until is NULL, retry_count = 1
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT status, lease_until, retry_count FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(task.getMessageId()));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("status")).isEqualTo(0);
                assertThat(rs.getTimestamp("lease_until")).isNull();
                assertThat(rs.getInt("retry_count")).isEqualTo(1);
            }
        }
    }

    @Test
    @DisplayName("Should successfully ack leased messages and update status to COMPLETED (2)")
    void testAckMessages_Success() throws Exception {
        long now = System.currentTimeMillis();
        EnqueueTask task1 = createTask("ack-topic", 1, "data-1".getBytes(StandardCharsets.UTF_8), now - 1000);
        EnqueueTask task2 = createTask("ack-topic", 2, "data-2".getBytes(StandardCharsets.UTF_8), now - 1000);
        repository.enqueueBatch(List.of(task1, task2));

        var leased = repository.leaseMessages("ack-topic", 2, java.time.Duration.ofSeconds(30));
        assertThat(leased).hasSize(2);

        List<UUID> ackedIds = repository.ackMessages(List.of(task1.getMessageId(), task2.getMessageId()));
        assertThat(ackedIds).containsExactlyInAnyOrder(task1.getMessageId(), task2.getMessageId());

        // Verify DB records: status = 2 (COMPLETED) and lease_until is NULL
        for (EnqueueTask task : List.of(task1, task2)) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement("SELECT status, lease_until FROM queue_messages WHERE id = ?")) {
                ps.setBytes(1, UUIDUtil.asByteArray(task.getMessageId()));
                try (ResultSet rs = ps.executeQuery()) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getInt("status")).isEqualTo(2);
                    assertThat(rs.getTimestamp("lease_until")).isNull();
                }
            }
        }
    }

    @Test
    @DisplayName("Should not ack messages that are not in LEASED status or already acked")
    void testAckMessages_NonLeasedAndAlreadyAcked() throws Exception {
        long now = System.currentTimeMillis();
        EnqueueTask leasedTask = createTask("ack-test", 2, "leased".getBytes(StandardCharsets.UTF_8), now - 1000);
        repository.enqueueBatch(List.of(leasedTask));

        // Lease leasedTask
        var leased = repository.leaseMessages("ack-test", 1, java.time.Duration.ofSeconds(30));
        assertThat(leased).hasSize(1);

        // Acknowledge leasedTask so it transitions to COMPLETED (status=2)
        var firstAck = repository.ackMessages(List.of(leasedTask.getMessageId()));
        assertThat(firstAck).containsExactly(leasedTask.getMessageId());

        // Enqueue readyTask (status=0, not leased)
        EnqueueTask readyTask = createTask("ack-test", 1, "ready".getBytes(StandardCharsets.UTF_8), now - 1000);
        repository.enqueueBatch(List.of(readyTask));

        // Attempt to ack readyTask (status=0) and leasedTask (already status=2)
        List<UUID> secondAck = repository.ackMessages(List.of(readyTask.getMessageId(), leasedTask.getMessageId()));
        assertThat(secondAck).isEmpty();

        // Verify readyTask remains READY (status=0)
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT status FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(readyTask.getMessageId()));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("status")).isEqualTo(0);
            }
        }

        // Verify leasedTask remains COMPLETED (status=2)
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT status FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(leasedTask.getMessageId()));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("status")).isEqualTo(2);
            }
        }
    }

    @Test
    @DisplayName("Should return empty list when acking non-existent message IDs")
    void testAckMessages_NonExistent() throws Exception {
        List<UUID> acked = repository.ackMessages(List.of(UUID.randomUUID(), UUID.randomUUID()));
        assertThat(acked).isEmpty();
    }

    @Test
    @DisplayName("Should return empty list when messageIds is empty or null for ackMessages")
    void testAckMessages_EmptyOrNullList() throws Exception {
        assertThat(repository.ackMessages(Collections.emptyList())).isEmpty();
        assertThat(repository.ackMessages(null)).isEmpty();
    }

    @Test
    @DisplayName("Should nack leased message with retry below maxRetries, resetting status to READY, incrementing retry count and updating deliver_after")
    void testNackMessages_RetryBelowMaxRetries() throws Exception {
        long now = System.currentTimeMillis();
        EnqueueTask task = createTask("nack-topic", 1, "nack-data".getBytes(StandardCharsets.UTF_8), now - 1000);
        repository.enqueueBatch(List.of(task));

        var leased = repository.leaseMessages("nack-topic", 1, java.time.Duration.ofSeconds(30));
        assertThat(leased).hasSize(1);

        long retryDelayMs = 5000L;
        int maxRetries = 3;
        int updated = repository.nackMessages(List.of(task.getMessageId()), retryDelayMs, maxRetries);
        assertThat(updated).isEqualTo(1);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT status, lease_until, deliver_after, retry_count FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(task.getMessageId()));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("status")).isEqualTo(0); // READY
                assertThat(rs.getTimestamp("lease_until")).isNull();
                assertThat(rs.getInt("retry_count")).isEqualTo(1);
                assertThat(rs.getTimestamp("deliver_after").getTime()).isGreaterThanOrEqualTo(now + retryDelayMs - 1000);
            }
        }
    }

    @Test
    @DisplayName("Should transition message to DEAD_LETTER (3) when retry_count + 1 >= maxRetries")
    void testNackMessages_DeadLetterWhenMaxRetriesExceeded() throws Exception {
        long now = System.currentTimeMillis();
        EnqueueTask task1 = createTask("dlq-topic", 1, "dlq-data-1".getBytes(StandardCharsets.UTF_8), now - 1000);
        EnqueueTask task2 = createTask("dlq-topic", 1, "dlq-data-2".getBytes(StandardCharsets.UTF_8), now - 1000);
        repository.enqueueBatch(List.of(task1, task2));

        // Lease both messages (initial retry_count = 0)
        var leased = repository.leaseMessages("dlq-topic", 2, java.time.Duration.ofSeconds(30));
        assertThat(leased).hasSize(2);

        // Manually set task2 retry_count to 2
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("UPDATE queue_messages SET retry_count = 2 WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(task2.getMessageId()));
            ps.executeUpdate();
        }

        // Nack task1 with maxRetries = 1 -> retry_count (0) + 1 >= 1 -> DEAD_LETTER (3)
        int updated1 = repository.nackMessages(List.of(task1.getMessageId()), 0L, 1);
        assertThat(updated1).isEqualTo(1);

        // Nack task2 with maxRetries = 3 -> retry_count (2) + 1 >= 3 -> DEAD_LETTER (3)
        int updated2 = repository.nackMessages(List.of(task2.getMessageId()), 0L, 3);
        assertThat(updated2).isEqualTo(1);

        // Verify task1 status in DB
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT status, lease_until, retry_count FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(task1.getMessageId()));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("status")).isEqualTo(3); // DEAD_LETTER
                assertThat(rs.getTimestamp("lease_until")).isNull();
                assertThat(rs.getInt("retry_count")).isEqualTo(1);
            }
        }

        // Verify task2 status in DB
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT status, lease_until, retry_count FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(task2.getMessageId()));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("status")).isEqualTo(3); // DEAD_LETTER
                assertThat(rs.getTimestamp("lease_until")).isNull();
                assertThat(rs.getInt("retry_count")).isEqualTo(3);
            }
        }
    }

    @Test
    @DisplayName("Should return 0 and not modify rows when nacking non-leased or non-existent messages")
    void testNackMessages_NonLeasedAndNonExistent() throws Exception {
        long now = System.currentTimeMillis();
        EnqueueTask readyTask = createTask("nack-test", 1, "ready".getBytes(StandardCharsets.UTF_8), now - 1000);
        repository.enqueueBatch(List.of(readyTask));

        UUID nonExistentId = UUID.randomUUID();
        int updated = repository.nackMessages(List.of(readyTask.getMessageId(), nonExistentId), 1000L, 5);
        assertThat(updated).isEqualTo(0);

        // Verify readyTask status remains 0 and retry_count remains 0
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT status, retry_count FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(readyTask.getMessageId()));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("status")).isEqualTo(0);
                assertThat(rs.getInt("retry_count")).isEqualTo(0);
            }
        }
    }

    @Test
    @DisplayName("Should return 0 when messageIds is empty or null for nackMessages")
    void testNackMessages_EmptyOrNullList() throws Exception {
        assertThat(repository.nackMessages(Collections.emptyList(), 1000L, 5)).isEqualTo(0);
        assertThat(repository.nackMessages(null, 1000L, 5)).isEqualTo(0);
    }
}
