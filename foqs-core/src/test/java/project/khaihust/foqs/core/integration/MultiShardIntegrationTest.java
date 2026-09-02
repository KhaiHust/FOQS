package project.khaihust.foqs.core.integration;

import com.fasterxml.uuid.impl.UUIDUtil;
import com.google.protobuf.ByteString;
import config.ShardConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import project.khaihust.foqs.core.Application;
import project.khaihust.foqs.core.proto.*;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class MultiShardIntegrationTest {

    private static final String SHARD_0_URL = "jdbc:h2:mem:multi_shard_0;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";
    private static final String SHARD_1_URL = "jdbc:h2:mem:multi_shard_1;MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";

    private Application app;
    private ManagedChannel channel;
    private EnqueueServiceGrpc.EnqueueServiceBlockingStub enqueueStub;
    private DequeueServiceGrpc.DequeueServiceBlockingStub dequeueStub;

    private String topicShard0;
    private String topicShard1;

    @BeforeEach
    void setUp() throws Exception {
        initDatabase(SHARD_0_URL);
        initDatabase(SHARD_1_URL);

        List<ShardConfig> shardConfigs = List.of(
                ShardConfig.builder()
                        .shardId(0)
                        .jdbcUrl(SHARD_0_URL)
                        .username(DB_USER)
                        .password(DB_PASSWORD)
                        .build(),
                ShardConfig.builder()
                        .shardId(1)
                        .jdbcUrl(SHARD_1_URL)
                        .username(DB_USER)
                        .password(DB_PASSWORD)
                        .build()
        );

        app = new Application(
                0,
                1000,
                10,
                10,
                100,
                Duration.ofSeconds(30),
                100,
                1,
                shardConfigs
        );
        app.start();

        channel = ManagedChannelBuilder.forAddress("localhost", app.getPort())
                .usePlaintext()
                .build();

        enqueueStub = EnqueueServiceGrpc.newBlockingStub(channel);
        dequeueStub = DequeueServiceGrpc.newBlockingStub(channel);

        // Discover topics that deterministically route to Shard 0 vs Shard 1
        int idx = 0;
        while (topicShard0 == null || topicShard1 == null) {
            String candidate = "multi-topic-" + idx++;
            int shard = app.getShardRouter().selectShard(candidate);
            if (shard == 0 && topicShard0 == null) {
                topicShard0 = candidate;
            } else if (shard == 1 && topicShard1 == null) {
                topicShard1 = candidate;
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        }

        if (app != null) {
            app.stop();
        }

        cleanDatabase(SHARD_0_URL);
        cleanDatabase(SHARD_1_URL);
    }

    private void initDatabase(String url) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
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
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_fetch_priority ON queue_messages (topic, status, deliver_after, priority, id)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_reclaim ON queue_messages (status, lease_until)");
        }
    }

    private void cleanDatabase(String url) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM queue_messages");
        }
    }

    private int countRows(String url, String topic) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM queue_messages WHERE topic = ?")) {
            ps.setString(1, topic);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                return rs.getInt(1);
            }
        }
    }

    private int getMessageStatus(String url, UUID messageId) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
             PreparedStatement ps = conn.prepareStatement("SELECT status FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(messageId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return -1; // Not found in this shard
            }
        }
    }

    @Test
    @DisplayName("Should physically isolate enqueued messages into their respective database shards")
    void testPhysicalShardIsolationOnEnqueue() throws Exception {
        int messageCount = 5;

        // Enqueue to Topic for Shard 0
        for (int i = 0; i < messageCount; i++) {
            enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                    .setTopic(topicShard0)
                    .setPriority(1)
                    .setPayload(ByteString.copyFromUtf8("shard-0-data-" + i))
                    .setDeliverAfter(System.currentTimeMillis())
                    .build());
        }

        // Enqueue to Topic for Shard 1
        for (int i = 0; i < messageCount; i++) {
            enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                    .setTopic(topicShard1)
                    .setPriority(2)
                    .setPayload(ByteString.copyFromUtf8("shard-1-data-" + i))
                    .setDeliverAfter(System.currentTimeMillis())
                    .build());
        }

        // Wait for buffer flush
        Thread.sleep(150);

        // Verify Shard 0 DB contains topicShard0 messages and NONE of topicShard1
        assertThat(countRows(SHARD_0_URL, topicShard0)).isEqualTo(messageCount);
        assertThat(countRows(SHARD_0_URL, topicShard1)).isEqualTo(0);

        // Verify Shard 1 DB contains topicShard1 messages and NONE of topicShard0
        assertThat(countRows(SHARD_1_URL, topicShard1)).isEqualTo(messageCount);
        assertThat(countRows(SHARD_1_URL, topicShard0)).isEqualTo(0);
    }

    @Test
    @DisplayName("Should dequeue messages from each shard through PrefetchBufferRegistry routing")
    void testMultiShardDequeueFlow() throws Exception {
        enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topicShard0)
                .setPriority(5)
                .setPayload(ByteString.copyFromUtf8("data-for-shard-0"))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topicShard1)
                .setPriority(10)
                .setPayload(ByteString.copyFromUtf8("data-for-shard-1"))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        // Wait for write buffer flusher
        Thread.sleep(150);

        // Dequeue from Shard 0
        DequeueResponseDto resp0 = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topicShard0)
                .setCount(1)
                .setTimeout(3000)
                .build());

        assertThat(resp0.getMessagesCount()).isEqualTo(1);
        DequeuedMessageDto msg0 = resp0.getMessages(0);
        assertThat(msg0.getTopic()).isEqualTo(topicShard0);
        assertThat(msg0.getPriority()).isEqualTo(5);
        assertThat(msg0.getPayload().toStringUtf8()).isEqualTo("data-for-shard-0");

        // Dequeue from Shard 1
        DequeueResponseDto resp1 = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topicShard1)
                .setCount(1)
                .setTimeout(3000)
                .build());

        assertThat(resp1.getMessagesCount()).isEqualTo(1);
        DequeuedMessageDto msg1 = resp1.getMessages(0);
        assertThat(msg1.getTopic()).isEqualTo(topicShard1);
        assertThat(msg1.getPriority()).isEqualTo(10);
        assertThat(msg1.getPayload().toStringUtf8()).isEqualTo("data-for-shard-1");
    }

    @Test
    @DisplayName("Should scatter-gather BatchAck across multiple shards in a single request")
    void testCrossShardBatchAck() throws Exception {
        EnqueueResponseDto enq0 = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topicShard0)
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("ack-test-0"))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        EnqueueResponseDto enq1 = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topicShard1)
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("ack-test-1"))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        // Dequeue both so they are in LEASED (1) state
        dequeueStub.dequeue(DequeueRequestDto.newBuilder().setTopic(topicShard0).setCount(1).setTimeout(3000).build());
        dequeueStub.dequeue(DequeueRequestDto.newBuilder().setTopic(topicShard1).setCount(1).setTimeout(3000).build());

        UUID id0 = UUID.fromString(enq0.getMessageId());
        UUID id1 = UUID.fromString(enq1.getMessageId());
        UUID nonExistentId = UUID.randomUUID();

        // Verify currently leased (status = 1) in their respective shards
        assertThat(getMessageStatus(SHARD_0_URL, id0)).isEqualTo(1);
        assertThat(getMessageStatus(SHARD_1_URL, id1)).isEqualTo(1);

        // Send a single batchAck containing IDs from Shard 0, Shard 1, and a nonexistent ID
        BatchAckResponseDto ackResp = dequeueStub.batchAck(BatchAckRequestDto.newBuilder()
                .addMessageIds(id0.toString())
                .addMessageIds(id1.toString())
                .addMessageIds(nonExistentId.toString())
                .build());

        assertThat(ackResp.getAckedMessageIdsList()).containsExactlyInAnyOrder(id0.toString(), id1.toString());
        assertThat(ackResp.getFailedMessageIdsList()).containsExactly(nonExistentId.toString());

        // Verify status transitioned to COMPLETED (2) in each physical shard
        assertThat(getMessageStatus(SHARD_0_URL, id0)).isEqualTo(2);
        assertThat(getMessageStatus(SHARD_1_URL, id1)).isEqualTo(2);
    }

    @Test
    @DisplayName("Should scatter-gather BatchNack across multiple shards and aggregate success counts")
    void testCrossShardBatchNack() throws Exception {
        EnqueueResponseDto enq0 = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topicShard0)
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("nack-test-0"))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        EnqueueResponseDto enq1 = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topicShard1)
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("nack-test-1"))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        // Dequeue both so status = 1 (LEASED)
        dequeueStub.dequeue(DequeueRequestDto.newBuilder().setTopic(topicShard0).setCount(1).setTimeout(3000).build());
        dequeueStub.dequeue(DequeueRequestDto.newBuilder().setTopic(topicShard1).setCount(1).setTimeout(3000).build());

        UUID id0 = UUID.fromString(enq0.getMessageId());
        UUID id1 = UUID.fromString(enq1.getMessageId());

        // Batch NACK both across shards
        BatchNackResponseDto nackResp = dequeueStub.batchNack(BatchNackRequestDto.newBuilder()
                .addMessageIds(id0.toString())
                .addMessageIds(id1.toString())
                .setRetryDelayMs(500L)
                .setMaxRetryCount(3)
                .build());

        assertThat(nackResp.getSuccessCount()).isEqualTo(2);

        // Verify status returned to READY (0) in each physical database
        assertThat(getMessageStatus(SHARD_0_URL, id0)).isEqualTo(0);
        assertThat(getMessageStatus(SHARD_1_URL, id1)).isEqualTo(0);
    }

    @Test
    @DisplayName("Should sweep and reclaim expired leases across all database shards")
    void testMultiShardLeaseReclaimer() throws Exception {
        EnqueueResponseDto enq0 = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topicShard0)
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("reclaim-test-0"))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        EnqueueResponseDto enq1 = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topicShard1)
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("reclaim-test-1"))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        // Dequeue so status = 1
        dequeueStub.dequeue(DequeueRequestDto.newBuilder().setTopic(topicShard0).setCount(1).setTimeout(3000).build());
        dequeueStub.dequeue(DequeueRequestDto.newBuilder().setTopic(topicShard1).setCount(1).setTimeout(3000).build());

        UUID id0 = UUID.fromString(enq0.getMessageId());
        UUID id1 = UUID.fromString(enq1.getMessageId());

        // Manually simulate lease expiration in both shard databases
        expireLease(SHARD_0_URL, id0);
        expireLease(SHARD_1_URL, id1);

        // Trigger multi-shard lease reclaimer sweep
        app.getLeaseReclaimer().reclaimLease();

        // Verify both messages recovered to READY (0)
        assertThat(getMessageStatus(SHARD_0_URL, id0)).isEqualTo(0);
        assertThat(getMessageStatus(SHARD_1_URL, id1)).isEqualTo(0);
    }

    @Test
    @DisplayName("Should concurrently enqueue, route, dequeue, and ack across multiple topics and shards")
    void testConcurrentMultiTopicMultiShardWorkload() throws Exception {
        int threads = 6;
        int messagesPerThread = 20;
        int totalExpected = threads * messagesPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        List<String> allEnqueuedIds = java.util.Collections.synchronizedList(new ArrayList<>());

        // Produce messages across both shards concurrently
        for (int t = 0; t < threads; t++) {
            final String topic = (t % 2 == 0) ? topicShard0 : topicShard1;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < messagesPerThread; i++) {
                        EnqueueResponseDto resp = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                                .setTopic(topic)
                                .setPriority(1)
                                .setPayload(ByteString.copyFromUtf8("concurrent-load-" + i))
                                .setDeliverAfter(System.currentTimeMillis())
                                .build());
                        allEnqueuedIds.add(resp.getMessageId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(allEnqueuedIds).hasSize(totalExpected);

        // Dequeue all messages from both topics
        List<String> dequeuedIds = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 8000;

        while (dequeuedIds.size() < totalExpected && System.currentTimeMillis() < deadline) {
            DequeueResponseDto r0 = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                    .setTopic(topicShard0)
                    .setCount(20)
                    .setTimeout(500)
                    .build());
            r0.getMessagesList().forEach(m -> dequeuedIds.add(m.getMessageId()));

            DequeueResponseDto r1 = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                    .setTopic(topicShard1)
                    .setCount(20)
                    .setTimeout(500)
                    .build());
            r1.getMessagesList().forEach(m -> dequeuedIds.add(m.getMessageId()));
        }

        assertThat(dequeuedIds).hasSize(totalExpected);

        // Batch ACK all dequeued messages in a single multi-shard gRPC call
        BatchAckResponseDto ackResp = dequeueStub.batchAck(BatchAckRequestDto.newBuilder()
                .addAllMessageIds(dequeuedIds)
                .build());

        assertThat(ackResp.getAckedMessageIdsCount()).isEqualTo(totalExpected);
        assertThat(ackResp.getFailedMessageIdsCount()).isEqualTo(0);

        executor.shutdown();
    }

    private void expireLease(String url, UUID messageId) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE queue_messages SET lease_until = ? WHERE id = ?")) {
            ps.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis() - 5000));
            ps.setBytes(2, UUIDUtil.asByteArray(messageId));
            ps.executeUpdate();
        }
    }
}
