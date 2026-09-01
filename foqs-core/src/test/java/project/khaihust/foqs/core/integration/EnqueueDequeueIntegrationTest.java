package project.khaihust.foqs.core.integration;

import com.google.protobuf.ByteString;
import config.ShardConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import project.khaihust.foqs.core.Application;
import com.fasterxml.uuid.impl.UUIDUtil;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class EnqueueDequeueIntegrationTest {

    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/foqs_shard_0?useSSL=false&allowPublicKeyRetrieval=true";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "root";

    private Application app;
    private ManagedChannel channel;
    private EnqueueServiceGrpc.EnqueueServiceBlockingStub enqueueStub;
    private DequeueServiceGrpc.DequeueServiceBlockingStub dequeueStub;

    @BeforeEach
    void setUp() throws Exception {
        cleanDatabase();

        ShardConfig shardConfig = ShardConfig.builder()
                .shardId(0)
                .jdbcUrl(JDBC_URL)
                .username(DB_USER)
                .password(DB_PASSWORD)
                .build();

        app = new Application(
                0,
                1000,
                10,
                10,
                100,
                Duration.ofSeconds(30),
                100,
                1,
                List.of(shardConfig)
        );
        app.start();

        channel = ManagedChannelBuilder.forAddress("localhost", app.getPort())
                .usePlaintext()
                .build();

        enqueueStub = EnqueueServiceGrpc.newBlockingStub(channel);
        dequeueStub = DequeueServiceGrpc.newBlockingStub(channel);
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

        cleanDatabase();
    }

    private void cleanDatabase() throws SQLException {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE queue_messages");
        }
    }

    @Test
    @DisplayName("Should enqueue single message, return valid UUID, and dequeue with all attributes matching")
    void testSingleMessageEnqueueAndDequeue() throws Exception {
        String topic = "single-message-topic";
        int priority = 3;
        byte[] payloadBytes = "test-payload-data".getBytes(StandardCharsets.UTF_8);
        long deliverAfter = System.currentTimeMillis();

        EnqueueRequestDto enqueueRequest = EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(priority)
                .setPayload(ByteString.copyFrom(payloadBytes))
                .setDeliverAfter(deliverAfter)
                .build();

        EnqueueResponseDto enqueueResponse = enqueueStub.enqueue(enqueueRequest);
        assertThat(enqueueResponse).isNotNull();
        assertThat(enqueueResponse.getMessageId()).isNotBlank();

        UUID messageId = UUID.fromString(enqueueResponse.getMessageId());
        assertThat(messageId).isNotNull();

        DequeueRequestDto dequeueRequest = DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(3000)
                .build();

        DequeueResponseDto dequeueResponse = dequeueStub.dequeue(dequeueRequest);
        assertThat(dequeueResponse.getMessagesCount()).isEqualTo(1);

        DequeuedMessageDto dequeuedMessage = dequeueResponse.getMessages(0);
        assertThat(dequeuedMessage.getMessageId()).isEqualTo(enqueueResponse.getMessageId());
        assertThat(dequeuedMessage.getTopic()).isEqualTo(topic);
        assertThat(dequeuedMessage.getPriority()).isEqualTo(priority);
        assertThat(dequeuedMessage.getPayload().toByteArray()).isEqualTo(payloadBytes);
        assertThat(dequeuedMessage.getLeaseUntilEpochMs()).isGreaterThan(System.currentTimeMillis());
        assertThat(dequeuedMessage.getRetryCount()).isEqualTo(0);
        assertThat(dequeuedMessage.getCreatedAtEpochMs()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Should dequeue messages strictly in priority order (1, 5, 10)")
    void testPriorityOrdering() {
        String topic = "priority-ordering-topic";
        long now = System.currentTimeMillis();

        EnqueueResponseDto respP10 = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(10)
                .setPayload(ByteString.copyFromUtf8("priority-10-data"))
                .setDeliverAfter(now)
                .build());

        EnqueueResponseDto respP1 = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("priority-1-data"))
                .setDeliverAfter(now)
                .build());

        EnqueueResponseDto respP5 = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(5)
                .setPayload(ByteString.copyFromUtf8("priority-5-data"))
                .setDeliverAfter(now)
                .build());

        DequeueRequestDto dequeueRequest = DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(3)
                .setTimeout(3000)
                .build();

        DequeueResponseDto dequeueResponse = dequeueStub.dequeue(dequeueRequest);
        assertThat(dequeueResponse.getMessagesCount()).isEqualTo(3);

        DequeuedMessageDto first = dequeueResponse.getMessages(0);
        assertThat(first.getMessageId()).isEqualTo(respP1.getMessageId());
        assertThat(first.getPriority()).isEqualTo(1);
        assertThat(first.getPayload().toStringUtf8()).isEqualTo("priority-1-data");

        DequeuedMessageDto second = dequeueResponse.getMessages(1);
        assertThat(second.getMessageId()).isEqualTo(respP5.getMessageId());
        assertThat(second.getPriority()).isEqualTo(5);
        assertThat(second.getPayload().toStringUtf8()).isEqualTo("priority-5-data");

        DequeuedMessageDto third = dequeueResponse.getMessages(2);
        assertThat(third.getMessageId()).isEqualTo(respP10.getMessageId());
        assertThat(third.getPriority()).isEqualTo(10);
        assertThat(third.getPayload().toStringUtf8()).isEqualTo("priority-10-data");
    }

    @Test
    @DisplayName("Should return empty list when message deliver_after is in the future")
    void testDelayedDelivery() {
        String topic = "delayed-delivery-topic";
        long futureDeliverAfter = System.currentTimeMillis() + 10_000L;

        EnqueueResponseDto enqueueResponse = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("future-delayed-data"))
                .setDeliverAfter(futureDeliverAfter)
                .build());

        assertThat(enqueueResponse.getMessageId()).isNotBlank();

        DequeueResponseDto dequeueResponse = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(300)
                .build());

        assertThat(dequeueResponse.getMessagesList()).isEmpty();
        assertThat(dequeueResponse.getMessagesCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should reclaim expired lease and re-lease message with incremented retry count")
    void testLeaseExpirationAndReclaim() throws Exception {
        String topic = "reclaim-lease-topic";
        long now = System.currentTimeMillis();

        EnqueueResponseDto enqueueResponse = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("lease-reclaim-data"))
                .setDeliverAfter(now)
                .build());

        String messageId = enqueueResponse.getMessageId();

        // Initial dequeue leases the message
        DequeueResponseDto initialDequeue = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(3000)
                .build());

        assertThat(initialDequeue.getMessagesCount()).isEqualTo(1);
        assertThat(initialDequeue.getMessages(0).getMessageId()).isEqualTo(messageId);
        assertThat(initialDequeue.getMessages(0).getRetryCount()).isEqualTo(0);

        // Manually update lease_until in MySQL to simulate expiration (5 seconds in the past)
        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            int updatedRows = stmt.executeUpdate("UPDATE queue_messages SET lease_until = NOW(3) - INTERVAL 5 SECOND WHERE topic = '" + topic + "'");
            assertThat(updatedRows).isEqualTo(1);
        }

        // Trigger manual lease reclamation
        app.getLeaseReclaimer().reclaimLease();

        // Dequeue again to verify re-leasing with retry_count = 1
        DequeueResponseDto secondDequeue = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(3000)
                .build());

        assertThat(secondDequeue.getMessagesCount()).isEqualTo(1);
        DequeuedMessageDto reLeasedMessage = secondDequeue.getMessages(0);
        assertThat(reLeasedMessage.getMessageId()).isEqualTo(messageId);
        assertThat(reLeasedMessage.getTopic()).isEqualTo(topic);
        assertThat(reLeasedMessage.getRetryCount()).isEqualTo(1);
        assertThat(reLeasedMessage.getPayload().toStringUtf8()).isEqualTo("lease-reclaim-data");
    }

    @Test
    @DisplayName("Should concurrently enqueue 50 messages and drain all unique message IDs in batches")
    void testConcurrentBatchEnqueueAndDrain() throws Exception {
        String topic = "concurrent-batch-topic";
        int totalMessages = 50;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        Set<String> enqueuedIds = ConcurrentHashMap.newKeySet();
        List<CompletableFuture<Void>> futures = new ArrayList<>(totalMessages);

        try {
            for (int i = 0; i < totalMessages; i++) {
                final int index = i;
                futures.add(CompletableFuture.runAsync(() -> {
                    EnqueueResponseDto resp = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                            .setTopic(topic)
                            .setPriority(index % 10)
                            .setPayload(ByteString.copyFromUtf8("concurrent-payload-" + index))
                            .setDeliverAfter(System.currentTimeMillis())
                            .build());
                    enqueuedIds.add(resp.getMessageId());
                }, executor));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertThat(enqueuedIds).hasSize(totalMessages);

        // Drain messages in batches
        Set<String> dequeuedIds = new HashSet<>();
        long deadline = System.currentTimeMillis() + 10_000L;

        while (dequeuedIds.size() < totalMessages && System.currentTimeMillis() < deadline) {
            DequeueResponseDto response = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                    .setTopic(topic)
                    .setCount(10)
                    .setTimeout(2000)
                    .build());

            for (DequeuedMessageDto msg : response.getMessagesList()) {
                dequeuedIds.add(msg.getMessageId());
            }

            if (response.getMessagesCount() == 0 && dequeuedIds.size() < totalMessages) {
                Thread.sleep(50);
            }
        }

        assertThat(dequeuedIds).hasSize(totalMessages);
        assertThat(dequeuedIds).containsExactlyInAnyOrderElementsOf(enqueuedIds);
    }

    @Test
    @DisplayName("Full Flow a: Enqueue -> Dequeue -> BatchAck -> Verify completed (status=2) and subsequent Dequeue empty")
    void testFullFlow_EnqueueDequeueBatchAck() throws Exception {
        String topic = "ack-flow-topic";
        byte[] payloadBytes = "ack-flow-payload".getBytes(StandardCharsets.UTF_8);

        // 1. Enqueue
        EnqueueResponseDto enqueueResponse = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(1)
                .setPayload(ByteString.copyFrom(payloadBytes))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        String messageId = enqueueResponse.getMessageId();
        assertThat(messageId).isNotBlank();

        // 2. Dequeue
        DequeueResponseDto dequeueResponse = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(3000)
                .build());

        assertThat(dequeueResponse.getMessagesCount()).isEqualTo(1);
        assertThat(dequeueResponse.getMessages(0).getMessageId()).isEqualTo(messageId);

        // 3. BatchAck
        BatchAckResponseDto ackResponse = dequeueStub.batchAck(BatchAckRequestDto.newBuilder()
                .addMessageIds(messageId)
                .build());

        assertThat(ackResponse.getAckedMessageIdsList()).containsExactly(messageId);
        assertThat(ackResponse.getFailedMessageIdsList()).isEmpty();

        // 4. Verify in DB: status = 2 (COMPLETED), lease_until is NULL
        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
             PreparedStatement ps = conn.prepareStatement("SELECT status, lease_until FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(UUID.fromString(messageId)));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("status")).isEqualTo(2);
                assertThat(rs.getTimestamp("lease_until")).isNull();
            }
        }

        // 5. Subsequent Dequeue returns empty
        DequeueResponseDto subsequentDequeue = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(300)
                .build());

        assertThat(subsequentDequeue.getMessagesList()).isEmpty();
    }

    @Test
    @DisplayName("Full Flow b: Enqueue -> Dequeue -> BatchNack (retryDelay=0) -> Dequeue again -> Verify retryCount=1 and matching payload/id")
    void testFullFlow_EnqueueDequeueBatchNack_Retry() {
        String topic = "nack-retry-flow-topic";
        byte[] payloadBytes = "nack-retry-payload".getBytes(StandardCharsets.UTF_8);

        // 1. Enqueue
        EnqueueResponseDto enqueueResponse = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(1)
                .setPayload(ByteString.copyFrom(payloadBytes))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        String messageId = enqueueResponse.getMessageId();

        // 2. Dequeue
        DequeueResponseDto firstDequeue = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(3000)
                .build());

        assertThat(firstDequeue.getMessagesCount()).isEqualTo(1);
        assertThat(firstDequeue.getMessages(0).getMessageId()).isEqualTo(messageId);
        assertThat(firstDequeue.getMessages(0).getRetryCount()).isEqualTo(0);

        // 3. BatchNack with retryDelay=0 and maxRetryCount=3
        BatchNackResponseDto nackResponse = dequeueStub.batchNack(BatchNackRequestDto.newBuilder()
                .addMessageIds(messageId)
                .setRetryDelayMs(0L)
                .setMaxRetryCount(3)
                .build());

        assertThat(nackResponse.getSuccessCount()).isEqualTo(1);

        // 4. Dequeue again -> message is immediately re-deliverable
        DequeueResponseDto secondDequeue = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(3000)
                .build());

        assertThat(secondDequeue.getMessagesCount()).isEqualTo(1);
        DequeuedMessageDto reDequeuedMsg = secondDequeue.getMessages(0);
        assertThat(reDequeuedMsg.getMessageId()).isEqualTo(messageId);
        assertThat(reDequeuedMsg.getTopic()).isEqualTo(topic);
        assertThat(reDequeuedMsg.getRetryCount()).isEqualTo(1);
        assertThat(reDequeuedMsg.getPayload().toByteArray()).isEqualTo(payloadBytes);
    }

    @Test
    @DisplayName("Full Flow c: Enqueue -> Dequeue -> BatchNack (exceeding maxRetries) -> Verify status=3 (DEAD_LETTER) in DB and subsequent Dequeue empty")
    void testFullFlow_EnqueueDequeueBatchNack_DeadLetter() throws Exception {
        String topic = "nack-dlq-flow-topic";
        byte[] payloadBytes = "nack-dlq-payload".getBytes(StandardCharsets.UTF_8);

        // 1. Enqueue
        EnqueueResponseDto enqueueResponse = enqueueStub.enqueue(EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(1)
                .setPayload(ByteString.copyFrom(payloadBytes))
                .setDeliverAfter(System.currentTimeMillis())
                .build());

        String messageId = enqueueResponse.getMessageId();

        // 2. Dequeue
        DequeueResponseDto dequeueResponse = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(3000)
                .build());

        assertThat(dequeueResponse.getMessagesCount()).isEqualTo(1);
        assertThat(dequeueResponse.getMessages(0).getMessageId()).isEqualTo(messageId);

        // 3. BatchNack exceeding maxRetries (maxRetryCount = 1, current retry_count 0 -> 0 + 1 >= 1 -> DEAD_LETTER)
        BatchNackResponseDto nackResponse = dequeueStub.batchNack(BatchNackRequestDto.newBuilder()
                .addMessageIds(messageId)
                .setRetryDelayMs(0L)
                .setMaxRetryCount(1)
                .build());

        assertThat(nackResponse.getSuccessCount()).isEqualTo(1);

        // 4. Verify in DB: status = 3 (DEAD_LETTER) and retry_count = 1
        try (Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
             PreparedStatement ps = conn.prepareStatement("SELECT status, retry_count, lease_until FROM queue_messages WHERE id = ?")) {
            ps.setBytes(1, UUIDUtil.asByteArray(UUID.fromString(messageId)));
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("status")).isEqualTo(3); // DEAD_LETTER
                assertThat(rs.getInt("retry_count")).isEqualTo(1);
                assertThat(rs.getTimestamp("lease_until")).isNull();
            }
        }

        // 5. Subsequent Dequeue returns empty
        DequeueResponseDto subsequentDequeue = dequeueStub.dequeue(DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(1)
                .setTimeout(300)
                .build());

        assertThat(subsequentDequeue.getMessagesList()).isEmpty();
    }

    @Test
    @DisplayName("Full Flow d: BatchAck with unknown UUIDs -> Verify response contains them in failedMessageIds")
    void testFullFlow_BatchAck_UnknownUuids() {
        String unknown1 = UUID.randomUUID().toString();
        String unknown2 = UUID.randomUUID().toString();

        BatchAckResponseDto response = dequeueStub.batchAck(BatchAckRequestDto.newBuilder()
                .addMessageIds(unknown1)
                .addMessageIds(unknown2)
                .build());

        assertThat(response.getAckedMessageIdsList()).isEmpty();
        assertThat(response.getFailedMessageIdsList()).containsExactlyInAnyOrder(unknown1, unknown2);
    }
}
