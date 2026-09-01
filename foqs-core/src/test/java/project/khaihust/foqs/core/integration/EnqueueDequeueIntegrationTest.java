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
import project.khaihust.foqs.core.proto.DequeueRequestDto;
import project.khaihust.foqs.core.proto.DequeueResponseDto;
import project.khaihust.foqs.core.proto.DequeueServiceGrpc;
import project.khaihust.foqs.core.proto.DequeuedMessageDto;
import project.khaihust.foqs.core.proto.EnqueueRequestDto;
import project.khaihust.foqs.core.proto.EnqueueResponseDto;
import project.khaihust.foqs.core.proto.EnqueueServiceGrpc;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
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
}
