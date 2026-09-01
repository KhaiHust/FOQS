package project.khaihust.foqs.core.buffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import project.khaihust.foqs.core.buffer.impl.PrefetchBatch;
import project.khaihust.foqs.core.enums.MessageStatus;
import project.khaihust.foqs.core.models.Message;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PrefetchBatchTest {

    @Mock
    private ISingleShardQueueRepository queueRepository;

    private PrefetchBatch prefetchBatch;

    @AfterEach
    void tearDown() {
        if (prefetchBatch != null) {
            prefetchBatch.close();
        }
    }

    private Message createMessage(String topic, int priority) {
        Instant now = Instant.now();
        return Message.builder()
                .id(UUID.randomUUID())
                .topic(topic)
                .priority(priority)
                .payload(("payload-" + priority).getBytes(StandardCharsets.UTF_8))
                .status(MessageStatus.LEASED)
                .deliverAfter(now)
                .leaseUntil(now.plusSeconds(30))
                .retryCount(0)
                .createdAt(now)
                .build();
    }

    @Test
    @DisplayName("Should poll messages in priority order from min heap")
    void testPollBatch_PriorityOrder() throws Exception {
        Message msg1 = createMessage("orders", 10);
        Message msg2 = createMessage("orders", 1);
        Message msg3 = createMessage("orders", 5);

        when(queueRepository.leaseMessages(eq("orders"), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg1, msg2, msg3), Collections.emptyList());

        // Refill immediately on construction
        prefetchBatch = new PrefetchBatch("orders", Duration.ofSeconds(30), 10, queueRepository, 1000);

        // Wait a small amount for initial replenishment to populate heap
        Thread.sleep(100);

        List<Message> polled = prefetchBatch.pollBatch(2, Duration.ofMillis(500));
        assertThat(polled).hasSize(2);
        assertThat(polled.get(0).getPriority()).isEqualTo(1);
        assertThat(polled.get(1).getPriority()).isEqualTo(5);

        List<Message> remaining = prefetchBatch.pollBatch(2, Duration.ofMillis(500));
        assertThat(remaining).hasSize(1);
        assertThat(remaining.get(0).getPriority()).isEqualTo(10);
    }

    @Test
    @DisplayName("Should return empty list when queue is empty and timeout expires")
    void testPollBatch_TimeoutEmpty() throws Exception {
        when(queueRepository.leaseMessages(eq("empty-topic"), anyInt(), any(Duration.class)))
                .thenReturn(List.of());

        prefetchBatch = new PrefetchBatch("empty-topic", Duration.ofSeconds(30), 10, queueRepository, 1000);

        List<Message> polled = prefetchBatch.pollBatch(5, Duration.ofMillis(100));
        assertThat(polled).isEmpty();
    }

    @Test
    @DisplayName("Should cleanly close replenisher and clear heap")
    void testClose() throws Exception {
        prefetchBatch = new PrefetchBatch("test-close", Duration.ofSeconds(30), 10, queueRepository, 1000);

        assertThatCode(() -> prefetchBatch.close()).doesNotThrowAnyException();

        // Polling after close when empty returns empty
        List<Message> polled = prefetchBatch.pollBatch(1, Duration.ofMillis(50));
        assertThat(polled).isEmpty();
    }
}
