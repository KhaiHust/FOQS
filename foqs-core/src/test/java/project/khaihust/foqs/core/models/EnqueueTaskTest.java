package project.khaihust.foqs.core.models;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EnqueueTaskTest {

    @Test
    @DisplayName("Should generate valid UUIDv7 for each EnqueueTask")
    void testUuidV7Generation() {
        EnqueueRequest request = EnqueueRequest.builder()
                .topic("orders")
                .priority(1)
                .payload("data".getBytes(StandardCharsets.UTF_8))
                .deliverAfter(System.currentTimeMillis())
                .build();

        EnqueueTask task = new EnqueueTask(request);

        UUID messageId = task.getMessageId();
        assertThat(messageId).isNotNull();
        assertThat(messageId.version()).isEqualTo(7);
        assertThat(task.getEnqueueRequest()).isSameAs(request);
        assertThat(task.getFuture()).isNotNull();
        assertThat(task.getFuture().isDone()).isFalse();
    }

    @Test
    @DisplayName("Should generate monotonically increasing UUIDv7 identifiers")
    void testUuidV7Monotonicity() {
        EnqueueRequest request = EnqueueRequest.builder()
                .topic("orders")
                .priority(1)
                .payload("data".getBytes(StandardCharsets.UTF_8))
                .deliverAfter(System.currentTimeMillis())
                .build();

        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ids.add(new EnqueueTask(request).getMessageId());
        }

        // Verify all IDs are UUIDv7
        for (UUID id : ids) {
            assertThat(id.version()).isEqualTo(7);
        }

        // Verify sorted order matches generation order
        for (int i = 1; i < ids.size(); i++) {
            assertThat(ids.get(i - 1)).isLessThan(ids.get(i));
        }
    }
}
