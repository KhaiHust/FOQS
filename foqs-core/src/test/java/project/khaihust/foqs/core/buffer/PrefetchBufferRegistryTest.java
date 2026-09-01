package project.khaihust.foqs.core.buffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import project.khaihust.foqs.core.buffer.impl.PrefetchBatch;
import project.khaihust.foqs.core.buffer.impl.PrefetchBufferRegistry;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@ExtendWith(MockitoExtension.class)
class PrefetchBufferRegistryTest {

    @Mock
    private ISingleShardQueueRepository queueRepository;

    private PrefetchBufferRegistry registry;

    @AfterEach
    void tearDown() {
        if (registry != null) {
            registry.close();
        }
    }

    @Test
    @DisplayName("Should create buffer for topic and return the same buffer on subsequent calls")
    void testGetOrCreateBuffer() {
        registry = new PrefetchBufferRegistry(queueRepository, 100, Duration.ofSeconds(30), 1000);

        PrefetchBatch buffer1 = registry.getOrCreateBuffer("topic-a");
        PrefetchBatch buffer2 = registry.getOrCreateBuffer("topic-a");
        PrefetchBatch bufferB = registry.getOrCreateBuffer("topic-b");

        assertThat(buffer1).isNotNull();
        assertThat(buffer2).isSameAs(buffer1);
        assertThat(bufferB).isNotNull().isNotSameAs(buffer1);
    }

    @Test
    @DisplayName("Should close all topic buffers upon registry close")
    void testClose() {
        registry = new PrefetchBufferRegistry(queueRepository, 100, Duration.ofSeconds(30), 1000);
        registry.getOrCreateBuffer("topic-1");
        registry.getOrCreateBuffer("topic-2");

        assertThatCode(() -> registry.close()).doesNotThrowAnyException();
    }
}
