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

    @Test
    @DisplayName("Should route different topics to respective shard repositories in multi-shard mode")
    void testMultiShardRouting() {
        var shardRouter = new project.khaihust.foqs.core.config.ShardRouter(java.util.List.of(0, 1), 64);
        var repo0 = org.mockito.Mockito.mock(ISingleShardQueueRepository.class);
        var repo1 = org.mockito.Mockito.mock(ISingleShardQueueRepository.class);

        registry = new PrefetchBufferRegistry(
                shardRouter,
                java.util.Map.of(0, repo0, 1, repo1),
                100,
                Duration.ofSeconds(30),
                1000
        );

        String topicFor0 = null;
        String topicFor1 = null;
        int idx = 0;
        while (topicFor0 == null || topicFor1 == null) {
            String candidate = "topic-" + idx++;
            int shard = shardRouter.selectShard(candidate);
            if (shard == 0 && topicFor0 == null) topicFor0 = candidate;
            else if (shard == 1 && topicFor1 == null) topicFor1 = candidate;
        }

        PrefetchBatch buffer0 = registry.getOrCreateBuffer(topicFor0);
        PrefetchBatch buffer1 = registry.getOrCreateBuffer(topicFor1);

        assertThat(buffer0).isNotNull();
        assertThat(buffer1).isNotNull();
        assertThat(buffer0).isNotSameAs(buffer1);
    }

    @Test
    @DisplayName("Should throw IllegalStateException when topic routes to unconfigured shard")
    void testMissingShardRepository() {
        var shardRouter = new project.khaihust.foqs.core.config.ShardRouter(java.util.List.of(0, 1), 64);
        var repo0 = org.mockito.Mockito.mock(ISingleShardQueueRepository.class);

        // Only shard 0 is configured, shard 1 is missing
        registry = new PrefetchBufferRegistry(
                shardRouter,
                java.util.Map.of(0, repo0),
                100,
                Duration.ofSeconds(30),
                1000
        );

        String topicFor1 = null;
        int idx = 0;
        while (topicFor1 == null) {
            String candidate = "topic-" + idx++;
            if (shardRouter.selectShard(candidate) == 1) {
                topicFor1 = candidate;
            }
        }

        final String missingTopic = topicFor1;
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> registry.getOrCreateBuffer(missingTopic))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No repository configured for shard: 1");
    }
}
