package project.khaihust.foqs.core.buffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import project.khaihust.foqs.core.buffer.impl.ShardedProducerBatch;
import project.khaihust.foqs.core.config.ShardRouter;
import project.khaihust.foqs.core.models.EnqueueRequest;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ShardedProducerBatchTest {

    @Mock
    private IProducerBatch batch0;

    @Mock
    private IProducerBatch batch1;

    private ShardRouter shardRouter;
    private ShardedProducerBatch shardedBatch;

    @BeforeEach
    void setUp() {
        shardRouter = new ShardRouter(List.of(0, 1), 64);
        shardedBatch = new ShardedProducerBatch(shardRouter, Map.of(0, batch0, 1, batch1));
    }

    @AfterEach
    void tearDown() {
        if (shardedBatch != null) {
            try {
                shardedBatch.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("Should route enqueueAsync to the correct shard based on topic")
    void testEnqueueAsyncRouting() throws Exception {
        String topicFor0 = null;
        String topicFor1 = null;

        int idx = 0;
        while (topicFor0 == null || topicFor1 == null) {
            String candidate = "topic-" + idx++;
            int shard = shardRouter.selectShard(candidate);
            if (shard == 0 && topicFor0 == null) {
                topicFor0 = candidate;
            } else if (shard == 1 && topicFor1 == null) {
                topicFor1 = candidate;
            }
        }

        UUID id0 = UUID.randomUUID();
        UUID id1 = UUID.randomUUID();

        EnqueueRequest req0 = EnqueueRequest.builder()
                .topic(topicFor0)
                .priority(1)
                .payload("payload-0".getBytes())
                .deliverAfter(System.currentTimeMillis())
                .build();

        EnqueueRequest req1 = EnqueueRequest.builder()
                .topic(topicFor1)
                .priority(1)
                .payload("payload-1".getBytes())
                .deliverAfter(System.currentTimeMillis())
                .build();

        when(batch0.enqueueAsync(req0)).thenReturn(CompletableFuture.completedFuture(id0));
        when(batch1.enqueueAsync(req1)).thenReturn(CompletableFuture.completedFuture(id1));

        CompletableFuture<UUID> future0 = shardedBatch.enqueueAsync(req0);
        CompletableFuture<UUID> future1 = shardedBatch.enqueueAsync(req1);

        assertThat(future0.get()).isEqualTo(id0);
        assertThat(future1.get()).isEqualTo(id1);

        verify(batch0).enqueueAsync(req0);
        verify(batch1).enqueueAsync(req1);
    }

    @Test
    @DisplayName("Should fail future when EnqueueRequest or topic is null or blank")
    void testEnqueueAsync_InvalidRequest() {
        CompletableFuture<UUID> nullReqFuture = shardedBatch.enqueueAsync(null);
        assertThat(nullReqFuture).isCompletedExceptionally();
        assertThatThrownBy(nullReqFuture::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);

        EnqueueRequest nullTopicReq = EnqueueRequest.builder().topic(null).build();
        CompletableFuture<UUID> nullTopicFuture = shardedBatch.enqueueAsync(nullTopicReq);
        assertThat(nullTopicFuture).isCompletedExceptionally();
        assertThatThrownBy(nullTopicFuture::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);

        EnqueueRequest blankTopicReq = EnqueueRequest.builder().topic("   ").build();
        CompletableFuture<UUID> blankTopicFuture = shardedBatch.enqueueAsync(blankTopicReq);
        assertThat(blankTopicFuture).isCompletedExceptionally();
        assertThatThrownBy(blankTopicFuture::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should close all shard batches upon close()")
    void testClose_AllBatchesClosed() throws Exception {
        shardedBatch.close();

        verify(batch0).close();
        verify(batch1).close();
    }

    @Test
    @DisplayName("Should attempt to close all batches even if one throws during close")
    void testClose_HandlesBatchCloseException() throws Exception {
        doThrow(new RuntimeException("Close error on batch0")).when(batch0).close();

        assertThatThrownBy(() -> shardedBatch.close())
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Close error on batch0");

        verify(batch1).close();
    }
}
