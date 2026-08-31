package project.khaihust.foqs.core.buffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import project.khaihust.foqs.core.buffer.impl.ProducerBatch;
import project.khaihust.foqs.core.models.EnqueueRequest;
import project.khaihust.foqs.core.models.EnqueueTask;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ProducerBatchTest {

    @Mock
    private ISingleShardQueueRepository singleShardQueueRepository;

    @Captor
    private ArgumentCaptor<List<EnqueueTask>> taskListCaptor;

    private ProducerBatch producerBatch;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
        if (producerBatch != null) {
            producerBatch.close();
        }
    }

    private EnqueueRequest createRequest(String topic, int priority) {
        return EnqueueRequest.builder()
                .topic(topic)
                .priority(priority)
                .payload(("Payload for " + topic).getBytes())
                .deliverAfter(System.currentTimeMillis())
                .build();
    }

    private void mockRepositoryCompleteSuccess() throws SQLException {
        doAnswer(invocation -> {
            List<EnqueueTask> tasks = invocation.getArgument(0);
            for (EnqueueTask task : tasks) {
                task.getFuture().complete(task.getMessageId());
            }
            return null;
        }).when(singleShardQueueRepository).enqueueBatch(anyList());
    }

    @Test
    @DisplayName("Happy path: enqueueAsync succeeds, batch is flushed to repository, and CompletableFuture is completed with expected UUID")
    void testHappyPath_EnqueueAsyncSuccess() throws Exception {
        mockRepositoryCompleteSuccess();
        producerBatch = new ProducerBatch(singleShardQueueRepository, 10, 2, 1000);

        EnqueueRequest request1 = createRequest("topic-1", 1);
        EnqueueRequest request2 = createRequest("topic-2", 2);

        CompletableFuture<UUID> future1 = producerBatch.enqueueAsync(request1);
        CompletableFuture<UUID> future2 = producerBatch.enqueueAsync(request2);

        UUID uuid1 = future1.get(2, TimeUnit.SECONDS);
        UUID uuid2 = future2.get(2, TimeUnit.SECONDS);

        assertThat(uuid1).isNotNull();
        assertThat(uuid2).isNotNull();
        assertThat(uuid1).isNotEqualTo(uuid2);

        verify(singleShardQueueRepository, times(1)).enqueueBatch(taskListCaptor.capture());
        List<EnqueueTask> flushedTasks = taskListCaptor.getValue();
        assertThat(flushedTasks).hasSize(2);
        assertThat(flushedTasks.get(0).getMessageId()).isEqualTo(uuid1);
        assertThat(flushedTasks.get(0).getEnqueueRequest().getTopic()).isEqualTo("topic-1");
        assertThat(flushedTasks.get(1).getMessageId()).isEqualTo(uuid2);
        assertThat(flushedTasks.get(1).getEnqueueRequest().getTopic()).isEqualTo("topic-2");
    }

    @Test
    @DisplayName("Buffer full: when writeBuffer capacity is reached, new enqueue returns a failed future with RejectedExecutionException")
    void testBufferFull_ThrowsRejectedExecutionException() {
        // Capacity = 2, Threshold = 10, long flush interval so items stay in buffer
        producerBatch = new ProducerBatch(singleShardQueueRepository, 2, 10, 60000);

        CompletableFuture<UUID> future1 = producerBatch.enqueueAsync(createRequest("topic-1", 1));
        CompletableFuture<UUID> future2 = producerBatch.enqueueAsync(createRequest("topic-2", 1));
        CompletableFuture<UUID> future3 = producerBatch.enqueueAsync(createRequest("topic-3", 1));

        assertThat(future1).isNotCompletedExceptionally();
        assertThat(future2).isNotCompletedExceptionally();
        assertThat(future3).isCompletedExceptionally();

        assertThatThrownBy(future3::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(RejectedExecutionException.class)
                .hasMessageContaining("Write buffer is full");
    }

    @Test
    @DisplayName("Producer closed: when producer is closed, enqueue returns a failed future with RejectedExecutionException")
    void testProducerClosed_ThrowsRejectedExecutionException() {
        producerBatch = new ProducerBatch(singleShardQueueRepository, 10, 2, 1000);
        producerBatch.close();

        CompletableFuture<UUID> future = producerBatch.enqueueAsync(createRequest("topic-closed", 1));

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(RejectedExecutionException.class)
                .hasMessageContaining("ProducerBatch is not running");
    }

    @Test
    @DisplayName("Threshold flush: enqueuing batchSizeThreshold items triggers flushing via executor")
    void testThresholdFlush_TriggersFlushingViaExecutor() throws Exception {
        mockRepositoryCompleteSuccess();
        // batchSizeThreshold = 3, high flush interval so only threshold triggers flush
        producerBatch = new ProducerBatch(singleShardQueueRepository, 10, 3, 60000);

        CompletableFuture<UUID> future1 = producerBatch.enqueueAsync(createRequest("topic-1", 1));
        CompletableFuture<UUID> future2 = producerBatch.enqueueAsync(createRequest("topic-2", 2));

        // Short sleep to confirm that with fewer than 3 items, repository is not called yet
        Thread.sleep(100);
        verify(singleShardQueueRepository, never()).enqueueBatch(anyList());
        assertThat(future1).isNotDone();
        assertThat(future2).isNotDone();

        // Enqueue 3rd item to hit threshold
        CompletableFuture<UUID> future3 = producerBatch.enqueueAsync(createRequest("topic-3", 3));

        CompletableFuture.allOf(future1, future2, future3).get(2, TimeUnit.SECONDS);

        assertThat(future1).isCompletedWithValueMatching(uuid -> uuid != null);
        assertThat(future2).isCompletedWithValueMatching(uuid -> uuid != null);
        assertThat(future3).isCompletedWithValueMatching(uuid -> uuid != null);

        verify(singleShardQueueRepository, times(1)).enqueueBatch(taskListCaptor.capture());
        assertThat(taskListCaptor.getValue()).hasSize(3);
    }

    @Test
    @DisplayName("Periodic linger flush: enqueuing fewer items than threshold still gets flushed when scheduled flusher runs")
    void testPeriodicLingerFlush_FlushedWhenScheduledFlusherRuns() throws Exception {
        mockRepositoryCompleteSuccess();
        // batchSizeThreshold = 10, flushIntervalMs = 50ms (quick linger flush)
        producerBatch = new ProducerBatch(singleShardQueueRepository, 10, 10, 50);

        CompletableFuture<UUID> future1 = producerBatch.enqueueAsync(createRequest("topic-linger-1", 1));
        CompletableFuture<UUID> future2 = producerBatch.enqueueAsync(createRequest("topic-linger-2", 2));

        // Wait for linger flusher to fire
        CompletableFuture.allOf(future1, future2).get(2, TimeUnit.SECONDS);

        assertThat(future1).isCompletedWithValueMatching(uuid -> uuid != null);
        assertThat(future2).isCompletedWithValueMatching(uuid -> uuid != null);

        verify(singleShardQueueRepository, times(1)).enqueueBatch(taskListCaptor.capture());
        assertThat(taskListCaptor.getValue()).hasSize(2);
    }

    @Test
    @DisplayName("Failure handling: when repository throws SQLException, all tasks in that batch have futures completed exceptionally")
    void testFailureHandling_RepositoryThrowsSQLException() throws Exception {
        doThrow(new SQLException("Database connection lost"))
                .when(singleShardQueueRepository).enqueueBatch(anyList());

        producerBatch = new ProducerBatch(singleShardQueueRepository, 10, 2, 1000);

        CompletableFuture<UUID> future1 = producerBatch.enqueueAsync(createRequest("topic-fail-1", 1));
        CompletableFuture<UUID> future2 = producerBatch.enqueueAsync(createRequest("topic-fail-2", 2));

        assertThatThrownBy(() -> future1.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(SQLException.class)
                .hasMessageContaining("Database connection lost");

        assertThatThrownBy(() -> future2.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(SQLException.class)
                .hasMessageContaining("Database connection lost");

        assertThat(future1).isCompletedExceptionally();
        assertThat(future2).isCompletedExceptionally();
    }

    @Test
    @DisplayName("Failure handling: when repository throws RuntimeException, all tasks in that batch have futures completed exceptionally")
    void testFailureHandling_RepositoryThrowsRuntimeException() throws Exception {
        doThrow(new RuntimeException("Unexpected runtime error"))
                .when(singleShardQueueRepository).enqueueBatch(anyList());

        producerBatch = new ProducerBatch(singleShardQueueRepository, 10, 2, 1000);

        CompletableFuture<UUID> future1 = producerBatch.enqueueAsync(createRequest("topic-rt-1", 1));
        CompletableFuture<UUID> future2 = producerBatch.enqueueAsync(createRequest("topic-rt-2", 2));

        assertThatThrownBy(() -> future1.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unexpected runtime error");

        assertThatThrownBy(() -> future2.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasMessageContaining("Unexpected runtime error");
    }

    @Test
    @DisplayName("Graceful shutdown: calling close() shuts down executor and flushes pending tasks in buffer")
    void testGracefulShutdown_FlushesPendingTasksInBuffer() throws Exception {
        mockRepositoryCompleteSuccess();
        // batchSizeThreshold = 10, high flushIntervalMs so items remain buffered
        producerBatch = new ProducerBatch(singleShardQueueRepository, 10, 10, 60000);

        CompletableFuture<UUID> future1 = producerBatch.enqueueAsync(createRequest("topic-shutdown-1", 1));
        CompletableFuture<UUID> future2 = producerBatch.enqueueAsync(createRequest("topic-shutdown-2", 2));
        CompletableFuture<UUID> future3 = producerBatch.enqueueAsync(createRequest("topic-shutdown-3", 3));

        assertThat(future1).isNotDone();
        assertThat(future2).isNotDone();
        assertThat(future3).isNotDone();

        // Close triggers flush of pending items
        producerBatch.close();

        assertThat(future1).isCompletedWithValueMatching(uuid -> uuid != null);
        assertThat(future2).isCompletedWithValueMatching(uuid -> uuid != null);
        assertThat(future3).isCompletedWithValueMatching(uuid -> uuid != null);

        verify(singleShardQueueRepository, times(1)).enqueueBatch(taskListCaptor.capture());
        assertThat(taskListCaptor.getValue()).hasSize(3);

        // After close, new enqueues are rejected
        CompletableFuture<UUID> futurePostClose = producerBatch.enqueueAsync(createRequest("topic-post", 1));
        assertThat(futurePostClose).isCompletedExceptionally();
    }

    @Test
    @DisplayName("Multiple batches: drains buffer in chunks of batchSizeThreshold when size exceeds threshold")
    void testMultipleBatchesDrained() throws Exception {
        mockRepositoryCompleteSuccess();
        // batchSizeThreshold = 2, bufferCapacity = 10, high flush interval
        producerBatch = new ProducerBatch(singleShardQueueRepository, 10, 2, 60000);

        List<CompletableFuture<UUID>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(producerBatch.enqueueAsync(createRequest("topic-" + i, i)));
        }

        // Close will flush any remaining items
        producerBatch.close();

        for (CompletableFuture<UUID> future : futures) {
            assertThat(future.get(2, TimeUnit.SECONDS)).isNotNull();
        }

        verify(singleShardQueueRepository, times(3)).enqueueBatch(taskListCaptor.capture());
        List<List<EnqueueTask>> capturedBatches = taskListCaptor.getAllValues();
        assertThat(capturedBatches.get(0)).hasSize(2);
        assertThat(capturedBatches.get(1)).hasSize(2);
        assertThat(capturedBatches.get(2)).hasSize(1);
    }

    @Test
    @DisplayName("Concurrency: concurrent producers enqueueing items all succeed and flush cleanly")
    void testConcurrentEnqueue() throws Exception {
        mockRepositoryCompleteSuccess();
        int threadCount = 10;
        int itemsPerThread = 20;
        int totalItems = threadCount * itemsPerThread;

        producerBatch = new ProducerBatch(singleShardQueueRepository, 500, 10, 20);

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<CompletableFuture<UUID>> allFutures = Collections.synchronizedList(new ArrayList<>());
        Set<UUID> completedUuids = ConcurrentHashMap.newKeySet();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < itemsPerThread; i++) {
                        EnqueueRequest req = createRequest("topic-" + threadId + "-" + i, i);
                        CompletableFuture<UUID> future = producerBatch.enqueueAsync(req);
                        allFutures.add(future);
                        future.thenAccept(completedUuids::add);
                    }
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        executorService.shutdown();
        assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

        assertThat(completedUuids).hasSize(totalItems);
    }

    @Test
    @DisplayName("Idempotent close: multiple close() calls do not throw exceptions")
    void testMultipleCloseCallsAreIdempotent() {
        producerBatch = new ProducerBatch(singleShardQueueRepository, 10, 2, 1000);
        assertThatCode(() -> {
            producerBatch.close();
            producerBatch.close();
            producerBatch.close();
        }).doesNotThrowAnyException();
    }
}
