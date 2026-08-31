package project.khaihust.foqs.core.buffer.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project.khaihust.foqs.core.buffer.IProducerBatch;
import project.khaihust.foqs.core.models.EnqueueRequest;
import project.khaihust.foqs.core.models.EnqueueTask;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerBatch implements IProducerBatch, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ProducerBatch.class);

    private final ISingleShardQueueRepository singleShardQueueRepository;
    private final BlockingQueue<EnqueueTask> writeBuffer;
    private final ScheduledExecutorService flusherExecutor;

    private final int batchSizeThreshold;

    private volatile boolean running = true;

    private final AtomicBoolean isFlushing = new AtomicBoolean(false);

    public ProducerBatch(ISingleShardQueueRepository singleShardQueueRepository,
                         int bufferCapacity,
                         int batchSizeThreshold,
                         int flushIntervalMs) {
        this.singleShardQueueRepository = singleShardQueueRepository;
        this.batchSizeThreshold = batchSizeThreshold;
        this.writeBuffer = new ArrayBlockingQueue<>(bufferCapacity);

        this.flusherExecutor = Executors.newSingleThreadScheduledExecutor(
                r -> {
                    Thread t = new Thread(r);
                    t.setName("ProducerBatch-Flusher");
                    t.setDaemon(true);
                    return t;
                }
        );
        this.flusherExecutor.scheduleWithFixedDelay(this::safeFlushAll,
                flushIntervalMs,
                flushIntervalMs,
                TimeUnit.MILLISECONDS);
    }


    @Override
    public CompletableFuture<UUID> enqueueAsync(EnqueueRequest enqueueRequest) {
        if (!running) {
            return CompletableFuture.failedFuture(new RejectedExecutionException("ProducerBatch is not running"));
        }

        var enqueueTask = new EnqueueTask(enqueueRequest);
        if (!writeBuffer.offer(enqueueTask)) {
            return CompletableFuture.failedFuture(new RejectedExecutionException("Write buffer is full"));
        }

        if (writeBuffer.size() >= batchSizeThreshold) {
            triggerFlushAsync();
        }

        return enqueueTask.getFuture();
    }

    private void triggerFlushAsync(){
        if(isFlushing.compareAndSet(false, true)){
            try{
                flusherExecutor.execute(() ->{
                    try {
                        flushAllPendingBatches();
                    } finally {
                        isFlushing.set(false);
                        if(writeBuffer.size() >= batchSizeThreshold){
                            triggerFlushAsync();
                        }
                    }
                });
            }
            catch (Exception e){
                logger.error("Failed to trigger flush asynchronously.", e);
                isFlushing.set(false);
            }
        }
    }

    private void safeFlushAll() {
        try {
            flushAllPendingBatches();
        } catch (Throwable t) {
            logger.error("Unexpected error in scheduled flush", t);
        }
    }

    private synchronized void flushAllPendingBatches() {
        while (!writeBuffer.isEmpty()) {
            var currentBatch = new ArrayList<EnqueueTask>(batchSizeThreshold);
            writeBuffer.drainTo(currentBatch, batchSizeThreshold);

            if (currentBatch.isEmpty()) {
                break;
            }

            try {
                singleShardQueueRepository.enqueueBatch(currentBatch);
            } catch (Exception e) {
                logger.error("Enqueue batch failed.", e);
                for (EnqueueTask enqueueTask : currentBatch) {
                    enqueueTask.getFuture().completeExceptionally(e);
                }
            }
        }

    }

    @Override
    public void close() {
        running = false;
        flusherExecutor.shutdown();
        try {
            if (!flusherExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                flusherExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            flusherExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        flushAllPendingBatches();
    }
}
