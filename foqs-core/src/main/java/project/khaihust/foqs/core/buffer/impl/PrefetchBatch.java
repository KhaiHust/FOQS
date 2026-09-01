package project.khaihust.foqs.core.buffer.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import project.khaihust.foqs.core.buffer.IPrefetchBatch;
import project.khaihust.foqs.core.models.Message;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@RequiredArgsConstructor
public class PrefetchBatch implements IPrefetchBatch, AutoCloseable {
    private final String topic;
    private final Duration leaseDuration;
    private final int targetCapacity;

    private final ISingleShardQueueRepository  queueRepository;
    private final PriorityBlockingQueue<Message> minHeap;
    private final ScheduledExecutorService replenisher;

    private final AtomicBoolean isReplenished = new AtomicBoolean(false);
    private volatile boolean running = true;

    public PrefetchBatch(String topic, Duration leaseDuration, int targetCapacity, ISingleShardQueueRepository queueRepository, long refillIntervalMs) {
        this.topic = topic;
        this.leaseDuration = leaseDuration;
        this.targetCapacity = targetCapacity;
        this.queueRepository = queueRepository;
        this.minHeap = new PriorityBlockingQueue<>(Math.max(1, targetCapacity),
                Comparator.comparingInt(Message::getPriority)
                .thenComparing(Message::getId));

        this.replenisher = Executors.newSingleThreadScheduledExecutor(
                r -> {
                    Thread t = new Thread(r, "foqs-prefetch-" + topic);
                    t.setDaemon(true);
                    return t;
                }
        );
        this.replenisher.scheduleWithFixedDelay(this::replenish,
                0,
                refillIntervalMs,
                TimeUnit.MILLISECONDS);
    }


    @Override
    public List<Message> pollBatch(int maxCount, Duration timeout) throws InterruptedException {
        var batches = new ArrayList<Message>(maxCount);

        var deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (batches.size() < maxCount) {
            var remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0 && !batches.isEmpty()) {
                break;
            }
            if (remainingNanos <= 0) {
                break;
            }

            var message = minHeap.poll(Math.max(0, remainingNanos), TimeUnit.NANOSECONDS);
            if (message == null) {
                break;
            }

            batches.add(message);
        }

        if (running && minHeap.size() < (targetCapacity / 2)) {
            replenisher.execute(this::replenish);
        }

        return batches;
    }

    @Override
    public void close() {
        running = false;
        replenisher.shutdown();
        try {
            if (!replenisher.awaitTermination(5, TimeUnit.SECONDS)) {
                replenisher.shutdownNow();
            }
        } catch (InterruptedException e) {
            replenisher.shutdownNow();
            Thread.currentThread().interrupt();
        }
        minHeap.clear();
    }


    private void replenish() {
        if (!running || !isReplenished.compareAndSet(false, true)) {
            return;
        }

        try{
            var needed = targetCapacity - minHeap.size();
            var messages = queueRepository.leaseMessages(topic, needed, leaseDuration);

            if(!messages.isEmpty()){
                minHeap.addAll(messages);
            }

        } catch (Exception e){
            log.error("Failed to replenish messages for topic {}: {}", topic, e.getMessage(), e);
        } finally {
            isReplenished.set(false);
        }
    }
}
