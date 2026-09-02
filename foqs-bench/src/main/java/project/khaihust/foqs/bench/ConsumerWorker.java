package project.khaihust.foqs.bench;

import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project.khaihust.foqs.core.proto.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background consumer that dequeues messages and (optionally) ACKs them.
 *
 * <p>For baseline and batch-sweep experiments, the consumer runs at full
 * speed to drain the queue and prevent backlog from contaminating the
 * enqueue latency measurement.
 *
 * <p>For the backlog experiment, set {@code maxRatePerSec > 0} to throttle
 * consumption to ~50% of the enqueue rate, forcing queue growth.
 *
 * <p>For lease recovery, set {@code ackMessages = false} so messages remain
 * LEASED without acknowledgment, then kill the consumer to trigger
 * lease expiration and reclamation.
 */
public class ConsumerWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);

    private final ChannelPool channelPool;
    private final String topic;
    private final int batchSize;
    private final int timeoutMs;
    private final boolean ackMessages;
    private final int maxRatePerSec;  // 0 = unlimited

    private final AtomicLong totalDequeued = new AtomicLong();
    private final AtomicLong totalAcked = new AtomicLong();
    private final List<String> lastDequeuedIds = new ArrayList<>();

    private volatile boolean stopped = false;
    private volatile long firstDequeueAfterResumeNs = 0;

    public ConsumerWorker(ChannelPool channelPool,
                          String topic,
                          int batchSize,
                          int timeoutMs,
                          boolean ackMessages,
                          int maxRatePerSec) {
        this.channelPool = channelPool;
        this.topic = topic;
        this.batchSize = batchSize;
        this.timeoutMs = timeoutMs;
        this.ackMessages = ackMessages;
        this.maxRatePerSec = maxRatePerSec;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("consumer-" + Thread.currentThread().getId());
        logger.info("Consumer started: topic={}, batchSize={}, ack={}, maxRate={}",
                topic, batchSize, ackMessages, maxRatePerSec > 0 ? maxRatePerSec : "unlimited");

        var dequeueStub = DequeueServiceGrpc.newBlockingStub(channelPool.getChannel());

        long windowStartNs = System.nanoTime();
        long windowDequeued = 0;

        while (!stopped) {
            try {
                // Rate limiting: if maxRatePerSec > 0, throttle
                if (maxRatePerSec > 0 && windowDequeued > 0) {
                    long elapsedNs = System.nanoTime() - windowStartNs;
                    double expectedNs = (windowDequeued * 1_000_000_000.0) / maxRatePerSec;
                    if (elapsedNs < expectedNs) {
                        long sleepNs = (long)(expectedNs - elapsedNs);
                        TimeUnit.NANOSECONDS.sleep(sleepNs);
                    }
                    // Reset window every second to avoid drift
                    if (elapsedNs > 1_000_000_000L) {
                        windowStartNs = System.nanoTime();
                        windowDequeued = 0;
                    }
                }

                // Dequeue a batch
                DequeueResponseDto response = dequeueStub.dequeue(
                        DequeueRequestDto.newBuilder()
                                .setTopic(topic)
                                .setCount(batchSize)
                                .setTimeout(timeoutMs)
                                .build()
                );

                List<DequeuedMessageDto> messages = response.getMessagesList();
                if (messages.isEmpty()) continue;

                int count = messages.size();
                totalDequeued.addAndGet(count);
                windowDequeued += count;

                // Track for lease recovery experiment
                if (firstDequeueAfterResumeNs == 0) {
                    firstDequeueAfterResumeNs = System.nanoTime();
                }
                synchronized (lastDequeuedIds) {
                    lastDequeuedIds.clear();
                    messages.forEach(m -> lastDequeuedIds.add(m.getMessageId()));
                }

                // ACK the batch
                if (ackMessages) {
                    List<String> ids = messages.stream()
                            .map(DequeuedMessageDto::getMessageId)
                            .toList();

                    BatchAckResponseDto ackResponse = dequeueStub.batchAck(
                            BatchAckRequestDto.newBuilder()
                                    .addAllMessageIds(ids)
                                    .build()
                    );
                    totalAcked.addAndGet(ackResponse.getAckedMessageIdsCount());
                }

            } catch (StatusRuntimeException e) {
                if (!stopped) {
                    logger.warn("Consumer gRPC error: {}", e.getStatus());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!stopped) {
                    logger.warn("Consumer error: {}", e.getMessage());
                }
            }
        }
        logger.info("Consumer stopped. Total dequeued={}, acked={}", totalDequeued.get(), totalAcked.get());
    }

    /** Signal the consumer to stop after the current iteration. */
    public void stop() {
        stopped = true;
    }

    /** Total messages dequeued during this consumer's lifetime. */
    public long getTotalDequeued() {
        return totalDequeued.get();
    }

    public long getTotalAcked() {
        return totalAcked.get();
    }

    /**
     * Returns the nanoTime of the first dequeue after the consumer started/resumed.
     * Used by the lease-recovery experiment to measure time-to-redelivery.
     */
    public long getFirstDequeueAfterResumeNs() {
        return firstDequeueAfterResumeNs;
    }

    /** Reset the firstDequeueAfterResumeNs marker (call before restarting after kill). */
    public void resetResumeMarker() {
        firstDequeueAfterResumeNs = 0;
    }

    /** Returns the most recently dequeued message IDs (snapshot). */
    public List<String> getLastDequeuedIds() {
        synchronized (lastDequeuedIds) {
            return new ArrayList<>(lastDequeuedIds);
        }
    }
}
