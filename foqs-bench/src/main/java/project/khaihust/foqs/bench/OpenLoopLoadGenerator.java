package project.khaihust.foqs.bench;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import project.khaihust.foqs.core.proto.EnqueueRequestDto;
import project.khaihust.foqs.core.proto.EnqueueResponseDto;
import project.khaihust.foqs.core.proto.EnqueueServiceGrpc;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Open-loop load generator that avoids coordinated omission.
 *
 * <h3>Why open-loop?</h3>
 * A closed-loop generator sends a request, waits for the response, records
 * latency, then sends the next request. This means when the server slows
 * down, the generator slows down too — it "omits" the requests that
 * <em>should</em> have been sent during the stall. The measured latencies
 * only reflect time-in-flight, not the queuing delay a real user would
 * experience. This is Gil Tene's "coordinated omission" problem.
 *
 * <h3>How this generator works</h3>
 * <ol>
 *   <li>A single sender thread computes how many messages <em>should</em>
 *       have been sent by now: {@code expected = elapsed_ns * targetRate / 1e9}</li>
 *   <li>It fires all "overdue" sends in a burst, each with its own
 *       {@code intendedSendTimeNs = startNs + msgIndex * intervalNs}</li>
 *   <li>Latency is recorded as {@code System.nanoTime() - intendedSendTimeNs}
 *       in the async callback, not as {@code now - actualSendTime}</li>
 *   <li>When the server stalls, overdue messages pile up and their latencies
 *       correctly reflect the full delay including queue time</li>
 * </ol>
 *
 * <h3>Backpressure</h3>
 * A {@link Semaphore} limits in-flight requests to prevent OOM. If the
 * semaphore cannot be acquired within 1 second, the send is counted as
 * an error (the server is overwhelmed).
 */
public class OpenLoopLoadGenerator {
    private static final Logger logger = LoggerFactory.getLogger(OpenLoopLoadGenerator.class);

    /** Maximum latency we track: 60 seconds in microseconds. */
    private static final long MAX_LATENCY_US = 60_000_000L;
    /** Minimum latency we track: 1 microsecond. */
    private static final long MIN_LATENCY_US = 1L;
    /** Cap per single run: 20 million messages. */
    private static final long MAX_MESSAGES = 20_000_000L;
    /** Spin-park interval between send bursts: 100 microseconds. */
    private static final long PARK_NS = 100_000L;

    private final ChannelPool channelPool;
    private final int targetRate;
    private final int warmupSeconds;
    private final int measurementSeconds;
    private final int payloadBytes;
    private final int maxInflight;
    private final java.util.List<String> topics;

    // -- Recording state (reset per run) --
    private final Recorder recorder;
    private final AtomicLong successCount = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();
    private final Semaphore semaphore;
    private volatile boolean stopped = false;

    public OpenLoopLoadGenerator(ChannelPool channelPool,
                                 int targetRate,
                                 int warmupSeconds,
                                 int measurementSeconds,
                                 int payloadBytes,
                                 int maxInflight,
                                 java.util.List<String> topics) {
        this.channelPool = channelPool;
        this.targetRate = targetRate;
        this.warmupSeconds = warmupSeconds;
        this.measurementSeconds = measurementSeconds;
        this.payloadBytes = payloadBytes;
        this.maxInflight = maxInflight;
        this.topics = (topics == null || topics.isEmpty()) ? java.util.List.of("bench-topic") : java.util.List.copyOf(topics);
        this.semaphore = new Semaphore(maxInflight);
        // HdrHistogram Recorder: thread-safe, double-buffered.
        // Range: 1μs – 60s, 3 significant figures.
        this.recorder = new Recorder(MIN_LATENCY_US, MAX_LATENCY_US, 3);
    }

    public OpenLoopLoadGenerator(ChannelPool channelPool,
                                 int targetRate,
                                 int warmupSeconds,
                                 int measurementSeconds,
                                 int payloadBytes,
                                 int maxInflight,
                                 String topic) {
        this(channelPool, targetRate, warmupSeconds, measurementSeconds, payloadBytes, maxInflight, java.util.List.of(topic));
    }

    /**
     * Runs the open-loop generator for warmup + measurement duration.
     * Blocks until complete. Returns the measurement-window results.
     */
    public BenchmarkResult run() {
        stopped = false;
        successCount.set(0);
        errorCount.set(0);
        // Drain any prior histogram data
        recorder.getIntervalHistogram();

        byte[] payloadRaw = new byte[payloadBytes];
        ThreadLocalRandom.current().nextBytes(payloadRaw);
        ByteString payload = ByteString.copyFrom(payloadRaw);

        long intervalNs = 1_000_000_000L / targetRate;
        long totalDurationNs = (long)(warmupSeconds + measurementSeconds) * 1_000_000_000L;
        long warmupDurationNs = (long) warmupSeconds * 1_000_000_000L;

        logger.info("Starting open-loop generator: targetRate={} msg/s, warmup={}s, measurement={}s, " +
                        "payload={}B, maxInflight={}, channels={}",
                targetRate, warmupSeconds, measurementSeconds, payloadBytes, maxInflight, channelPool.size());

        long startNs = System.nanoTime();
        long sentCount = 0;

        while (!stopped) {
            long now = System.nanoTime();
            long elapsedNs = now - startNs;

            if (elapsedNs >= totalDurationNs) break;
            if (sentCount >= MAX_MESSAGES) {
                logger.warn("Hit 20M message cap at sentCount={}", sentCount);
                break;
            }

            // How many messages should have been sent by now?
            long expectedSent = (elapsedNs * targetRate) / 1_000_000_000L;

            // Fire all overdue sends in a burst
            while (sentCount < expectedSent && sentCount < MAX_MESSAGES && !stopped) {
                long intendedSendTimeNs = startNs + sentCount * intervalNs;
                boolean pastWarmup = (intendedSendTimeNs - startNs) >= warmupDurationNs;

                try {
                    if (!semaphore.tryAcquire(1, TimeUnit.SECONDS)) {
                        // Semaphore exhausted — server can't keep up
                        if (pastWarmup) errorCount.incrementAndGet();
                        sentCount++;
                        continue;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    stopped = true;
                    break;
                }

                sendAsync(payload, intendedSendTimeNs, pastWarmup, sentCount);
                sentCount++;
            }

            // Brief park to avoid busy-spinning when caught up
            LockSupport.parkNanos(PARK_NS);
        }

        logger.info("Generator loop ended. Waiting for {} in-flight requests to complete...",
                maxInflight - semaphore.availablePermits());

        // Wait for all in-flight requests to complete (up to 30s)
        waitForInflight(30);

        // Snapshot the histogram
        Histogram histogram = recorder.getIntervalHistogram();

        long measurementMs = (long) measurementSeconds * 1000L;
        logger.info("Generator complete: success={}, errors={}, achieved={} msg/s, p99={}ms",
                successCount.get(), errorCount.get(),
                String.format("%.0f", (successCount.get() * 1000.0) / measurementMs),
                String.format("%.3f", histogram.getValueAtPercentile(99.0) / 1000.0));

        return new BenchmarkResult(
                histogram,
                successCount.get(),
                errorCount.get(),
                measurementMs,
                0.0,   // dequeueThroughput filled in by caller
                null   // explainExtra filled in by caller
        );
    }

    /** Stop the generator loop (called from another thread). */
    public void stop() {
        stopped = true;
    }

    /**
     * Send one enqueue RPC asynchronously.
     * The callback records latency against the intended send time.
     */
    private void sendAsync(ByteString payload, long intendedSendTimeNs, boolean recording, long msgIndex) {
        var channel = channelPool.getChannel();
        var stub = EnqueueServiceGrpc.newStub(channel);

        String topic = topics.get((int) (msgIndex % topics.size()));

        var request = EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(ThreadLocalRandom.current().nextInt(0, 10))
                .setPayload(payload)
                .setDeliverAfter(System.currentTimeMillis())
                .build();

        stub.enqueue(request, new StreamObserver<>() {
            @Override
            public void onNext(EnqueueResponseDto value) {
                if (recording) {
                    long latencyUs = (System.nanoTime() - intendedSendTimeNs) / 1000;
                    latencyUs = Math.max(latencyUs, MIN_LATENCY_US);
                    latencyUs = Math.min(latencyUs, MAX_LATENCY_US);
                    recorder.recordValue(latencyUs);
                    successCount.incrementAndGet();
                }
            }

            @Override
            public void onError(Throwable t) {
                if (recording) errorCount.incrementAndGet();
                semaphore.release();
            }

            @Override
            public void onCompleted() {
                semaphore.release();
            }
        });
    }

    /** Block until all semaphore permits are returned or timeout. */
    private void waitForInflight(int timeoutSeconds) {
        try {
            long deadline = System.nanoTime() + (long) timeoutSeconds * 1_000_000_000L;
            while (semaphore.availablePermits() < maxInflight) {
                if (System.nanoTime() > deadline) {
                    logger.warn("Timeout waiting for in-flight requests. {} still outstanding.",
                            maxInflight - semaphore.availablePermits());
                    break;
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
