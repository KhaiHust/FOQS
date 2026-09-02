package project.khaihust.foqs.bench;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round-robin pool of gRPC ManagedChannels.
 *
 * Each ManagedChannel maps to one HTTP/2 connection (one TCP socket).
 * A single HTTP/2 connection can multiplex many streams but will saturate
 * at ~30-50k msgs/s due to head-of-line blocking on the TCP layer.
 * Pooling 4-8 channels gives 120-400k msgs/s headroom before MySQL
 * becomes the bottleneck.
 */
public class ChannelPool implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ChannelPool.class);

    private final ManagedChannel[] channels;
    private final AtomicLong counter = new AtomicLong();

    public ChannelPool(String host, int port, int poolSize) {
        if (poolSize < 1) throw new IllegalArgumentException("poolSize must be >= 1");
        this.channels = new ManagedChannel[poolSize];
        for (int i = 0; i < poolSize; i++) {
            channels[i] = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .maxInboundMessageSize(16 * 1024 * 1024) // 16 MB
                    .build();
        }
        logger.info("Created channel pool: {} channels to {}:{}", poolSize, host, port);
    }

    /**
     * Returns the next channel via round-robin.
     * Thread-safe; uses an atomic counter with modular indexing.
     */
    public ManagedChannel getChannel() {
        long idx = counter.getAndIncrement();
        // Mask to positive value to avoid negative index from overflow
        return channels[(int) (Math.abs(idx) % channels.length)];
    }

    public int size() {
        return channels.length;
    }

    @Override
    public void close() {
        for (ManagedChannel ch : channels) {
            try {
                ch.shutdown();
                if (!ch.awaitTermination(5, TimeUnit.SECONDS)) {
                    ch.shutdownNow();
                }
            } catch (InterruptedException e) {
                ch.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        logger.info("Channel pool closed ({} channels)", channels.length);
    }
}
