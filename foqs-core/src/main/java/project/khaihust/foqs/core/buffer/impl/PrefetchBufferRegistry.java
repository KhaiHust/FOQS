package project.khaihust.foqs.core.buffer.impl;

import project.khaihust.foqs.core.buffer.IPrefetchBatch;
import project.khaihust.foqs.core.buffer.IPrefetchBufferRegistry;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PrefetchBufferRegistry implements IPrefetchBufferRegistry, AutoCloseable {
    private final ISingleShardQueueRepository queueRepository;
    private final ConcurrentMap<String, PrefetchBatch> buffers = new ConcurrentHashMap<>();
    private final int defaultTargetSize;
    private final Duration defaultLeaseDuration;
    private final long defaultRefillIntervalMs;

    public PrefetchBufferRegistry(ISingleShardQueueRepository queueRepository,
                                  int defaultTargetSize,
                                  Duration defaultLeaseDuration,
                                  long defaultRefillIntervalMs) {
        this.queueRepository = queueRepository;
        this.defaultTargetSize = defaultTargetSize;
        this.defaultLeaseDuration = defaultLeaseDuration;
        this.defaultRefillIntervalMs = defaultRefillIntervalMs;
    }

    public PrefetchBatch getOrCreateBuffer(String topic){
        return buffers.computeIfAbsent(topic, t -> new PrefetchBatch(
                t,
                defaultLeaseDuration,
                defaultTargetSize,
                queueRepository,
                defaultRefillIntervalMs
        ));
    }
    @Override
    public void close()  {
        buffers.values().forEach(PrefetchBatch::close);
        buffers.clear();
    }
}
