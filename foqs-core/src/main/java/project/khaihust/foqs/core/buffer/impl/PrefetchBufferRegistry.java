package project.khaihust.foqs.core.buffer.impl;

import project.khaihust.foqs.core.buffer.IPrefetchBufferRegistry;
import project.khaihust.foqs.core.config.ShardRouter;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PrefetchBufferRegistry implements IPrefetchBufferRegistry, AutoCloseable {
    private final ShardRouter shardRouter;
    private final Map<Integer, ISingleShardQueueRepository> shardRepositories;
    private final ConcurrentMap<String, PrefetchBatch> buffers = new ConcurrentHashMap<>();
    private final int defaultTargetSize;
    private final Duration defaultLeaseDuration;
    private final long defaultRefillIntervalMs;

    public PrefetchBufferRegistry(ISingleShardQueueRepository queueRepository,
                                  int defaultTargetSize,
                                  Duration defaultLeaseDuration,
                                  long defaultRefillIntervalMs) {
        this(new ShardRouter(List.of(0), 128), Map.of(0, queueRepository), defaultTargetSize, defaultLeaseDuration, defaultRefillIntervalMs);
    }

    public PrefetchBufferRegistry(ShardRouter shardRouter,
                                  Map<Integer, ISingleShardQueueRepository> shardRepositories,
                                  int defaultTargetSize,
                                  Duration defaultLeaseDuration,
                                  long defaultRefillIntervalMs) {
        if (shardRouter == null) {
            throw new IllegalArgumentException("shardRouter must not be null");
        }
        if (shardRepositories == null || shardRepositories.isEmpty()) {
            throw new IllegalArgumentException("shardRepositories must not be null or empty");
        }
        this.shardRouter = shardRouter;
        this.shardRepositories = Map.copyOf(shardRepositories);
        this.defaultTargetSize = defaultTargetSize;
        this.defaultLeaseDuration = defaultLeaseDuration;
        this.defaultRefillIntervalMs = defaultRefillIntervalMs;
    }

    @Override
    public PrefetchBatch getOrCreateBuffer(String topic) {
        return buffers.computeIfAbsent(topic, t -> {
            var shardId = shardRouter.selectShard(t);
            var repo = shardRepositories.get(shardId);
            if (repo == null) {
                throw new IllegalStateException("No repository configured for shard: " + shardId);
            }
            return new PrefetchBatch(
                    t,
                    defaultLeaseDuration,
                    defaultTargetSize,
                    repo,
                    defaultRefillIntervalMs
            );
        });
    }

    @Override
    public void close() {
        buffers.values().forEach(PrefetchBatch::close);
        buffers.clear();
    }
}

