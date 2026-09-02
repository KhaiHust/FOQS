package project.khaihust.foqs.core.buffer.impl;

import project.khaihust.foqs.core.buffer.IProducerBatch;
import project.khaihust.foqs.core.config.ShardRouter;
import project.khaihust.foqs.core.models.EnqueueRequest;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ShardedProducerBatch implements IProducerBatch, AutoCloseable {
    private final ShardRouter shardRouter;
    private final Map<Integer, IProducerBatch> shardBatches;

    public ShardedProducerBatch(ShardRouter shardRouter,
                                Map<Integer, ISingleShardQueueRepository> repositories,
                                int bufferCapacityPerShard,
                                int batchThreshold,
                                int flushIntervalMs) {
        this(shardRouter, repositories.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> new ProducerBatch(e.getValue(), bufferCapacityPerShard, batchThreshold, flushIntervalMs)
        )));
    }

    public ShardedProducerBatch(ShardRouter shardRouter, Map<Integer, IProducerBatch> shardBatches) {
        if (shardRouter == null) {
            throw new IllegalArgumentException("shardRouter must not be null");
        }
        if (shardBatches == null || shardBatches.isEmpty()) {
            throw new IllegalArgumentException("shardBatches must not be null or empty");
        }
        this.shardRouter = shardRouter;
        this.shardBatches = Collections.unmodifiableMap(shardBatches);
    }

    @Override
    public void close() throws Exception {
        Exception firstException = null;
        for (var batch : shardBatches.values()) {
            try {
                batch.close();
            } catch (Exception e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    @Override
    public CompletableFuture<UUID> enqueueAsync(EnqueueRequest enqueueRequest) {
        if (enqueueRequest == null || enqueueRequest.getTopic() == null || enqueueRequest.getTopic().isBlank()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("EnqueueRequest and topic must not be null or blank"));
        }
        int shardId;
        try {
            shardId = shardRouter.selectShard(enqueueRequest.getTopic());
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }

        var batch = shardBatches.get(shardId);
        if (batch == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("No producer batch for shard: " + shardId));
        }
        return batch.enqueueAsync(enqueueRequest);
    }

    public Map<Integer, IProducerBatch> getShardBatches() {
        return shardBatches;
    }
}

