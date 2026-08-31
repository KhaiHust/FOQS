package project.khaihust.foqs.core.buffer;

import project.khaihust.foqs.core.models.EnqueueRequest;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface IProducerBatch {
    CompletableFuture<UUID> enqueueAsync(EnqueueRequest enqueueRequest);
}
