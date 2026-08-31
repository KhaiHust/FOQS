package project.khaihust.foqs.core.models;

import lombok.Getter;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Getter
public class EnqueueTask {
    private final UUID messageId;
    private final EnqueueRequest enqueueRequest;
    private final CompletableFuture<UUID> future;

    public EnqueueTask(EnqueueRequest enqueueRequest) {
        this.messageId = UUID.randomUUID();
        this.enqueueRequest = enqueueRequest;
        this.future = new CompletableFuture<>();
    }

}
