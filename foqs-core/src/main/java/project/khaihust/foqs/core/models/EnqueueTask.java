package project.khaihust.foqs.core.models;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochGenerator;
import lombok.Getter;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Getter
public class EnqueueTask {
    private static final TimeBasedEpochGenerator UUID_V7_GENERATOR = Generators.timeBasedEpochGenerator();

    private final UUID messageId;
    private final EnqueueRequest enqueueRequest;
    private final CompletableFuture<UUID> future;

    public EnqueueTask(EnqueueRequest enqueueRequest) {
        this.messageId = UUID_V7_GENERATOR.generate();
        this.enqueueRequest = enqueueRequest;
        this.future = new CompletableFuture<>();
    }

}
