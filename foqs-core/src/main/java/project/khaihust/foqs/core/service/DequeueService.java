package project.khaihust.foqs.core.service;

import com.fasterxml.uuid.impl.UUIDUtil;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import project.khaihust.foqs.core.buffer.IPrefetchBufferRegistry;
import project.khaihust.foqs.core.proto.*;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.time.Duration;
import java.util.*;

@Slf4j
@RequiredArgsConstructor
public class DequeueService extends DequeueServiceGrpc.DequeueServiceImplBase {
    private static final int DEFAULT_MAX_RETRIES = 5;
    private final IPrefetchBufferRegistry prefetchBufferRegistry;
    private final Map<Integer, ISingleShardQueueRepository> shardQueueRepositories;

    public DequeueService(IPrefetchBufferRegistry prefetchBufferRegistry, ISingleShardQueueRepository singleRepo) {
        this(prefetchBufferRegistry, Map.of(0, singleRepo));
    }

    @Override
    public void batchAck(BatchAckRequestDto request, StreamObserver<BatchAckResponseDto> responseObserver) {
        try {
            var incomingUuids = request.getMessageIdsList().stream()
                    .map(UUID::fromString)
                    .toList();

            var remaining = new LinkedHashSet<>(incomingUuids);
            var allAcked = new ArrayList<UUID>();

            for (var repo : shardQueueRepositories.values()) {
                if (remaining.isEmpty()) {
                    break;
                }

                var acked = repo.ackMessages(new ArrayList<>(remaining));
                allAcked.addAll(acked);
                remaining.removeAll(new HashSet<>(acked));
            }
            var response = BatchAckResponseDto.newBuilder()
                    .addAllAckedMessageIds(allAcked.stream().map(UUID::toString).toList())
                    .addAllFailedMessageIds(remaining.stream().map(UUID::toString).toList())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("One or more message IDs are invalid UUIDs")
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("Batch ACK failed", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void batchNack(BatchNackRequestDto request, StreamObserver<BatchNackResponseDto> responseObserver) {
        try {
            var messageIds = request.getMessageIdsList().stream()
                    .map(UUID::fromString)
                    .toList();

            var retryDelayMs = request.getRetryDelayMs();
            var maxRetryCount = request.getMaxRetryCount() <= 0 ? DEFAULT_MAX_RETRIES : request.getMaxRetryCount();

            var updatedCount = 0;
            for (var repo : shardQueueRepositories.values()) {
                updatedCount += repo.nackMessages(messageIds, retryDelayMs, maxRetryCount);
            }
            responseObserver.onNext(BatchNackResponseDto.newBuilder()
                    .setSuccessCount(updatedCount)
                    .build());
            responseObserver.onCompleted();

        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("One or more message IDs are invalid UUIDs")
                    .asRuntimeException());
        } catch (Exception e) {
            log.error("Failed to process Batch NACK", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void dequeue(DequeueRequestDto request, StreamObserver<DequeueResponseDto> responseObserver) {
        var topic = request.getTopic();
        var maxCount = request.getCount();
        var timeoutDuration = Duration.ofMillis(Math.max(0, request.getTimeout()));

        if (topic.isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Topic cannot be empty").asRuntimeException());
            return;
        }

        if (maxCount <= 0) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Count must be greater than 0").asRuntimeException());
            return;
        }

        var prefetchBuffer = prefetchBufferRegistry.getOrCreateBuffer(topic);

        try {
            var messages = prefetchBuffer.pollBatch(maxCount, timeoutDuration);
            var dequeuedResponses = DequeueResponseDto.newBuilder();
            for (var msg : messages) {
                var messageBuilder = DequeuedMessageDto.newBuilder()
                        .setMessageId(msg.getId().toString())
                        .setTopic(msg.getTopic() != null ? msg.getTopic() : "")
                        .setPriority(msg.getPriority())
                        .setPayload(msg.getPayload() != null ? ByteString.copyFrom(msg.getPayload()) : ByteString.EMPTY)
                        .setLeaseUntilEpochMs(msg.getLeaseUntil() != null ? msg.getLeaseUntil().toEpochMilli() : 0L)
                        .setRetryCount(msg.getRetryCount())
                        .setCreatedAtEpochMs(msg.getCreatedAt() != null ? msg.getCreatedAt().toEpochMilli() : 0L);

                dequeuedResponses.addMessages(messageBuilder.build());
            }

            responseObserver.onNext(dequeuedResponses.build());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            responseObserver.onError(Status.CANCELLED.withDescription("Dequeue interrupted").asRuntimeException());
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

}
