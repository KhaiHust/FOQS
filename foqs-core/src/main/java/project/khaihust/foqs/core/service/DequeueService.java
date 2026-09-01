package project.khaihust.foqs.core.service;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import project.khaihust.foqs.core.buffer.IPrefetchBufferRegistry;
import project.khaihust.foqs.core.proto.DequeueRequestDto;
import project.khaihust.foqs.core.proto.DequeueResponseDto;
import project.khaihust.foqs.core.proto.DequeueServiceGrpc;
import project.khaihust.foqs.core.proto.DequeuedMessageDto;

import java.time.Duration;

@RequiredArgsConstructor
public class DequeueService extends DequeueServiceGrpc.DequeueServiceImplBase {
    private final IPrefetchBufferRegistry prefetchBufferRegistry;

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
