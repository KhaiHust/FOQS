package project.khaihust.foqs.core.service;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import project.khaihust.foqs.core.buffer.IProducerBatch;
import project.khaihust.foqs.core.models.EnqueueRequest;
import project.khaihust.foqs.core.proto.EnqueueRequestDto;
import project.khaihust.foqs.core.proto.EnqueueResponseDto;
import project.khaihust.foqs.core.proto.EnqueueServiceGrpc;

@RequiredArgsConstructor
public class EnqueueService extends EnqueueServiceGrpc.EnqueueServiceImplBase {
    private final IProducerBatch producerBatch;
    @Override
    public void enqueue(EnqueueRequestDto requestDto, StreamObserver<EnqueueResponseDto> responseObserver) {
        EnqueueRequest enqueueRequest = toEnqueueRequest(requestDto);
        producerBatch.enqueueAsync(enqueueRequest)
                .thenAccept(messageId -> {
                    EnqueueResponseDto responseDto = EnqueueResponseDto.newBuilder()
                            .setMessageId(messageId.toString())
                            .build();
                    responseObserver.onNext(responseDto);
                    responseObserver.onCompleted();
                })
                .exceptionally(ex -> {
                    responseObserver.onError(ex);
                    return null;
                });
    }

    private EnqueueRequest toEnqueueRequest(EnqueueRequestDto requestDto) {
        return EnqueueRequest.builder()
                .topic(requestDto.getTopic())
                .priority(requestDto.getPriority())
                .payload(requestDto.getPayload().toByteArray())
                .deliverAfter(requestDto.getDeliverAfter())
                .build();
    }
}
