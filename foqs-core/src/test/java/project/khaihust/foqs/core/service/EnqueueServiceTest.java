package project.khaihust.foqs.core.service;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import project.khaihust.foqs.core.buffer.IProducerBatch;
import project.khaihust.foqs.core.models.EnqueueRequest;
import project.khaihust.foqs.core.proto.EnqueueRequestDto;
import project.khaihust.foqs.core.proto.EnqueueResponseDto;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EnqueueServiceTest {

    @Mock
    private IProducerBatch producerBatch;

    @Mock
    private StreamObserver<EnqueueResponseDto> responseObserver;

    @Captor
    private ArgumentCaptor<EnqueueResponseDto> responseCaptor;

    @Captor
    private ArgumentCaptor<EnqueueRequest> requestCaptor;

    @Captor
    private ArgumentCaptor<Throwable> errorCaptor;

    private EnqueueService enqueueService;

    @BeforeEach
    void setUp() {
        enqueueService = new EnqueueService(producerBatch);
    }

    @Test
    @DisplayName("Should successfully process enqueue request and return message_id in response observer")
    void testEnqueue_Success() {
        UUID expectedUuid = UUID.randomUUID();
        when(producerBatch.enqueueAsync(any(EnqueueRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedUuid));

        EnqueueRequestDto requestDto = EnqueueRequestDto.newBuilder()
                .setTopic("user-notifications")
                .setPriority(1)
                .setPayload(ByteString.copyFromUtf8("notification payload"))
                .setDeliverAfter(System.currentTimeMillis() + 5000)
                .build();

        enqueueService.enqueue(requestDto, responseObserver);

        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());

        EnqueueResponseDto actualResponse = responseCaptor.getValue();
        assertThat(actualResponse.getMessageId()).isEqualTo(expectedUuid.toString());
    }

    @Test
    @DisplayName("Should invoke response observer onError when producer batch fails with exception")
    void testEnqueue_ProducerBatchException() {
        RejectedExecutionException expectedException = new RejectedExecutionException("Write buffer is full");
        CompletableFuture<UUID> failedFuture = CompletableFuture.failedFuture(expectedException);
        when(producerBatch.enqueueAsync(any(EnqueueRequest.class)))
                .thenReturn(failedFuture);

        EnqueueRequestDto requestDto = EnqueueRequestDto.newBuilder()
                .setTopic("failing-topic")
                .setPriority(2)
                .setPayload(ByteString.copyFromUtf8("test"))
                .setDeliverAfter(System.currentTimeMillis())
                .build();

        enqueueService.enqueue(requestDto, responseObserver);

        verify(responseObserver).onError(errorCaptor.capture());
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();

        Throwable capturedError = errorCaptor.getValue();
        assertThat(capturedError)
                .isInstanceOf(CompletionException.class)
                .hasCause(expectedException);
    }

    @Test
    @DisplayName("Should correctly map all fields from EnqueueRequestDto to EnqueueRequest model")
    void testEnqueue_VerifyRequestConversion() {
        String topic = "order-checkout-topic";
        int priority = 7;
        byte[] payloadBytes = "{\"orderId\": 999, \"amount\": 49.99}".getBytes(StandardCharsets.UTF_8);
        long deliverAfter = 1750000000000L;

        EnqueueRequestDto requestDto = EnqueueRequestDto.newBuilder()
                .setTopic(topic)
                .setPriority(priority)
                .setPayload(ByteString.copyFrom(payloadBytes))
                .setDeliverAfter(deliverAfter)
                .build();

        when(producerBatch.enqueueAsync(requestCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(UUID.randomUUID()));

        enqueueService.enqueue(requestDto, responseObserver);

        EnqueueRequest convertedRequest = requestCaptor.getValue();
        assertThat(convertedRequest).isNotNull();
        assertThat(convertedRequest.getTopic()).isEqualTo(topic);
        assertThat(convertedRequest.getPriority()).isEqualTo(priority);
        assertThat(convertedRequest.getPayload()).isEqualTo(payloadBytes);
        assertThat(convertedRequest.getDeliverAfter()).isEqualTo(deliverAfter);
    }
}
