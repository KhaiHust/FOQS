package project.khaihust.foqs.core.service;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import project.khaihust.foqs.core.buffer.IPrefetchBatch;
import project.khaihust.foqs.core.buffer.IPrefetchBufferRegistry;
import project.khaihust.foqs.core.enums.MessageStatus;
import project.khaihust.foqs.core.models.Message;
import project.khaihust.foqs.core.proto.DequeueRequestDto;
import project.khaihust.foqs.core.proto.DequeueResponseDto;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DequeueServiceTest {

    @Mock
    private IPrefetchBufferRegistry prefetchBufferRegistry;

    @Mock
    private IPrefetchBatch prefetchBatch;

    @Mock
    private StreamObserver<DequeueResponseDto> responseObserver;

    @Captor
    private ArgumentCaptor<DequeueResponseDto> responseCaptor;

    @Captor
    private ArgumentCaptor<Throwable> errorCaptor;

    private DequeueService dequeueService;

    @BeforeEach
    void setUp() {
        dequeueService = new DequeueService(prefetchBufferRegistry);
    }

    @Test
    @DisplayName("Should successfully dequeue messages and map all fields to DequeueResponseDto")
    void testDequeue_Success() throws Exception {
        String topic = "order-events";
        UUID msgId = UUID.randomUUID();
        byte[] payload = "{\"orderId\":123}".getBytes(StandardCharsets.UTF_8);
        Instant now = Instant.now();
        Instant leaseUntil = now.plusSeconds(30);

        Message message = Message.builder()
                .id(msgId)
                .topic(topic)
                .priority(1)
                .payload(payload)
                .status(MessageStatus.LEASED)
                .deliverAfter(now)
                .leaseUntil(leaseUntil)
                .retryCount(0)
                .createdAt(now)
                .build();

        when(prefetchBufferRegistry.getOrCreateBuffer(topic)).thenReturn(prefetchBatch);
        when(prefetchBatch.pollBatch(eq(5), any(Duration.class))).thenReturn(List.of(message));

        DequeueRequestDto request = DequeueRequestDto.newBuilder()
                .setTopic(topic)
                .setCount(5)
                .setTimeout(1000)
                .build();

        dequeueService.dequeue(request, responseObserver);

        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());

        DequeueResponseDto response = responseCaptor.getValue();
        assertThat(response.getMessagesCount()).isEqualTo(1);

        var dequeuedMsg = response.getMessages(0);
        assertThat(dequeuedMsg.getMessageId()).isEqualTo(msgId.toString());
        assertThat(dequeuedMsg.getTopic()).isEqualTo(topic);
        assertThat(dequeuedMsg.getPriority()).isEqualTo(1);
        assertThat(dequeuedMsg.getPayload()).isEqualTo(ByteString.copyFrom(payload));
        assertThat(dequeuedMsg.getLeaseUntilEpochMs()).isEqualTo(leaseUntil.toEpochMilli());
        assertThat(dequeuedMsg.getRetryCount()).isEqualTo(0);
        assertThat(dequeuedMsg.getCreatedAtEpochMs()).isEqualTo(now.toEpochMilli());
    }

    @Test
    @DisplayName("Should return INVALID_ARGUMENT when topic is null or blank")
    void testDequeue_BlankTopic() {
        DequeueRequestDto request = DequeueRequestDto.newBuilder()
                .setTopic("  ")
                .setCount(5)
                .setTimeout(1000)
                .build();

        dequeueService.dequeue(request, responseObserver);

        verify(responseObserver).onError(errorCaptor.capture());
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();

        Throwable error = errorCaptor.getValue();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException sre = (StatusRuntimeException) error;
        assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(sre.getStatus().getDescription()).contains("Topic cannot be empty");
    }

    @Test
    @DisplayName("Should return INVALID_ARGUMENT when count is less than or equal to zero")
    void testDequeue_InvalidCount() {
        DequeueRequestDto request = DequeueRequestDto.newBuilder()
                .setTopic("orders")
                .setCount(0)
                .setTimeout(1000)
                .build();

        dequeueService.dequeue(request, responseObserver);

        verify(responseObserver).onError(errorCaptor.capture());
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();

        Throwable error = errorCaptor.getValue();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException sre = (StatusRuntimeException) error;
        assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(sre.getStatus().getDescription()).contains("Count must be greater than 0");
    }

    @Test
    @DisplayName("Should return CANCELLED status when pollBatch is interrupted")
    void testDequeue_Interrupted() throws Exception {
        when(prefetchBufferRegistry.getOrCreateBuffer("orders")).thenReturn(prefetchBatch);
        when(prefetchBatch.pollBatch(anyInt(), any(Duration.class)))
                .thenThrow(new InterruptedException("Thread interrupted"));

        DequeueRequestDto request = DequeueRequestDto.newBuilder()
                .setTopic("orders")
                .setCount(2)
                .setTimeout(500)
                .build();

        dequeueService.dequeue(request, responseObserver);

        verify(responseObserver).onError(errorCaptor.capture());
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();

        Throwable error = errorCaptor.getValue();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException sre = (StatusRuntimeException) error;
        assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.CANCELLED);
    }

    @Test
    @DisplayName("Should return INTERNAL status when unexpected exception occurs")
    void testDequeue_UnexpectedException() throws Exception {
        when(prefetchBufferRegistry.getOrCreateBuffer("orders")).thenReturn(prefetchBatch);
        when(prefetchBatch.pollBatch(anyInt(), any(Duration.class)))
                .thenThrow(new RuntimeException("Database failure"));

        DequeueRequestDto request = DequeueRequestDto.newBuilder()
                .setTopic("orders")
                .setCount(2)
                .setTimeout(500)
                .build();

        dequeueService.dequeue(request, responseObserver);

        verify(responseObserver).onError(errorCaptor.capture());
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onCompleted();

        Throwable error = errorCaptor.getValue();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException sre = (StatusRuntimeException) error;
        assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    }
}
