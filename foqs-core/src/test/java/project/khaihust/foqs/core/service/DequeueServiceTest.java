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
import project.khaihust.foqs.core.proto.*;
import project.khaihust.foqs.core.storage.ISingleShardQueueRepository;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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
    private ISingleShardQueueRepository singleShardQueueRepository;

    @Mock
    private StreamObserver<DequeueResponseDto> responseObserver;

    @Mock
    private StreamObserver<BatchAckResponseDto> batchAckResponseObserver;

    @Mock
    private StreamObserver<BatchNackResponseDto> batchNackResponseObserver;

    @Captor
    private ArgumentCaptor<DequeueResponseDto> responseCaptor;

    @Captor
    private ArgumentCaptor<BatchAckResponseDto> batchAckResponseCaptor;

    @Captor
    private ArgumentCaptor<BatchNackResponseDto> batchNackResponseCaptor;

    @Captor
    private ArgumentCaptor<Throwable> errorCaptor;

    private DequeueService dequeueService;

    @BeforeEach
    void setUp() {
        dequeueService = new DequeueService(prefetchBufferRegistry, singleShardQueueRepository);
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

    @Test
    @DisplayName("Should successfully batch ack messages and populate ackedMessageIds in response")
    void testBatchAck_Success() throws Exception {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        when(singleShardQueueRepository.ackMessages(List.of(id1, id2))).thenReturn(List.of(id1, id2));

        BatchAckRequestDto request = BatchAckRequestDto.newBuilder()
                .addAllMessageIds(List.of(id1.toString(), id2.toString()))
                .build();

        dequeueService.batchAck(request, batchAckResponseObserver);

        verify(batchAckResponseObserver).onNext(batchAckResponseCaptor.capture());
        verify(batchAckResponseObserver).onCompleted();
        verify(batchAckResponseObserver, never()).onError(any());

        BatchAckResponseDto response = batchAckResponseCaptor.getValue();
        assertThat(response.getAckedMessageIdsList()).containsExactlyInAnyOrder(id1.toString(), id2.toString());
        assertThat(response.getFailedMessageIdsList()).isEmpty();
    }

    @Test
    @DisplayName("Should handle partial failure in batch ack and populate both acked and failed message IDs")
    void testBatchAck_PartialFailure() throws Exception {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();
        when(singleShardQueueRepository.ackMessages(List.of(id1, id2, id3))).thenReturn(List.of(id1, id3));

        BatchAckRequestDto request = BatchAckRequestDto.newBuilder()
                .addAllMessageIds(List.of(id1.toString(), id2.toString(), id3.toString()))
                .build();

        dequeueService.batchAck(request, batchAckResponseObserver);

        verify(batchAckResponseObserver).onNext(batchAckResponseCaptor.capture());
        verify(batchAckResponseObserver).onCompleted();
        verify(batchAckResponseObserver, never()).onError(any());

        BatchAckResponseDto response = batchAckResponseCaptor.getValue();
        assertThat(response.getAckedMessageIdsList()).containsExactlyInAnyOrder(id1.toString(), id3.toString());
        assertThat(response.getFailedMessageIdsList()).containsExactly(id2.toString());
    }

    @Test
    @DisplayName("Should return INVALID_ARGUMENT on batch ack when message IDs contains invalid UUID")
    void testBatchAck_InvalidUuids() {
        BatchAckRequestDto request = BatchAckRequestDto.newBuilder()
                .addAllMessageIds(List.of("not-a-valid-uuid"))
                .build();

        dequeueService.batchAck(request, batchAckResponseObserver);

        verify(batchAckResponseObserver).onError(errorCaptor.capture());
        verify(batchAckResponseObserver, never()).onNext(any());
        verify(batchAckResponseObserver, never()).onCompleted();

        Throwable error = errorCaptor.getValue();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException sre = (StatusRuntimeException) error;
        assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(sre.getStatus().getDescription()).contains("One or more message IDs are invalid UUIDs");
    }

    @Test
    @DisplayName("Should return INTERNAL on batch ack when repository throws exception")
    void testBatchAck_RepositoryException() throws Exception {
        UUID id1 = UUID.randomUUID();
        when(singleShardQueueRepository.ackMessages(any())).thenThrow(new SQLException("Database failure"));

        BatchAckRequestDto request = BatchAckRequestDto.newBuilder()
                .addMessageIds(id1.toString())
                .build();

        dequeueService.batchAck(request, batchAckResponseObserver);

        verify(batchAckResponseObserver).onError(errorCaptor.capture());
        verify(batchAckResponseObserver, never()).onNext(any());
        verify(batchAckResponseObserver, never()).onCompleted();

        Throwable error = errorCaptor.getValue();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException sre = (StatusRuntimeException) error;
        assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    }

    @Test
    @DisplayName("Should successfully batch nack messages with custom retry delay and max retries")
    void testBatchNack_Success() throws Exception {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        when(singleShardQueueRepository.nackMessages(List.of(id1, id2), 3000L, 4)).thenReturn(2);

        BatchNackRequestDto request = BatchNackRequestDto.newBuilder()
                .addAllMessageIds(List.of(id1.toString(), id2.toString()))
                .setRetryDelayMs(3000L)
                .setMaxRetryCount(4)
                .build();

        dequeueService.batchNack(request, batchNackResponseObserver);

        verify(batchNackResponseObserver).onNext(batchNackResponseCaptor.capture());
        verify(batchNackResponseObserver).onCompleted();
        verify(batchNackResponseObserver, never()).onError(any());

        BatchNackResponseDto response = batchNackResponseCaptor.getValue();
        assertThat(response.getSuccessCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should fallback to DEFAULT_MAX_RETRIES when maxRetryCount is 0 or negative")
    void testBatchNack_FallbackToDefaultMaxRetries() throws Exception {
        UUID id1 = UUID.randomUUID();
        when(singleShardQueueRepository.nackMessages(List.of(id1), 0L, 5)).thenReturn(1);

        BatchNackRequestDto request = BatchNackRequestDto.newBuilder()
                .addMessageIds(id1.toString())
                .setRetryDelayMs(0L)
                .setMaxRetryCount(0)
                .build();

        dequeueService.batchNack(request, batchNackResponseObserver);

        verify(singleShardQueueRepository).nackMessages(List.of(id1), 0L, 5);
        verify(batchNackResponseObserver).onNext(batchNackResponseCaptor.capture());
        verify(batchNackResponseObserver).onCompleted();
        verify(batchNackResponseObserver, never()).onError(any());

        BatchNackResponseDto response = batchNackResponseCaptor.getValue();
        assertThat(response.getSuccessCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should return INVALID_ARGUMENT on batch nack when message IDs contains invalid UUID")
    void testBatchNack_InvalidUuids() {
        BatchNackRequestDto request = BatchNackRequestDto.newBuilder()
                .addMessageIds("not-a-valid-uuid")
                .setRetryDelayMs(1000L)
                .setMaxRetryCount(3)
                .build();

        dequeueService.batchNack(request, batchNackResponseObserver);

        verify(batchNackResponseObserver).onError(errorCaptor.capture());
        verify(batchNackResponseObserver, never()).onNext(any());
        verify(batchNackResponseObserver, never()).onCompleted();

        Throwable error = errorCaptor.getValue();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException sre = (StatusRuntimeException) error;
        assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(sre.getStatus().getDescription()).contains("One or more message IDs are invalid UUIDs");
    }

    @Test
    @DisplayName("Should return INTERNAL on batch nack when repository throws exception")
    void testBatchNack_RepositoryException() throws Exception {
        UUID id1 = UUID.randomUUID();
        when(singleShardQueueRepository.nackMessages(any(), anyLong(), anyInt()))
                .thenThrow(new RuntimeException("Storage failure"));

        BatchNackRequestDto request = BatchNackRequestDto.newBuilder()
                .addMessageIds(id1.toString())
                .setRetryDelayMs(1000L)
                .setMaxRetryCount(3)
                .build();

        dequeueService.batchNack(request, batchNackResponseObserver);

        verify(batchNackResponseObserver).onError(errorCaptor.capture());
        verify(batchNackResponseObserver, never()).onNext(any());
        verify(batchNackResponseObserver, never()).onCompleted();

        Throwable error = errorCaptor.getValue();
        assertThat(error).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException sre = (StatusRuntimeException) error;
        assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
    }
}
