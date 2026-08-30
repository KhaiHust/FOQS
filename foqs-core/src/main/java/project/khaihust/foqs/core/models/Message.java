package project.khaihust.foqs.core.models;

import lombok.Builder;
import lombok.Value;
import project.khaihust.foqs.core.enums.MessageStatus;

import java.time.Instant;
import java.util.UUID;

@Value
@Builder(toBuilder = true)
public class Message {
    UUID id;
    String topic;
    int priority;
    byte[] payload;
    MessageStatus status;
    Instant deliverAfter;
    Instant leaseUntil;
    int retryCount;
    Instant createdAt;
}
