package project.khaihust.foqs.core.models;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class EnqueueRequest {
    String topic;
    int priority;
    byte[] payload;
    long deliverAfter;
}
