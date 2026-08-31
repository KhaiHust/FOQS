package project.khaihust.foqs.core.models;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import project.khaihust.foqs.core.enums.MessageStatus;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MessageTest {

    @Test
    @DisplayName("Should build Message object correctly with all fields")
    void testMessageBuilderAndGetters() {
        UUID id = UUID.randomUUID();
        String topic = "order-created";
        byte[] payload = "{\"orderId\":123}".getBytes(StandardCharsets.UTF_8);
        Instant now = Instant.now();
        Instant deliverAfter = now.plusSeconds(60);
        Instant leaseUntil = now.plusSeconds(120);

        Message message = Message.builder()
                .id(id)
                .topic(topic)
                .priority(5)
                .payload(payload)
                .status(MessageStatus.READY)
                .deliverAfter(deliverAfter)
                .leaseUntil(leaseUntil)
                .retryCount(0)
                .createdAt(now)
                .build();

        assertThat(message.getId()).isEqualTo(id);
        assertThat(message.getTopic()).isEqualTo(topic);
        assertThat(message.getPriority()).isEqualTo(5);
        assertThat(message.getPayload()).isEqualTo(payload);
        assertThat(message.getStatus()).isEqualTo(MessageStatus.READY);
        assertThat(message.getDeliverAfter()).isEqualTo(deliverAfter);
        assertThat(message.getLeaseUntil()).isEqualTo(leaseUntil);
        assertThat(message.getRetryCount()).isEqualTo(0);
        assertThat(message.getCreatedAt()).isEqualTo(now);
    }

    @Test
    @DisplayName("Should support toBuilder() for immutability updates")
    void testToBuilder() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        Instant leaseUntil = now.plusSeconds(30);

        Message original = Message.builder()
                .id(id)
                .topic("notification-topic")
                .priority(1)
                .payload("hello".getBytes(StandardCharsets.UTF_8))
                .status(MessageStatus.READY)
                .deliverAfter(now)
                .retryCount(0)
                .createdAt(now)
                .build();

        Message leased = original.toBuilder()
                .status(MessageStatus.LEASED)
                .leaseUntil(leaseUntil)
                .retryCount(1)
                .build();

        // Check original is unchanged
        assertThat(original.getStatus()).isEqualTo(MessageStatus.READY);
        assertThat(original.getLeaseUntil()).isNull();
        assertThat(original.getRetryCount()).isEqualTo(0);

        // Check new instance has updated fields and preserved other fields
        assertThat(leased.getId()).isEqualTo(id);
        assertThat(leased.getTopic()).isEqualTo("notification-topic");
        assertThat(leased.getStatus()).isEqualTo(MessageStatus.LEASED);
        assertThat(leased.getLeaseUntil()).isEqualTo(leaseUntil);
        assertThat(leased.getRetryCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should verify equals and hashCode contracts")
    void testEqualsAndHashCode() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        byte[] payload = "test-data".getBytes(StandardCharsets.UTF_8);

        Message msg1 = Message.builder()
                .id(id)
                .topic("topic-a")
                .priority(10)
                .payload(payload)
                .status(MessageStatus.READY)
                .deliverAfter(now)
                .createdAt(now)
                .build();

        Message msg2 = Message.builder()
                .id(id)
                .topic("topic-a")
                .priority(10)
                .payload(payload)
                .status(MessageStatus.READY)
                .deliverAfter(now)
                .createdAt(now)
                .build();

        Message msg3 = msg1.toBuilder().priority(20).build();

        assertThat(msg1).isEqualTo(msg2);
        assertThat(msg1.hashCode()).isEqualTo(msg2.hashCode());
        assertThat(msg1).isNotEqualTo(msg3);
    }
}
