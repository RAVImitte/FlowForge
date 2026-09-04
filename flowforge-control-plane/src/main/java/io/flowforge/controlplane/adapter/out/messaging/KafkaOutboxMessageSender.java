package io.flowforge.controlplane.adapter.out.messaging;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Component
@ConditionalOnProperty(prefix = "flowforge.outbox", name = "publisher-enabled", havingValue = "true")
public class KafkaOutboxMessageSender implements OutboxMessageSender {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaOutboxMessageSender(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(OutboxMessage message, Duration timeout) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                message.topic(),
                message.recordKey(),
                message.payload()
        );
        addHeader(record, "flowforge-event-id", message.id().toString());
        addHeader(record, "flowforge-event-type", message.eventType());
        addHeader(record, "flowforge-schema-version", Integer.toString(message.schemaVersion()));
        addHeader(record, "flowforge-correlation-id", message.workflowExecutionId().toString());
        kafkaTemplate.send(record).get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
