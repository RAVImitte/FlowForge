package io.flowforge.controlplane.adapter.out.messaging;

import io.flowforge.application.recovery.DeadLetterLocation;
import io.flowforge.application.recovery.DeadLetterRecord;
import io.flowforge.application.recovery.DeadLetterTransport;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.kafka.KafkaConsumerRecoveryFactory;
import io.flowforge.messaging.FlowForgeHeaders;
import io.flowforge.messaging.FlowForgeTopics;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaDeadLetterTransport implements DeadLetterTransport {
    private static final List<String> REPLAY_HEADERS = List.of(
            FlowForgeHeaders.EVENT_ID,
            FlowForgeHeaders.EVENT_TYPE,
            FlowForgeHeaders.SCHEMA_VERSION,
            FlowForgeHeaders.CORRELATION_ID,
            FlowForgeHeaders.TENANT_ID,
            "traceparent",
            "tracestate",
            "baggage"
    );

    private final ConsumerFactory<String, String> consumers;
    private final KafkaTemplate<String, String> kafka;
    private final MeterRegistry meters;

    public KafkaDeadLetterTransport(
            ConsumerFactory<String, String> consumers,
            KafkaTemplate<String, String> kafka,
            MeterRegistry meters
    ) {
        this.consumers = consumers;
        this.kafka = kafka;
        this.meters = meters;
    }

    @Override
    public Optional<DeadLetterRecord> read(DeadLetterLocation location, Duration timeout) {
        String sourceTopic = FlowForgeTopics.sourceTopicForDeadLetter(location.topic());
        TopicPartition partition = new TopicPartition(location.topic(), location.partition());
        try (Consumer<String, String> consumer = consumers.createConsumer(
                "flowforge-dlq-inspector-", UUID.randomUUID().toString()
        )) {
            consumer.assign(List.of(partition));
            long beginning = consumer.beginningOffsets(List.of(partition), timeout).get(partition);
            long end = consumer.endOffsets(List.of(partition), timeout).get(partition);
            if (location.offset() < beginning || location.offset() >= end) return Optional.empty();
            consumer.seek(partition, location.offset());
            return consumer.poll(timeout).records(partition).stream()
                    .filter(record -> record.offset() == location.offset())
                    .findFirst()
                    .map(record -> toDeadLetterRecord(location, sourceTopic, record));
        }
    }

    @Override
    public void replay(DeadLetterRecord record, Duration timeout) {
        ProducerRecord<String, String> replay = new ProducerRecord<>(
                record.sourceTopic(), record.location().partition(), record.key(), record.value()
        );
        record.replayHeaders().forEach((name, value) -> replay.headers().add(
                name, value.getBytes(StandardCharsets.UTF_8)
        ));
        try {
            kafka.send(replay).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            meters.counter("flowforge.kafka.dlq.replay.published", "source_topic", record.sourceTopic())
                    .increment();
        } catch (Exception failure) {
            meters.counter("flowforge.kafka.dlq.replay.failures", "source_topic", record.sourceTopic())
                    .increment();
            throw new IllegalStateException("Kafka did not acknowledge dead-letter replay", failure);
        }
    }

    private static DeadLetterRecord toDeadLetterRecord(
            DeadLetterLocation location,
            String sourceTopic,
            ConsumerRecord<String, String> record
    ) {
        String recordId = requiredSingleHeader(record, KafkaConsumerRecoveryFactory.DLQ_RECORD_ID_HEADER);
        if (!recordId.startsWith(sourceTopic + ":" + location.partition() + ":")) {
            throw new IllegalArgumentException("Dead-letter source identity does not match its allowed source topic");
        }
        String tenant = requiredSingleHeader(record, FlowForgeHeaders.TENANT_ID);
        String failureClass = requiredSingleHeader(record, KafkaConsumerRecoveryFactory.DLQ_FAILURE_CLASS_HEADER);
        Instant failedAt;
        try {
            failedAt = Instant.parse(requiredSingleHeader(record, KafkaConsumerRecoveryFactory.DLQ_FAILED_AT_HEADER));
        } catch (java.time.format.DateTimeParseException invalid) {
            throw new IllegalArgumentException("Dead-letter failure timestamp is invalid", invalid);
        }

        Map<String, String> replayHeaders = new LinkedHashMap<>();
        for (String name : REPLAY_HEADERS) {
            Header header = record.headers().lastHeader(name);
            if (header != null) replayHeaders.put(name, text(header));
        }
        return new DeadLetterRecord(
                location, sourceTopic, recordId, new TenantId(tenant), record.key(), record.value(),
                replayHeaders, failureClass, failedAt
        );
    }

    private static String requiredSingleHeader(ConsumerRecord<String, String> record, String name) {
        List<Header> values = new java.util.ArrayList<>();
        record.headers().headers(name).forEach(values::add);
        if (values.size() != 1) throw new IllegalArgumentException("Dead-letter header " + name + " must occur once");
        String value = text(values.getFirst());
        if (value.isBlank()) throw new IllegalArgumentException("Dead-letter header " + name + " must not be blank");
        return value;
    }

    private static String text(Header header) {
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
