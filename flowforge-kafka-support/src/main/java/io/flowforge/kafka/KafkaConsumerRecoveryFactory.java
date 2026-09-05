package io.flowforge.kafka;

import io.flowforge.messaging.FlowForgeTopics;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

public final class KafkaConsumerRecoveryFactory {
    static final String DLQ_SCHEMA_VERSION_HEADER = "flowforge-dlq-schema-version";
    static final String DLQ_RECORD_ID_HEADER = "flowforge-dlq-record-id";
    static final String DLQ_FAILURE_CLASS_HEADER = "flowforge-dlq-failure-class";
    static final String DLQ_FAILED_AT_HEADER = "flowforge-dlq-failed-at";

    private KafkaConsumerRecoveryFactory() {
    }

    public static DefaultErrorHandler create(
            KafkaTemplate<String, String> kafka,
            MeterRegistry meters,
            Clock clock,
            int maxRetries,
            Duration initialBackoff,
            double multiplier,
            Duration maxBackoff,
            Duration publishTimeout
    ) {
        ExponentialBackOffWithMaxRetries backOff = backOff(
                maxRetries, initialBackoff, multiplier, maxBackoff
        );
        DeadLetterPublishingRecoverer publisher = publisher(kafka, clock, publishTimeout);
        DefaultErrorHandler handler = new DefaultErrorHandler((record, failure) -> {
            publisher.accept(record, failure);
            recordRecoveryMetrics(meters, clock, record);
        }, backOff);
        handler.setCommitRecovered(true);
        handler.setAckAfterHandle(true);
        handler.setResetStateOnRecoveryFailure(true);
        handler.setRetryListeners(retryMetrics(meters));
        return handler;
    }

    static ExponentialBackOffWithMaxRetries backOff(
            int maxRetries,
            Duration initialBackoff,
            double multiplier,
            Duration maxBackoff
    ) {
        if (maxRetries < 0) throw new IllegalArgumentException("Kafka recovery max retries must not be negative");
        if (initialBackoff.isNegative() || initialBackoff.isZero()) {
            throw new IllegalArgumentException("Kafka recovery initial backoff must be positive");
        }
        if (multiplier < 1.0) throw new IllegalArgumentException("Kafka recovery multiplier must be at least 1.0");
        if (maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException("Kafka recovery max backoff must not be less than initial backoff");
        }
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialBackoff.toMillis());
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxBackoff.toMillis());
        return backOff;
    }

    static DeadLetterPublishingRecoverer publisher(
            KafkaTemplate<String, String> kafka,
            Clock clock,
            Duration publishTimeout
    ) {
        if (publishTimeout.isNegative() || publishTimeout.isZero()) {
            throw new IllegalArgumentException("Kafka DLQ publish timeout must be positive");
        }
        DeadLetterPublishingRecoverer publisher = new DeadLetterPublishingRecoverer(
                kafka,
                (record, failure) -> new TopicPartition(
                        FlowForgeTopics.deadLetterTopicFor(record.topic()),
                        record.partition()
                )
        );
        publisher.setFailIfSendResultIsError(true);
        publisher.setWaitForSendResultTimeout(publishTimeout);
        publisher.addHeadersFunction((record, failure) -> {
            Throwable root = rootCause(failure);
            RecordHeaders headers = new RecordHeaders();
            add(headers, DLQ_SCHEMA_VERSION_HEADER, "1");
            add(headers, DLQ_RECORD_ID_HEADER, record.topic() + ":" + record.partition() + ":" + record.offset());
            add(headers, DLQ_FAILURE_CLASS_HEADER, root.getClass().getName());
            add(headers, DLQ_FAILED_AT_HEADER, clock.instant().toString());
            return headers;
        });
        return publisher;
    }

    private static RetryListener retryMetrics(MeterRegistry meters) {
        return new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception failure, int deliveryAttempt) {
                meters.counter(
                        "flowforge.kafka.consumer.delivery.failures",
                        "source_topic", record.topic()
                ).increment();
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                meters.counter(
                        "flowforge.kafka.consumer.recovery.failures",
                        "source_topic", record.topic()
                ).increment();
            }
        };
    }

    private static void recordRecoveryMetrics(MeterRegistry meters, Clock clock, ConsumerRecord<?, ?> record) {
        String destination = FlowForgeTopics.deadLetterTopicFor(record.topic());
        meters.counter(
                "flowforge.kafka.dlq.published",
                "source_topic", record.topic(),
                "dlq_topic", destination
        ).increment();
        meters.counter(
                "flowforge.kafka.consumer.recovered",
                "source_topic", record.topic()
        ).increment();
        if (record.timestamp() >= 0) {
            long age = Math.max(0, Duration.between(
                    Instant.ofEpochMilli(record.timestamp()),
                    clock.instant()
            ).toMillis());
            DistributionSummary.builder("flowforge.kafka.dlq.record.age")
                    .baseUnit("milliseconds")
                    .description("Age of a source record when it is recovered to a transport DLQ")
                    .tag("source_topic", record.topic())
                    .register(meters)
                    .record(age);
        }
    }

    private static void add(RecordHeaders headers, String name, String value) {
        headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) current = current.getCause();
        return current;
    }
}
