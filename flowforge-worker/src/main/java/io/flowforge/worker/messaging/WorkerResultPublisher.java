package io.flowforge.worker.messaging;

import io.flowforge.observability.LogContext;
import io.flowforge.observability.LogFields;
import io.flowforge.worker.config.WorkerExecutionProperties;
import io.flowforge.worker.config.WorkerProperties;
import io.flowforge.worker.persistence.WorkerResultMessage;
import io.flowforge.worker.persistence.WorkerResultOutboxRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(prefix = "flowforge.kafka", name = "enabled", havingValue = "true")
@ConditionalOnProperty(
        prefix = "flowforge.worker",
        name = "result-publisher-enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class WorkerResultPublisher implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerResultPublisher.class);

    private final WorkerResultOutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final WorkerProperties worker;
    private final WorkerExecutionProperties properties;
    private final Clock clock;
    private final MeterRegistry meters;
    private final AtomicBoolean publishing = new AtomicBoolean();

    public WorkerResultPublisher(
            WorkerResultOutboxRepository repository,
            KafkaTemplate<String, String> kafkaTemplate,
            WorkerProperties worker,
            WorkerExecutionProperties properties,
            Clock clock,
            MeterRegistry meters
    ) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.worker = worker;
        this.properties = properties;
        this.clock = clock;
        this.meters = meters;
        Gauge.builder("flowforge.worker.results.pending", repository, WorkerResultOutboxRepository::pendingCount)
                .description("Worker results awaiting Kafka publication")
                .register(meters);
    }

    @Override
    public void run(ApplicationArguments args) {
        publishAvailable();
    }

    public int publishAvailable() {
        if (!publishing.compareAndSet(false, true)) return 0;
        try {
            var messages = repository.claimBatch(
                    properties.resultBatchSize(),
                    worker.id(),
                    clock.instant(),
                    properties.resultLeaseDuration()
            );
            int published = 0;
            for (WorkerResultMessage message : messages) {
                if (publish(message)) published++;
            }
            return published;
        } finally {
            publishing.set(false);
        }
    }

    @Scheduled(fixedDelayString = "${flowforge.worker.execution.result-poll-interval-ms:250}")
    void scheduledPublish() {
        publishAvailable();
    }

    private boolean publish(WorkerResultMessage message) {
        try (LogContext ignored = LogContext.open(
                LogFields.CORRELATION_ID, message.workflowExecutionId(),
                LogFields.EVENT_ID, message.id(),
                LogFields.WORKFLOW_EXECUTION_ID, message.workflowExecutionId(),
                LogFields.WORKER_ID, worker.id(),
                LogFields.KAFKA_TOPIC, message.topic()
        )) {
            return publishWithContext(message);
        }
    }

    private boolean publishWithContext(WorkerResultMessage message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    message.topic(), message.recordKey(), message.payload()
            );
            addHeader(record, "flowforge-event-id", message.id().toString());
            addHeader(record, "flowforge-event-type", message.eventType());
            addHeader(record, "flowforge-schema-version", Integer.toString(message.schemaVersion()));
            addHeader(record, "flowforge-correlation-id", message.workflowExecutionId().toString());
            kafkaTemplate.send(record).get(
                    properties.resultPublishTimeout().toMillis(),
                    TimeUnit.MILLISECONDS
            );
        } catch (TimeoutException failure) {
            recordUnknownOutcome(message, failure);
            return false;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            recordUnknownOutcome(message, failure);
            return false;
        } catch (ExecutionException failure) {
            releaseAfterKnownFailure(message, failure);
            return false;
        } catch (RuntimeException failure) {
            releaseAfterKnownFailure(message, failure);
            return false;
        }

        try {
            if (!repository.markPublished(message.id(), message.claimToken(), clock.instant())) {
                meters.counter("flowforge.worker.results.stale.acknowledgements").increment();
                return false;
            }
            meters.counter("flowforge.worker.results.published").increment();
            return true;
        } catch (RuntimeException failure) {
            meters.counter("flowforge.worker.results.acknowledgement.failures").increment();
            LOGGER.error(
                    "Kafka acknowledged worker result {}, but its published state could not be persisted; "
                            + "the lease will expire and the stable event ID will be replayed",
                    message.id(),
                    failure
            );
            return false;
        }
    }

    private void releaseAfterKnownFailure(WorkerResultMessage message, Exception failure) {
        try {
            repository.release(message.id(), message.claimToken(), clock.instant(), rootMessage(failure));
        } catch (RuntimeException releaseFailure) {
            LOGGER.error("Could not release worker result {}; its lease will expire", message.id(), releaseFailure);
        }
        meters.counter("flowforge.worker.results.publish.failures").increment();
        LOGGER.warn("Could not publish worker result {} on attempt {}", message.id(), message.attemptCount(), failure);
    }

    private void recordUnknownOutcome(WorkerResultMessage message, Exception failure) {
        meters.counter("flowforge.worker.results.unknown.outcomes").increment();
        LOGGER.warn(
                "Kafka publication outcome is unknown for worker result {}; its lease will expire before replay",
                message.id(),
                failure
        );
    }

    private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String rootMessage(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) current = current.getCause();
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }
}
