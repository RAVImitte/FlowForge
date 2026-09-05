package io.flowforge.controlplane.adapter.out.messaging;

import io.flowforge.controlplane.config.OutboxProperties;
import io.flowforge.observability.LogContext;
import io.flowforge.observability.LogFields;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@ConditionalOnProperty(prefix = "flowforge.outbox", name = "publisher-enabled", havingValue = "true")
public class OutboxPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxMessageRepository repository;
    private final OutboxMessageSender sender;
    private final OutboxProperties properties;
    private final Clock clock;
    private final MeterRegistry meters;
    private final AtomicBoolean publishing = new AtomicBoolean();

    public OutboxPublisher(
            OutboxMessageRepository repository,
            OutboxMessageSender sender,
            OutboxProperties properties,
            Clock clock,
            MeterRegistry meters
    ) {
        this.repository = repository;
        this.sender = sender;
        this.properties = properties;
        this.clock = clock;
        this.meters = meters;
        Gauge.builder("flowforge.outbox.pending", repository, OutboxMessageRepository::pendingCount)
                .description("Control-plane outbox messages awaiting durable publication")
                .register(meters);
    }

    public int publishAvailable() {
        if (!publishing.compareAndSet(false, true)) return 0;
        try {
            var messages = repository.claimBatch(
                    properties.batchSize(),
                    properties.instanceId(),
                    clock.instant(),
                    properties.leaseDuration()
            );
            int published = 0;
            for (OutboxMessage message : messages) {
                if (publish(message)) published++;
            }
            return published;
        } finally {
            publishing.set(false);
        }
    }

    private boolean publish(OutboxMessage message) {
        try (LogContext ignored = LogContext.open(
                LogFields.CORRELATION_ID, message.workflowExecutionId(),
                LogFields.EVENT_ID, message.id(),
                LogFields.WORKFLOW_EXECUTION_ID, message.workflowExecutionId(),
                LogFields.TASK_EXECUTION_ID, message.taskExecutionId(),
                LogFields.KAFKA_TOPIC, message.topic()
        )) {
            return publishWithContext(message);
        }
    }

    private boolean publishWithContext(OutboxMessage message) {
        try {
            sender.send(message, properties.publishTimeout());
        } catch (Exception failure) {
            meters.counter("flowforge.outbox.publish.failures", "topic", message.topic()).increment();
            LOGGER.warn(
                    "Could not publish outbox message {} to {} on attempt {}",
                    message.id(),
                    message.topic(),
                    message.attemptCount(),
                    failure
            );
            if (deliveryOutcomeIsUnknown(failure)) {
                if (contains(failure, InterruptedException.class)) Thread.currentThread().interrupt();
                LOGGER.warn("Leaving outbox message {} leased because its delivery outcome is unknown", message.id());
                return false;
            }
            releaseAfterFailure(message, failure);
            return false;
        }

        try {
            if (!repository.markPublished(message.id(), message.claimToken(), clock.instant())) {
                LOGGER.warn("Outbox acknowledgement was stale for message {}", message.id());
                meters.counter("flowforge.outbox.stale.acknowledgements").increment();
                return false;
            }
            meters.counter("flowforge.outbox.published", "topic", message.topic()).increment();
            return true;
        } catch (RuntimeException failure) {
            // Kafka may already have the record. Keep the lease so recovery deliberately
            // replays the same stable event ID after expiry instead of losing the message.
            meters.counter("flowforge.outbox.acknowledgement.failures", "topic", message.topic()).increment();
            LOGGER.error("Kafka acknowledged outbox message {} but the database update failed", message.id(), failure);
            return false;
        }
    }

    private void releaseAfterFailure(OutboxMessage message, Exception sendFailure) {
        try {
            repository.release(message.id(), message.claimToken(), clock.instant(), rootMessage(sendFailure));
        } catch (RuntimeException releaseFailure) {
            LOGGER.error("Could not release failed outbox message {}; its lease will expire", message.id(), releaseFailure);
        }
    }

    private static String rootMessage(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) current = current.getCause();
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    private static boolean deliveryOutcomeIsUnknown(Throwable failure) {
        return contains(failure, InterruptedException.class) || contains(failure, TimeoutException.class);
    }

    private static boolean contains(Throwable failure, Class<? extends Throwable> type) {
        Throwable current = failure;
        while (current != null) {
            if (type.isInstance(current)) return true;
            current = current.getCause();
        }
        return false;
    }
}
