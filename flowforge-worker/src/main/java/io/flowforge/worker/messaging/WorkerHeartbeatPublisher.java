package io.flowforge.worker.messaging;

import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.messaging.TaskHeartbeatV1;
import io.flowforge.worker.config.WorkerProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Component
public class WorkerHeartbeatPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerHeartbeatPublisher.class);
    private static final HeartbeatHandle NO_HEARTBEAT = () -> { };

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;
    private final WorkerProperties worker;
    private final Clock clock;
    private final MeterRegistry meters;
    private final ScheduledExecutorService scheduler;
    private final Duration interval;

    public WorkerHeartbeatPublisher(
            KafkaTemplate<String, String> kafka,
            ObjectMapper objectMapper,
            WorkerProperties worker,
            Clock clock,
            MeterRegistry meters,
            @Qualifier("workerHeartbeatExecutor") ScheduledExecutorService scheduler,
            @Value("${flowforge.worker.execution.heartbeat-interval:10s}") Duration interval
    ) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("Worker heartbeat interval must be positive");
        }
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.worker = worker;
        this.clock = clock;
        this.meters = meters;
        this.scheduler = scheduler;
        this.interval = interval;
    }

    public HeartbeatHandle start(TaskCommandV1 command) {
        if (command.fencingToken() == null) return NO_HEARTBEAT;
        publish(command);
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
                () -> publish(command),
                interval.toMillis(),
                interval.toMillis(),
                TimeUnit.MILLISECONDS
        );
        return () -> future.cancel(false);
    }

    private void publish(TaskCommandV1 command) {
        try {
            Instant now = clock.instant();
            MessageEnvelope<TaskHeartbeatV1> envelope = new MessageEnvelope<>(
                    UUID.randomUUID(),
                    TaskHeartbeatV1.EVENT_TYPE,
                    TaskHeartbeatV1.SCHEMA_VERSION,
                    now,
                    command.workflowExecutionId(),
                    new TaskHeartbeatV1(
                            command.workflowExecutionId(),
                            command.taskExecutionId(),
                            command.taskKey(),
                            command.attemptNumber(),
                            command.fencingToken(),
                            worker.id()
                    )
            );
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    FlowForgeTopics.TASK_HEARTBEATS_V1,
                    command.taskExecutionId().toString(),
                    objectMapper.writeValueAsString(envelope)
            );
            addHeader(record, "flowforge-event-id", envelope.eventId().toString());
            addHeader(record, "flowforge-event-type", envelope.eventType());
            addHeader(record, "flowforge-schema-version", Integer.toString(envelope.schemaVersion()));
            addHeader(record, "flowforge-correlation-id", envelope.correlationId().toString());
            kafka.send(record).whenComplete((result, failure) -> {
                if (failure == null) {
                    meters.counter("flowforge.worker.heartbeats.sent").increment();
                } else {
                    meters.counter("flowforge.worker.heartbeats.failures").increment();
                    LOGGER.warn("Kafka rejected heartbeat for task {}", command.taskExecutionId(), failure);
                }
            });
        } catch (RuntimeException failure) {
            meters.counter("flowforge.worker.heartbeats.failures").increment();
            LOGGER.warn("Could not publish heartbeat for task {}", command.taskExecutionId(), failure);
        }
    }

    private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    @FunctionalInterface
    public interface HeartbeatHandle extends AutoCloseable {
        @Override
        void close();
    }
}
