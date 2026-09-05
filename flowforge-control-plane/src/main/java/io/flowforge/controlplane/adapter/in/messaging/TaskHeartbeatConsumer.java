package io.flowforge.controlplane.adapter.in.messaging;

import io.flowforge.application.execution.InboundTaskHeartbeat;
import io.flowforge.application.execution.TaskHeartbeatIngestion;
import io.flowforge.application.execution.TaskHeartbeatIngestionOutcome;
import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskHeartbeatV1;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;

@Component
@ConditionalOnProperty(prefix = "flowforge.kafka", name = "enabled", havingValue = "true")
@ConditionalOnProperty(prefix = "flowforge.leases", name = "heartbeat-consumer-enabled", havingValue = "true")
public class TaskHeartbeatConsumer {
    private static final TypeReference<MessageEnvelope<TaskHeartbeatV1>> HEARTBEAT_TYPE = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;
    private final TaskHeartbeatIngestion ingestion;
    private final Clock clock;
    private final MeterRegistry meters;

    public TaskHeartbeatConsumer(
            ObjectMapper objectMapper,
            TaskHeartbeatIngestion ingestion,
            Clock clock,
            MeterRegistry meters
    ) {
        this.objectMapper = objectMapper;
        this.ingestion = ingestion;
        this.clock = clock;
        this.meters = meters;
    }

    @KafkaListener(
            topics = FlowForgeTopics.TASK_HEARTBEATS_V1,
            groupId = "${flowforge.leases.heartbeat-consumer-group:flowforge-control-plane-heartbeats-v1}"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            MessageEnvelope<TaskHeartbeatV1> envelope = decode(record.value());
            validate(record, envelope);
            TaskHeartbeatV1 payload = envelope.payload();
            TaskHeartbeatIngestionOutcome outcome = ingestion.ingest(
                    new InboundTaskHeartbeat(
                            envelope.eventId(),
                            payload.workflowExecutionId(),
                            payload.taskExecutionId(),
                            payload.taskKey(),
                            payload.attemptNumber(),
                            payload.fencingToken(),
                            payload.workerId()
                    ),
                    record.value(),
                    clock.instant()
            );
            meters.counter("flowforge.leases.heartbeats", "outcome", outcome.name()).increment();
            acknowledgment.acknowledge();
        } catch (RuntimeException failure) {
            meters.counter("flowforge.leases.heartbeat.failures").increment();
            throw failure;
        }
    }

    private MessageEnvelope<TaskHeartbeatV1> decode(String payload) {
        try {
            return objectMapper.readValue(payload, HEARTBEAT_TYPE);
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("Task heartbeat is not valid JSON", exception);
        }
    }

    private static void validate(
            ConsumerRecord<String, String> record,
            MessageEnvelope<TaskHeartbeatV1> envelope
    ) {
        if (!TaskHeartbeatV1.EVENT_TYPE.equals(envelope.eventType())
                || envelope.schemaVersion() != TaskHeartbeatV1.SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported task-heartbeat contract version");
        }
        if (!envelope.correlationId().equals(envelope.payload().workflowExecutionId())) {
            throw new IllegalArgumentException("Heartbeat correlation ID does not match workflow execution");
        }
        if (record.key() == null || !record.key().equals(envelope.payload().taskExecutionId().toString())) {
            throw new IllegalArgumentException("Heartbeat record key does not match task execution");
        }
    }
}
