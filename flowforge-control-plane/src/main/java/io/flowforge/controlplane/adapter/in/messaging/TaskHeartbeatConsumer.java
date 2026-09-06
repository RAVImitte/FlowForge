package io.flowforge.controlplane.adapter.in.messaging;

import io.flowforge.application.execution.InboundTaskHeartbeat;
import io.flowforge.application.execution.TaskHeartbeatIngestion;
import io.flowforge.application.execution.TaskHeartbeatIngestionOutcome;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.kafka.KafkaTenantHeader;
import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskHeartbeatV1;
import io.flowforge.observability.LogContext;
import io.flowforge.observability.LogFields;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskHeartbeatConsumer.class);
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
        try (LogContext recordContext = recordContext(record)) {
            MessageEnvelope<TaskHeartbeatV1> envelope;
            try {
                envelope = decode(record.value());
                validate(record, envelope);
            } catch (RuntimeException failure) {
                recordFailure(failure);
                throw failure;
            }
            TaskHeartbeatV1 payload = envelope.payload();
            try (LogContext messageContext = LogContext.open(
                    LogFields.TENANT_ID, envelope.tenantId(),
                    LogFields.CORRELATION_ID, envelope.correlationId(),
                    LogFields.EVENT_ID, envelope.eventId(),
                    LogFields.WORKFLOW_EXECUTION_ID, payload.workflowExecutionId(),
                    LogFields.TASK_EXECUTION_ID, payload.taskExecutionId(),
                    LogFields.TASK_KEY, payload.taskKey(),
                    LogFields.ATTEMPT_NUMBER, payload.attemptNumber(),
                    LogFields.FENCING_TOKEN, payload.fencingToken(),
                    LogFields.WORKER_ID, payload.workerId()
            )) {
                try {
                    TaskHeartbeatIngestionOutcome outcome = ingestion.ingest(
                            new InboundTaskHeartbeat(
                                    new TenantId(envelope.tenantId()),
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
                    recordFailure(failure);
                    throw failure;
                }
            }
        }
    }

    private LogContext recordContext(ConsumerRecord<String, String> record) {
        return LogContext.open(
                LogFields.KAFKA_TOPIC, record.topic(),
                LogFields.KAFKA_PARTITION, record.partition(),
                LogFields.KAFKA_OFFSET, record.offset()
        );
    }

    private void recordFailure(RuntimeException failure) {
        meters.counter("flowforge.leases.heartbeat.failures").increment();
        LOGGER.warn("Task heartbeat consumption failed", failure);
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
        KafkaTenantHeader.requireMatching(record.headers(), envelope.tenantId());
        if (!envelope.correlationId().equals(envelope.payload().workflowExecutionId())) {
            throw new IllegalArgumentException("Heartbeat correlation ID does not match workflow execution");
        }
        if (record.key() == null || !record.key().equals(envelope.payload().taskExecutionId().toString())) {
            throw new IllegalArgumentException("Heartbeat record key does not match task execution");
        }
    }
}
