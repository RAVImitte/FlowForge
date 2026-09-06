package io.flowforge.controlplane.adapter.in.messaging;

import io.flowforge.application.execution.InboundTaskResult;
import io.flowforge.application.execution.TaskOutcome;
import io.flowforge.application.execution.TaskResultIngestion;
import io.flowforge.application.execution.TaskResultIngestionOutcome;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.kafka.KafkaTenantHeader;
import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskResultV1;
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
@ConditionalOnProperty(prefix = "flowforge.results", name = "consumer-enabled", havingValue = "true")
public class TaskResultConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskResultConsumer.class);
    private static final TypeReference<MessageEnvelope<TaskResultV1>> RESULT_TYPE = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;
    private final TaskResultIngestion ingestion;
    private final Clock clock;
    private final MeterRegistry meters;

    public TaskResultConsumer(
            ObjectMapper objectMapper,
            TaskResultIngestion ingestion,
            Clock clock,
            MeterRegistry meters
    ) {
        this.objectMapper = objectMapper;
        this.ingestion = ingestion;
        this.clock = clock;
        this.meters = meters;
    }

    @KafkaListener(
            topics = FlowForgeTopics.TASK_RESULTS_V1,
            groupId = "${flowforge.results.consumer-group:flowforge-control-plane-results-v1}"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try (LogContext recordContext = recordContext(record)) {
            MessageEnvelope<TaskResultV1> envelope;
            try {
                envelope = decode(record.value());
                validate(record, envelope);
            } catch (RuntimeException failure) {
                recordFailure(failure);
                throw failure;
            }
            TaskResultV1 payload = envelope.payload();
            try (LogContext messageContext = LogContext.open(
                    LogFields.TENANT_ID, envelope.tenantId(),
                    LogFields.CORRELATION_ID, envelope.correlationId(),
                    LogFields.EVENT_ID, envelope.eventId(),
                    LogFields.WORKFLOW_EXECUTION_ID, payload.workflowExecutionId(),
                    LogFields.TASK_EXECUTION_ID, payload.taskExecutionId(),
                    LogFields.TASK_KEY, payload.taskKey(),
                    LogFields.ATTEMPT_NUMBER, payload.attemptNumber(),
                    LogFields.FENCING_TOKEN, payload.fencingToken()
            )) {
                try {
                    TaskResultIngestionOutcome outcome = ingestion.ingest(
                            new InboundTaskResult(
                                    new TenantId(envelope.tenantId()),
                                    envelope.eventId(),
                                    payload.workflowExecutionId(),
                                    payload.taskExecutionId(),
                                    payload.taskKey(),
                                    payload.expectedStateVersion(),
                                    payload.attemptNumber(),
                                    TaskOutcome.valueOf(payload.outcome().name()),
                                    payload.errorCode(),
                                    payload.errorMessage(),
                                    payload.retryable(),
                                    payload.fencingToken()
                            ),
                            record.value(),
                            clock.instant()
                    );
                    meters.counter("flowforge.results.consumed", "outcome", outcome.name()).increment();
                    acknowledgment.acknowledge();
                    LOGGER.debug("Task result applied and acknowledged: outcome={}", outcome);
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
        meters.counter("flowforge.results.failures").increment();
        LOGGER.warn("Task result consumption failed", failure);
    }

    private MessageEnvelope<TaskResultV1> decode(String payload) {
        try {
            return objectMapper.readValue(payload, RESULT_TYPE);
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("Task result is not valid JSON", exception);
        }
    }

    private static void validate(
            ConsumerRecord<String, String> record,
            MessageEnvelope<TaskResultV1> envelope
    ) {
        if (!TaskResultV1.EVENT_TYPE.equals(envelope.eventType())
                || envelope.schemaVersion() != TaskResultV1.SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported task-result contract version");
        }
        KafkaTenantHeader.requireMatching(record.headers(), envelope.tenantId());
        if (!envelope.correlationId().equals(envelope.payload().workflowExecutionId())) {
            throw new IllegalArgumentException("Task-result correlation ID does not match workflow execution");
        }
        if (record.key() == null || !record.key().equals(envelope.payload().workflowExecutionId().toString())) {
            throw new IllegalArgumentException("Task-result record key does not match workflow execution");
        }
    }
}
