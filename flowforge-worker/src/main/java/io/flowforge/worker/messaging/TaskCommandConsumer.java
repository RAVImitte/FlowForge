package io.flowforge.worker.messaging;

import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.observability.LogContext;
import io.flowforge.observability.LogFields;
import io.flowforge.worker.application.WorkerCommandProcessor;
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

@Component
@ConditionalOnProperty(prefix = "flowforge.kafka", name = "enabled", havingValue = "true")
public class TaskCommandConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskCommandConsumer.class);
    private static final TypeReference<MessageEnvelope<TaskCommandV1>> COMMAND_TYPE = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;
    private final WorkerCommandProcessor processor;

    public TaskCommandConsumer(ObjectMapper objectMapper, WorkerCommandProcessor processor) {
        this.objectMapper = objectMapper;
        this.processor = processor;
    }

    @KafkaListener(
            topics = FlowForgeTopics.TASK_COMMANDS_V1,
            groupId = "${flowforge.worker.consumer-group:flowforge-workers-v1}",
            concurrency = "${flowforge.worker.execution.concurrency:1}"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try (LogContext recordContext = LogContext.open(
                LogFields.KAFKA_TOPIC, record.topic(),
                LogFields.KAFKA_PARTITION, record.partition(),
                LogFields.KAFKA_OFFSET, record.offset()
        )) {
            MessageEnvelope<TaskCommandV1> envelope;
            try {
                envelope = decode(record.value());
                validate(record, envelope);
            } catch (RuntimeException failure) {
                LOGGER.warn("Task command validation failed", failure);
                throw failure;
            }
            TaskCommandV1 command = envelope.payload();
            try (LogContext messageContext = LogContext.open(
                    LogFields.CORRELATION_ID, envelope.correlationId(),
                    LogFields.EVENT_ID, envelope.eventId(),
                    LogFields.WORKFLOW_EXECUTION_ID, command.workflowExecutionId(),
                    LogFields.TASK_EXECUTION_ID, command.taskExecutionId(),
                    LogFields.TASK_KEY, command.taskKey(),
                    LogFields.ATTEMPT_NUMBER, command.attemptNumber(),
                    LogFields.FENCING_TOKEN, command.fencingToken()
            )) {
                try {
                    processor.process(envelope, record.value());
                    acknowledgment.acknowledge();
                    LOGGER.debug("Task command completed and acknowledged");
                } catch (RuntimeException failure) {
                    LOGGER.warn("Task command handling failed", failure);
                    throw failure;
                }
            }
        }
    }

    private MessageEnvelope<TaskCommandV1> decode(String payload) {
        try {
            return objectMapper.readValue(payload, COMMAND_TYPE);
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("Task command is not valid JSON", exception);
        }
    }

    private static void validate(
            ConsumerRecord<String, String> record,
            MessageEnvelope<TaskCommandV1> envelope
    ) {
        if (!TaskCommandV1.EVENT_TYPE.equals(envelope.eventType())
                || envelope.schemaVersion() != TaskCommandV1.SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported task command contract version");
        }
        if (!envelope.correlationId().equals(envelope.payload().workflowExecutionId())) {
            throw new IllegalArgumentException("Task command correlation ID does not match workflow execution");
        }
        if (record.key() == null || !record.key().equals(envelope.payload().taskExecutionId().toString())) {
            throw new IllegalArgumentException("Task command record key does not match task execution");
        }
    }
}
