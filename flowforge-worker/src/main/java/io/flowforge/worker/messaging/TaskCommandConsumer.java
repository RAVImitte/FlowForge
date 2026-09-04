package io.flowforge.worker.messaging;

import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.worker.application.WorkerCommandProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
        MessageEnvelope<TaskCommandV1> envelope = decode(record.value());
        validate(record, envelope);
        processor.process(envelope, record.value());
        acknowledgment.acknowledge();
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
