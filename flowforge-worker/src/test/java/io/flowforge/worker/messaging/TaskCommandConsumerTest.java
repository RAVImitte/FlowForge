package io.flowforge.worker.messaging;

import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.observability.LogFields;
import io.flowforge.worker.application.WorkerCommandProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TaskCommandConsumerTest {
    @Test
    void scopesMessageAndKafkaIdentifiersWithoutLeakingTheConsumerThread() throws Exception {
        JsonMapper json = JsonMapper.builder().findAndAddModules().build();
        WorkerCommandProcessor processor = mock(WorkerCommandProcessor.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        UUID workflowId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        UUID fencingToken = UUID.randomUUID();
        MessageEnvelope<TaskCommandV1> envelope = new MessageEnvelope<>(
                eventId,
                TaskCommandV1.EVENT_TYPE,
                TaskCommandV1.SCHEMA_VERSION,
                Instant.now(),
                workflowId,
                new TaskCommandV1(
                        workflowId,
                        taskId,
                        "ROOT",
                        "NOOP",
                        Map.of(),
                        2,
                        3,
                        fencingToken,
                        30_000L
                )
        );
        String payload = json.writeValueAsString(envelope);
        AtomicReference<Map<String, String>> observed = new AtomicReference<>();
        doAnswer(invocation -> {
            observed.set(MDC.getCopyOfContextMap());
            return null;
        }).when(processor).process(any(), any());

        new TaskCommandConsumer(json, processor).consume(
                new ConsumerRecord<>(FlowForgeTopics.TASK_COMMANDS_V1, 4, 19, taskId.toString(), payload),
                acknowledgment
        );

        verify(acknowledgment).acknowledge();
        assertThat(observed.get())
                .containsEntry(LogFields.CORRELATION_ID, workflowId.toString())
                .containsEntry(LogFields.EVENT_ID, eventId.toString())
                .containsEntry(LogFields.WORKFLOW_EXECUTION_ID, workflowId.toString())
                .containsEntry(LogFields.TASK_EXECUTION_ID, taskId.toString())
                .containsEntry(LogFields.TASK_KEY, "ROOT")
                .containsEntry(LogFields.ATTEMPT_NUMBER, "3")
                .containsEntry(LogFields.FENCING_TOKEN, fencingToken.toString())
                .containsEntry(LogFields.KAFKA_TOPIC, FlowForgeTopics.TASK_COMMANDS_V1)
                .containsEntry(LogFields.KAFKA_PARTITION, "4")
                .containsEntry(LogFields.KAFKA_OFFSET, "19");
        assertThat(MDC.getCopyOfContextMap()).isNull();
    }
}
