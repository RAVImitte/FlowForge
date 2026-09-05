package io.flowforge.controlplane.adapter.in.messaging;

import io.flowforge.application.execution.ExecutionConflictException;
import io.flowforge.application.execution.InboundTaskResult;
import io.flowforge.application.execution.TaskOutcome;
import io.flowforge.application.execution.TaskResultIngestion;
import io.flowforge.application.execution.TaskResultIngestionOutcome;
import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskResultOutcomeV1;
import io.flowforge.messaging.TaskResultV1;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskResultConsumerTest {
    private static final Instant NOW = Instant.parse("2026-09-04T12:00:00Z");
    private static final JsonMapper JSON = JsonMapper.builder().findAndAddModules().build();

    @Test
    void validatesMapsPersistsAndAcknowledgesAResult() throws Exception {
        TaskResultIngestion ingestion = mock(TaskResultIngestion.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        MessageEnvelope<TaskResultV1> envelope = resultEnvelope();
        String json = JSON.writeValueAsString(envelope);
        when(ingestion.ingest(any(), eq(json), eq(NOW))).thenReturn(TaskResultIngestionOutcome.APPLIED);

        consumer(ingestion, meters).consume(record(envelope.correlationId().toString(), json), acknowledgment);

        ArgumentCaptor<InboundTaskResult> result = ArgumentCaptor.forClass(InboundTaskResult.class);
        verify(ingestion).ingest(result.capture(), eq(json), eq(NOW));
        verify(acknowledgment).acknowledge();
        assertThat(result.getValue().eventId()).isEqualTo(envelope.eventId());
        assertThat(result.getValue().outcome()).isEqualTo(TaskOutcome.SUCCEEDED);
        assertThat(result.getValue().fencingToken()).isEqualTo(envelope.payload().fencingToken());
        assertThat(meters.counter("flowforge.results.consumed", "outcome", "APPLIED").count()).isEqualTo(1);
    }

    @Test
    void rejectsInvalidRecordKeysWithoutAcknowledging() throws Exception {
        TaskResultIngestion ingestion = mock(TaskResultIngestion.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        MessageEnvelope<TaskResultV1> envelope = resultEnvelope();

        assertThatThrownBy(() -> consumer(ingestion, new SimpleMeterRegistry()).consume(
                record("wrong-workflow", JSON.writeValueAsString(envelope)),
                acknowledgment
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("record key");

        verify(ingestion, never()).ingest(any(), any(), any());
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void leavesOffsetsUnacknowledgedWhenPersistenceFails() throws Exception {
        TaskResultIngestion ingestion = mock(TaskResultIngestion.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        MessageEnvelope<TaskResultV1> envelope = resultEnvelope();
        String json = JSON.writeValueAsString(envelope);
        when(ingestion.ingest(any(), eq(json), eq(NOW)))
                .thenThrow(new ExecutionConflictException("stale result"));

        assertThatThrownBy(() -> consumer(ingestion, new SimpleMeterRegistry()).consume(
                record(envelope.correlationId().toString(), json),
                acknowledgment
        )).isInstanceOf(ExecutionConflictException.class);

        verify(acknowledgment, never()).acknowledge();
    }

    private static TaskResultConsumer consumer(TaskResultIngestion ingestion, SimpleMeterRegistry meters) {
        return new TaskResultConsumer(
                JSON,
                ingestion,
                Clock.fixed(NOW, ZoneOffset.UTC),
                meters
        );
    }

    private static ConsumerRecord<String, String> record(String key, String json) {
        return new ConsumerRecord<>(FlowForgeTopics.TASK_RESULTS_V1, 0, 1, key, json);
    }

    private static MessageEnvelope<TaskResultV1> resultEnvelope() {
        UUID workflowId = UUID.randomUUID();
        return new MessageEnvelope<>(
                UUID.randomUUID(),
                TaskResultV1.EVENT_TYPE,
                TaskResultV1.SCHEMA_VERSION,
                NOW,
                workflowId,
                new TaskResultV1(
                        workflowId,
                        UUID.randomUUID(),
                        "ROOT",
                        1,
                        1,
                        TaskResultOutcomeV1.SUCCEEDED,
                        null,
                        null,
                        UUID.randomUUID(),
                        null
                )
        );
    }
}
