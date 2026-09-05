package io.flowforge.controlplane.adapter.in.messaging;

import io.flowforge.application.execution.InboundTaskHeartbeat;
import io.flowforge.application.execution.TaskHeartbeatIngestion;
import io.flowforge.application.execution.TaskHeartbeatIngestionOutcome;
import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskHeartbeatV1;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskHeartbeatConsumerTest {
    private static final Instant NOW = Instant.parse("2026-09-05T08:00:00Z");

    @Test
    void validatesMapsPersistsAndAcknowledgesAHeartbeat() throws Exception {
        var jsonMapper = JsonMapper.builder().findAndAddModules().build();
        TaskHeartbeatIngestion ingestion = mock(TaskHeartbeatIngestion.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        UUID workflowId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        UUID token = UUID.randomUUID();
        MessageEnvelope<TaskHeartbeatV1> envelope = new MessageEnvelope<>(
                UUID.randomUUID(), TaskHeartbeatV1.EVENT_TYPE, TaskHeartbeatV1.SCHEMA_VERSION,
                NOW, workflowId,
                new TaskHeartbeatV1(workflowId, taskId, "ROOT", 2, token, "worker-a")
        );
        String json = jsonMapper.writeValueAsString(envelope);
        when(ingestion.ingest(any(), eq(json), eq(NOW))).thenReturn(TaskHeartbeatIngestionOutcome.APPLIED);
        TaskHeartbeatConsumer consumer = new TaskHeartbeatConsumer(
                jsonMapper, ingestion, Clock.fixed(NOW, ZoneOffset.UTC), meters
        );

        consumer.consume(new ConsumerRecord<>(
                FlowForgeTopics.TASK_HEARTBEATS_V1, 0, 1, taskId.toString(), json
        ), acknowledgment);

        ArgumentCaptor<InboundTaskHeartbeat> heartbeat = ArgumentCaptor.forClass(InboundTaskHeartbeat.class);
        verify(ingestion).ingest(heartbeat.capture(), eq(json), eq(NOW));
        verify(acknowledgment).acknowledge();
        assertThat(heartbeat.getValue().fencingToken()).isEqualTo(token);
        assertThat(meters.counter("flowforge.leases.heartbeats", "outcome", "APPLIED").count())
                .isEqualTo(1.0);
    }
}
