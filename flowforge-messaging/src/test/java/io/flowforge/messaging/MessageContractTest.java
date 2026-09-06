package io.flowforge.messaging;

import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessageContractTest {
    private static final JsonMapper JSON = JsonMapper.builder().findAndAddModules().build();

    @Test
    void taskCommandRoundTripsThroughItsVersionedEnvelope() throws Exception {
        UUID workflowId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID taskId = UUID.fromString("00000000-0000-0000-0000-000000000002");
        MessageEnvelope<TaskCommandV1> envelope = new MessageEnvelope<>(
                UUID.fromString("00000000-0000-0000-0000-000000000003"),
                TaskCommandV1.EVENT_TYPE,
                TaskCommandV1.SCHEMA_VERSION,
                Instant.parse("2026-09-04T12:00:00Z"),
                workflowId,
                "merchant-a",
                new TaskCommandV1(
                        workflowId,
                        taskId,
                        "PROCESS_PAYMENT",
                        "HTTP",
                        Map.of("uri", "/payments"),
                        4,
                        2
                )
        );

        String encoded = JSON.writeValueAsString(envelope);
        MessageEnvelope<TaskCommandV1> decoded = JSON.readValue(
                encoded,
                new TypeReference<MessageEnvelope<TaskCommandV1>>() {
                }
        );

        assertThat(decoded).isEqualTo(envelope);
        assertThat(encoded).contains(
                "\"schemaVersion\":1", "\"tenantId\":\"merchant-a\"", "\"expectedStateVersion\":4"
        );
    }

    @Test
    void taskHeartbeatRoundTripsWithItsFencingIdentity() throws Exception {
        UUID workflowId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        UUID token = UUID.randomUUID();
        MessageEnvelope<TaskHeartbeatV1> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                TaskHeartbeatV1.EVENT_TYPE,
                TaskHeartbeatV1.SCHEMA_VERSION,
                Instant.parse("2026-09-05T08:00:00Z"),
                workflowId,
                new TaskHeartbeatV1(workflowId, taskId, "ROOT", 2, token, "worker-a")
        );

        String encoded = JSON.writeValueAsString(envelope);
        MessageEnvelope<TaskHeartbeatV1> decoded = JSON.readValue(
                encoded, new TypeReference<MessageEnvelope<TaskHeartbeatV1>>() { }
        );

        assertThat(decoded).isEqualTo(envelope);
        assertThat(decoded.payload().fencingToken()).isEqualTo(token);
        assertThat(encoded).contains("flowforge.task.heartbeat", "worker-a");
    }

    @Test
    void readsTheOriginalVersionOneFixtureWithoutInventingDefaults() throws Exception {
        String fixture = """
                {
                  "eventId":"00000000-0000-0000-0000-000000000003",
                  "eventType":"flowforge.task.command",
                  "schemaVersion":1,
                  "occurredAt":"2026-09-04T12:00:00Z",
                  "correlationId":"00000000-0000-0000-0000-000000000001",
                  "payload":{
                    "workflowExecutionId":"00000000-0000-0000-0000-000000000001",
                    "taskExecutionId":"00000000-0000-0000-0000-000000000002",
                    "taskKey":"VALIDATE_ORDER",
                    "taskType":"NOOP",
                    "configuration":{},
                    "expectedStateVersion":1,
                    "attemptNumber":1
                  }
                }
                """;

        MessageEnvelope<TaskCommandV1> decoded = JSON.readValue(
                fixture,
                new TypeReference<MessageEnvelope<TaskCommandV1>>() {
                }
        );

        assertThat(decoded.schemaVersion()).isEqualTo(1);
        assertThat(decoded.payload().taskKey()).isEqualTo("VALIDATE_ORDER");
        assertThat(decoded.payload().configuration()).isEmpty();
        assertThat(decoded.payload().fencingToken()).isNull();
        assertThat(decoded.payload().attemptTimeoutMs()).isNull();
        assertThat(decoded.tenantId()).isEqualTo("local");
    }

    @Test
    void additiveReliabilityFieldsRoundTripWithoutChangingVersionOneCompatibility() throws Exception {
        UUID fencingToken = UUID.fromString("00000000-0000-0000-0000-000000000004");
        TaskCommandV1 command = new TaskCommandV1(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "PROCESS_PAYMENT",
                "HTTP",
                Map.of(),
                3,
                2,
                fencingToken,
                30_000L
        );

        String encoded = JSON.writeValueAsString(command);
        TaskCommandV1 decoded = JSON.readValue(encoded, TaskCommandV1.class);

        assertThat(decoded).isEqualTo(command);
        assertThat(decoded.fencingToken()).isEqualTo(fencingToken);
        assertThat(decoded.attemptTimeoutMs()).isEqualTo(30_000L);
        assertThat(TaskCommandV1.SCHEMA_VERSION).isEqualTo(1);
    }

    @Test
    void contractsRejectInvalidIdentityVersionAndMutableConfiguration() {
        Map<String, Object> configuration = new LinkedHashMap<>();
        configuration.put("durationMs", 100);
        TaskCommandV1 command = new TaskCommandV1(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "DELAY",
                "DELAY",
                configuration,
                0,
                1
        );
        configuration.put("durationMs", 200);

        assertThat(command.configuration()).containsEntry("durationMs", 100);
        assertThatThrownBy(() -> new MessageEnvelope<>(
                UUID.randomUUID(), " ", 0, Instant.now(), UUID.randomUUID(), command
        )).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TaskCommandV1(
                UUID.randomUUID(), UUID.randomUUID(), "task", "type", Map.of(), -1, 0
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void mapsEachConsumedContractToItsVersionedDeadLetterTopic() {
        assertThat(FlowForgeTopics.deadLetterTopicFor(FlowForgeTopics.TASK_COMMANDS_V1))
                .isEqualTo(FlowForgeTopics.TASK_COMMANDS_DLQ_V1);
        assertThat(FlowForgeTopics.deadLetterTopicFor(FlowForgeTopics.TASK_RESULTS_V1))
                .isEqualTo(FlowForgeTopics.TASK_RESULTS_DLQ_V1);
        assertThat(FlowForgeTopics.deadLetterTopicFor(FlowForgeTopics.TASK_HEARTBEATS_V1))
                .isEqualTo(FlowForgeTopics.TASK_HEARTBEATS_DLQ_V1);
        assertThat(FlowForgeTopics.sourceTopicForDeadLetter(FlowForgeTopics.TASK_COMMANDS_DLQ_V1))
                .isEqualTo(FlowForgeTopics.TASK_COMMANDS_V1);
        assertThat(FlowForgeTopics.sourceTopicForDeadLetter(FlowForgeTopics.TASK_RESULTS_DLQ_V1))
                .isEqualTo(FlowForgeTopics.TASK_RESULTS_V1);
        assertThat(FlowForgeTopics.sourceTopicForDeadLetter(FlowForgeTopics.TASK_HEARTBEATS_DLQ_V1))
                .isEqualTo(FlowForgeTopics.TASK_HEARTBEATS_V1);
        assertThatThrownBy(() -> FlowForgeTopics.deadLetterTopicFor("flowforge.unknown.v1"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FlowForgeTopics.sourceTopicForDeadLetter("flowforge.unknown.dlq.v1"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
