package io.flowforge.worker.persistence;

import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.messaging.TaskResultV1;
import io.flowforge.worker.application.WorkerTaskResult;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Repository
public class WorkerCommandRepository {
    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public WorkerCommandRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public CommandReceipt receive(MessageEnvelope<TaskCommandV1> envelope, String rawPayload, Instant now) {
        TaskCommandV1 command = envelope.payload();
        int inserted = jdbc.sql("""
                INSERT INTO worker_command_inbox(
                    event_id, workflow_execution_id, task_execution_id, task_key, task_type,
                    expected_state_version, attempt_number, command_payload, status, received_at
                ) VALUES (
                    :eventId, :workflowId, :taskId, :taskKey, :taskType,
                    :stateVersion, :attemptNumber, CAST(:payload AS jsonb), 'RECEIVED', :receivedAt
                )
                ON CONFLICT (event_id) DO NOTHING
                """)
                .param("eventId", envelope.eventId())
                .param("workflowId", command.workflowExecutionId())
                .param("taskId", command.taskExecutionId())
                .param("taskKey", command.taskKey())
                .param("taskType", command.taskType())
                .param("stateVersion", command.expectedStateVersion())
                .param("attemptNumber", command.attemptNumber())
                .param("payload", rawPayload)
                .param("receivedAt", Timestamp.from(now))
                .update();

        StoredCommand stored = find(envelope.eventId(), false);
        validateIdentity(envelope, stored);
        return new CommandReceipt(
                inserted == 1,
                stored.status().equals("COMPLETED"),
                stored.resultEventId()
        );
    }

    @Transactional
    public UUID complete(
            MessageEnvelope<TaskCommandV1> envelope,
            WorkerTaskResult result,
            String workerId,
            Instant now
    ) {
        StoredCommand stored = find(envelope.eventId(), true);
        validateIdentity(envelope, stored);
        if (stored.status().equals("COMPLETED")) return stored.resultEventId();

        UUID resultEventId = deterministicResultId(envelope.eventId());
        TaskCommandV1 command = envelope.payload();
        MessageEnvelope<TaskResultV1> resultEnvelope = new MessageEnvelope<>(
                resultEventId,
                TaskResultV1.EVENT_TYPE,
                TaskResultV1.SCHEMA_VERSION,
                now,
                command.workflowExecutionId(),
                new TaskResultV1(
                        command.workflowExecutionId(),
                        command.taskExecutionId(),
                        command.taskKey(),
                        command.expectedStateVersion(),
                        command.attemptNumber(),
                        result.outcome(),
                        result.errorCode(),
                        result.errorMessage()
                )
        );
        jdbc.sql("""
                INSERT INTO worker_result_outbox(
                    id, command_event_id, workflow_execution_id, task_execution_id,
                    topic, record_key, event_type, schema_version, payload,
                    status, available_at, created_at
                ) VALUES (
                    :id, :commandEventId, :workflowId, :taskId,
                    :topic, :recordKey, :eventType, :schemaVersion, CAST(:payload AS jsonb),
                    'PENDING', :availableAt, :createdAt
                )
                """)
                .param("id", resultEventId)
                .param("commandEventId", envelope.eventId())
                .param("workflowId", command.workflowExecutionId())
                .param("taskId", command.taskExecutionId())
                .param("topic", FlowForgeTopics.TASK_RESULTS_V1)
                .param("recordKey", command.workflowExecutionId().toString())
                .param("eventType", resultEnvelope.eventType())
                .param("schemaVersion", resultEnvelope.schemaVersion())
                .param("payload", toJson(resultEnvelope))
                .param("availableAt", Timestamp.from(now))
                .param("createdAt", Timestamp.from(now))
                .update();
        jdbc.sql("""
                UPDATE worker_command_inbox
                   SET status = 'COMPLETED',
                       completed_at = :completedAt,
                       completed_by = :completedBy,
                       result_event_id = :resultEventId
                 WHERE event_id = :eventId
                   AND status = 'RECEIVED'
                """)
                .param("completedAt", Timestamp.from(now))
                .param("completedBy", workerId)
                .param("resultEventId", resultEventId)
                .param("eventId", envelope.eventId())
                .update();
        return resultEventId;
    }

    private StoredCommand find(UUID eventId, boolean lock) {
        return jdbc.sql("""
                SELECT event_id, workflow_execution_id, task_execution_id, task_key, task_type,
                       expected_state_version, attempt_number, status, result_event_id
                  FROM worker_command_inbox
                 WHERE event_id = :eventId
                """ + (lock ? " FOR UPDATE" : ""))
                .param("eventId", eventId)
                .query((rs, rowNum) -> new StoredCommand(
                        rs.getObject("event_id", UUID.class),
                        rs.getObject("workflow_execution_id", UUID.class),
                        rs.getObject("task_execution_id", UUID.class),
                        rs.getString("task_key"),
                        rs.getString("task_type"),
                        rs.getLong("expected_state_version"),
                        rs.getInt("attempt_number"),
                        rs.getString("status"),
                        rs.getObject("result_event_id", UUID.class)
                ))
                .single();
    }

    private static void validateIdentity(MessageEnvelope<TaskCommandV1> envelope, StoredCommand stored) {
        TaskCommandV1 command = envelope.payload();
        if (!stored.workflowExecutionId().equals(command.workflowExecutionId())
                || !stored.taskExecutionId().equals(command.taskExecutionId())
                || !stored.taskKey().equals(command.taskKey())
                || !stored.taskType().equals(command.taskType())
                || stored.expectedStateVersion() != command.expectedStateVersion()
                || stored.attemptNumber() != command.attemptNumber()) {
            throw new IllegalStateException("Command event ID was reused with a different task identity");
        }
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JacksonException exception) {
            throw new IllegalStateException("Could not serialize task result", exception);
        }
    }

    private static UUID deterministicResultId(UUID commandEventId) {
        return UUID.nameUUIDFromBytes(
                ("flowforge-task-result:" + commandEventId).getBytes(StandardCharsets.UTF_8)
        );
    }

    private record StoredCommand(
            UUID eventId,
            UUID workflowExecutionId,
            UUID taskExecutionId,
            String taskKey,
            String taskType,
            long expectedStateVersion,
            int attemptNumber,
            String status,
            UUID resultEventId
    ) {
    }
}
