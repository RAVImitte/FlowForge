package io.flowforge.worker.persistence;

import io.flowforge.messaging.FlowForgeTopics;
import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.messaging.TaskResultV1;
import io.flowforge.observability.TraceContextPropagation;
import io.flowforge.observability.TraceContextSnapshot;
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
                    tenant_id, event_id, workflow_execution_id, task_execution_id, task_key, task_type,
                    expected_state_version, attempt_number, command_payload, status, received_at
                ) VALUES (
                    :tenantId, :eventId, :workflowId, :taskId, :taskKey, :taskType,
                    :stateVersion, :attemptNumber, CAST(:payload AS jsonb), 'RECEIVED', :receivedAt
                )
                ON CONFLICT (tenant_id, event_id) DO NOTHING
                """)
                .param("tenantId", envelope.tenantId())
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

        StoredCommand stored = find(envelope.tenantId(), envelope.eventId(), false);
        if (inserted == 0) {
            boolean samePayload = jdbc.sql("""
                    SELECT command_payload = CAST(:payload AS jsonb)
                     FROM worker_command_inbox
                     WHERE tenant_id = :tenantId AND event_id = :eventId
                    """)
                    .param("payload", rawPayload)
                    .param("tenantId", envelope.tenantId())
                    .param("eventId", envelope.eventId())
                    .query(Boolean.class)
                    .single();
            if (!samePayload) {
                throw new IllegalStateException("Command event ID was reused with a different payload");
            }
        }
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
        StoredCommand stored = find(envelope.tenantId(), envelope.eventId(), true);
        validateIdentity(envelope, stored);
        if (stored.status().equals("COMPLETED")) return stored.resultEventId();

        UUID resultEventId = deterministicResultId(envelope.tenantId(), envelope.eventId());
        TaskCommandV1 command = envelope.payload();
        MessageEnvelope<TaskResultV1> resultEnvelope = new MessageEnvelope<>(
                resultEventId,
                TaskResultV1.EVENT_TYPE,
                TaskResultV1.SCHEMA_VERSION,
                now,
                command.workflowExecutionId(),
                envelope.tenantId(),
                new TaskResultV1(
                        command.workflowExecutionId(),
                        command.taskExecutionId(),
                        command.taskKey(),
                        command.expectedStateVersion(),
                        command.attemptNumber(),
                        result.outcome(),
                        result.errorCode(),
                        result.errorMessage(),
                        command.fencingToken(),
                        result.retryable()
                )
        );
        TraceContextSnapshot traceContext = TraceContextPropagation.capture();
        jdbc.sql("""
                INSERT INTO worker_result_outbox(
                    tenant_id, id, command_event_id, workflow_execution_id, task_execution_id,
                    topic, record_key, event_type, schema_version, payload,
                    status, available_at, created_at,
                    trace_parent, trace_state, trace_baggage
                ) VALUES (
                    :tenantId, :id, :commandEventId, :workflowId, :taskId,
                    :topic, :recordKey, :eventType, :schemaVersion, CAST(:payload AS jsonb),
                    'PENDING', :availableAt, :createdAt,
                    :traceParent, :traceState, :traceBaggage
                )
                """)
                .param("tenantId", envelope.tenantId())
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
                .param("traceParent", traceContext.traceParent())
                .param("traceState", traceContext.traceState())
                .param("traceBaggage", traceContext.baggage())
                .update();
        jdbc.sql("""
                UPDATE worker_command_inbox
                   SET status = 'COMPLETED',
                       completed_at = :completedAt,
                       completed_by = :completedBy,
                       result_event_id = :resultEventId
                 WHERE event_id = :eventId
                   AND tenant_id = :tenantId
                   AND status = 'RECEIVED'
                """)
                .param("completedAt", Timestamp.from(now))
                .param("completedBy", workerId)
                .param("resultEventId", resultEventId)
                .param("tenantId", envelope.tenantId())
                .param("eventId", envelope.eventId())
                .update();
        return resultEventId;
    }

    private StoredCommand find(String tenantId, UUID eventId, boolean lock) {
        return jdbc.sql("""
                SELECT tenant_id, event_id, workflow_execution_id, task_execution_id, task_key, task_type,
                       expected_state_version, attempt_number, status, result_event_id
                  FROM worker_command_inbox
                 WHERE tenant_id = :tenantId AND event_id = :eventId
                """ + (lock ? " FOR UPDATE" : ""))
                .param("tenantId", tenantId)
                .param("eventId", eventId)
                .query((rs, rowNum) -> new StoredCommand(
                        rs.getString("tenant_id"),
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
        if (!stored.tenantId().equals(envelope.tenantId())
                || !stored.workflowExecutionId().equals(command.workflowExecutionId())
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

    private static UUID deterministicResultId(String tenantId, UUID commandEventId) {
        return UUID.nameUUIDFromBytes(
                ("flowforge-task-result:" + tenantId + ":" + commandEventId)
                        .getBytes(StandardCharsets.UTF_8)
        );
    }

    private record StoredCommand(
            String tenantId,
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
