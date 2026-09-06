package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.execution.ExecutionConflictException;
import io.flowforge.application.execution.InboundTaskHeartbeat;
import io.flowforge.application.execution.TaskHeartbeatIngestion;
import io.flowforge.application.execution.TaskHeartbeatIngestionOutcome;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Repository
public class JdbcTaskHeartbeatIngestion implements TaskHeartbeatIngestion {
    static final String CONSUMER_NAME = "flowforge-control-plane-heartbeats-v1";

    private final JdbcClient jdbc;
    private final Duration leaseDuration;

    public JdbcTaskHeartbeatIngestion(
            JdbcClient jdbc,
            @Value("${flowforge.leases.duration:30s}") Duration leaseDuration
    ) {
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("Worker lease duration must be positive");
        }
        this.jdbc = jdbc;
        this.leaseDuration = leaseDuration;
    }

    @Override
    @Transactional
    public TaskHeartbeatIngestionOutcome ingest(
            InboundTaskHeartbeat heartbeat,
            String serializedEnvelope,
            Instant receivedAt
    ) {
        if (serializedEnvelope == null || serializedEnvelope.isBlank()) {
            throw new IllegalArgumentException("Serialized heartbeat envelope must not be blank");
        }
        validateAndLockTask(heartbeat);
        int inserted = jdbc.sql("""
                INSERT INTO control_plane_heartbeat_inbox(
                    tenant_id, consumer_name, event_id, workflow_execution_id, task_execution_id,
                    task_key, attempt_number, fencing_token, worker_id,
                    payload, disposition, received_at
                ) VALUES (
                    :tenantId, :consumerName, :eventId, :workflowId, :taskId,
                    :taskKey, :attemptNumber, :fencingToken, :workerId,
                    CAST(:payload AS jsonb), 'PROCESSING', :receivedAt
                )
                ON CONFLICT (tenant_id, consumer_name, event_id) DO NOTHING
                """)
                .param("tenantId", heartbeat.tenantId().value())
                .param("consumerName", CONSUMER_NAME)
                .param("eventId", heartbeat.eventId())
                .param("workflowId", heartbeat.workflowExecutionId())
                .param("taskId", heartbeat.taskExecutionId())
                .param("taskKey", heartbeat.taskKey())
                .param("attemptNumber", heartbeat.attemptNumber())
                .param("fencingToken", heartbeat.fencingToken())
                .param("workerId", heartbeat.workerId())
                .param("payload", serializedEnvelope)
                .param("receivedAt", Timestamp.from(receivedAt))
                .update();
        if (inserted == 0) {
            boolean samePayload = jdbc.sql("""
                    SELECT payload = CAST(:payload AS jsonb)
                     FROM control_plane_heartbeat_inbox
                     WHERE tenant_id = :tenantId
                       AND consumer_name = :consumerName AND event_id = :eventId
                    """)
                    .param("tenantId", heartbeat.tenantId().value())
                    .param("payload", serializedEnvelope)
                    .param("consumerName", CONSUMER_NAME)
                    .param("eventId", heartbeat.eventId())
                    .query(Boolean.class)
                    .single();
            if (!samePayload) {
                throw new ExecutionConflictException(
                        "Heartbeat event ID " + heartbeat.eventId() + " was reused with a different payload"
                );
            }
            return TaskHeartbeatIngestionOutcome.DUPLICATE;
        }

        int renewed = jdbc.sql("""
                UPDATE task_attempt ta
                   SET lease_deadline = GREATEST(ta.lease_deadline, :leaseDeadline)
                  FROM task_execution te, workflow_execution we
                 WHERE ta.task_execution_id = te.id
                   AND te.workflow_execution_id = we.id
                   AND we.tenant_id = :tenantId
                   AND ta.task_execution_id = :taskId
                   AND ta.attempt_number = :attemptNumber
                   AND ta.fencing_token = :fencingToken
                   AND ta.status = 'RUNNING'
                   AND te.status = 'RUNNING'
                   AND we.status = 'RUNNING'
                """)
                .param("leaseDeadline", Timestamp.from(receivedAt.plus(leaseDuration)))
                .param("tenantId", heartbeat.tenantId().value())
                .param("taskId", heartbeat.taskExecutionId())
                .param("attemptNumber", heartbeat.attemptNumber())
                .param("fencingToken", heartbeat.fencingToken())
                .update();
        TaskHeartbeatIngestionOutcome outcome = renewed == 1
                ? TaskHeartbeatIngestionOutcome.APPLIED
                : TaskHeartbeatIngestionOutcome.STALE;
        jdbc.sql("""
                UPDATE control_plane_heartbeat_inbox
                   SET disposition = :disposition, processed_at = :processedAt
                 WHERE consumer_name = :consumerName AND event_id = :eventId
                   AND tenant_id = :tenantId
                   AND disposition = 'PROCESSING'
                """)
                .param("disposition", outcome.name())
                .param("processedAt", Timestamp.from(receivedAt))
                .param("consumerName", CONSUMER_NAME)
                .param("tenantId", heartbeat.tenantId().value())
                .param("eventId", heartbeat.eventId())
                .update();
        return outcome;
    }

    private void validateAndLockTask(InboundTaskHeartbeat heartbeat) {
        StoredTask task = jdbc.sql("""
                SELECT te.workflow_execution_id, te.task_key
                  FROM task_execution te
                 JOIN workflow_execution we ON we.id = te.workflow_execution_id
                 WHERE te.id = :taskId
                   AND we.tenant_id = :tenantId
                 FOR UPDATE OF we, te
                """)
                .param("tenantId", heartbeat.tenantId().value())
                .param("taskId", heartbeat.taskExecutionId())
                .query((rs, rowNum) -> new StoredTask(
                        rs.getObject("workflow_execution_id", UUID.class),
                        rs.getString("task_key")
                ))
                .optional()
                .orElseThrow(() -> new ExecutionConflictException(
                        "Unknown task execution " + heartbeat.taskExecutionId()
                ));
        if (!task.workflowExecutionId().equals(heartbeat.workflowExecutionId())) {
            throw new ExecutionConflictException("Heartbeat references the wrong workflow execution");
        }
        if (!task.taskKey().equals(heartbeat.taskKey())) {
            throw new ExecutionConflictException("Heartbeat references the wrong task key");
        }
    }

    private record StoredTask(UUID workflowExecutionId, String taskKey) {
    }
}
