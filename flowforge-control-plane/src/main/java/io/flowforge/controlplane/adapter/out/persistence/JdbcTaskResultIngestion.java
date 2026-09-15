package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.coordination.TokenBucketDecision;
import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.coordination.TokenBucketRateLimiter;
import io.flowforge.application.tenancy.TenantQuotaProvider;
import io.flowforge.application.execution.AdmissionBackpressureObserver;
import io.flowforge.application.execution.DurableTaskQueue;
import io.flowforge.application.execution.ExecutionConflictException;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.execution.InboundTaskResult;
import io.flowforge.application.execution.TaskResultIngestion;
import io.flowforge.application.execution.TaskResultIngestionOutcome;
import io.flowforge.application.execution.ReadyQueueSnapshot;
import io.flowforge.controlplane.config.ResultIngestionProperties;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;

@Repository
public class JdbcTaskResultIngestion implements TaskResultIngestion {
    static final String CONSUMER_NAME = "flowforge-control-plane-results-v1";

    private final JdbcClient jdbc;
    private final ExecutionRepository executions;
    private final DurableTaskQueue taskQueue;
    private final ResultIngestionProperties properties;
    private final TokenBucketRateLimiter rateLimiter;
    private final TenantQuotaProvider quotas;
    private final AdmissionBackpressureObserver backpressureObserver;

    public JdbcTaskResultIngestion(
            JdbcClient jdbc,
            ExecutionRepository executions,
            DurableTaskQueue taskQueue,
            ResultIngestionProperties properties,
            TokenBucketRateLimiter rateLimiter,
            TenantQuotaProvider quotas,
            AdmissionBackpressureObserver backpressureObserver
    ) {
        this.jdbc = jdbc;
        this.executions = executions;
        this.taskQueue = taskQueue;
        this.properties = properties;
        this.rateLimiter = rateLimiter;
        this.quotas = quotas;
        this.backpressureObserver = backpressureObserver;
    }

    @Override
    @Transactional
    public TaskResultIngestionOutcome ingest(
            InboundTaskResult result,
            String serializedEnvelope,
            Instant receivedAt
    ) {
        if (serializedEnvelope == null || serializedEnvelope.isBlank()) {
            throw new IllegalArgumentException("Serialized task-result envelope must not be blank");
        }
        validateAndLockWorkflow(result);
        int inserted = jdbc.sql("""
                INSERT INTO control_plane_result_inbox(
                    tenant_id, consumer_name, event_id, workflow_execution_id, task_execution_id,
                    task_key, expected_state_version, attempt_number, outcome,
                    payload, disposition, received_at
                ) VALUES (
                    :tenantId, :consumerName, :eventId, :workflowId, :taskId,
                    :taskKey, :stateVersion, :attemptNumber, :outcome,
                    CAST(:payload AS jsonb), 'PROCESSING', :receivedAt
                )
                ON CONFLICT (tenant_id, consumer_name, event_id) DO NOTHING
                """)
                .param("tenantId", result.tenantId().value())
                .param("consumerName", CONSUMER_NAME)
                .param("eventId", result.eventId())
                .param("workflowId", result.workflowExecutionId())
                .param("taskId", result.taskExecutionId())
                .param("taskKey", result.taskKey())
                .param("stateVersion", result.expectedStateVersion())
                .param("attemptNumber", result.attemptNumber())
                .param("outcome", result.outcome().name())
                .param("payload", serializedEnvelope)
                .param("receivedAt", Timestamp.from(receivedAt))
                .update();

        if (inserted == 0) {
            boolean samePayload = jdbc.sql("""
                    SELECT payload = CAST(:payload AS jsonb)
                     FROM control_plane_result_inbox
                     WHERE tenant_id = :tenantId
                       AND consumer_name = :consumerName AND event_id = :eventId
                    """)
                    .param("tenantId", result.tenantId().value())
                    .param("payload", serializedEnvelope)
                    .param("consumerName", CONSUMER_NAME)
                    .param("eventId", result.eventId())
                    .query(Boolean.class)
                    .single();
            if (!samePayload) {
                throw new ExecutionConflictException(
                        "Task-result event ID " + result.eventId() + " was reused with a different payload"
                );
            }
            return TaskResultIngestionOutcome.DUPLICATE;
        }

        boolean applied = executions.applyTaskCompletion(result.toCompletion(), receivedAt);
        if (applied && properties.inlineDispatchEnabled()) {
            enqueueWithinRateLimit(result, receivedAt);
        }
        TaskResultIngestionOutcome outcome = applied
                ? TaskResultIngestionOutcome.APPLIED
                : TaskResultIngestionOutcome.REDUNDANT;
        int updated = jdbc.sql("""
                UPDATE control_plane_result_inbox
                   SET disposition = :disposition, processed_at = :processedAt
                 WHERE consumer_name = :consumerName
                   AND tenant_id = :tenantId
                   AND event_id = :eventId
                   AND disposition = 'PROCESSING'
                """)
                .param("disposition", outcome.name())
                .param("processedAt", Timestamp.from(receivedAt))
                .param("consumerName", CONSUMER_NAME)
                .param("tenantId", result.tenantId().value())
                .param("eventId", result.eventId())
                .update();
        if (updated != 1) {
            throw new IllegalStateException("Could not finalize task-result inbox event " + result.eventId());
        }
        return outcome;
    }

    private void enqueueWithinRateLimit(InboundTaskResult result, Instant receivedAt) {
        ReadyQueueSnapshot queue = taskQueue.readyQueue(
                result.tenantId(), receivedAt, properties.enqueueBatchSize()
        );
        if (queue == null) queue = ReadyQueueSnapshot.unknown(properties.enqueueBatchSize());
        backpressureObserver.readyQueueObserved(queue.depth(), queue.oldestAge());
        int requested = (int) Math.min(properties.enqueueBatchSize(), queue.depth());
        if (requested == 0) return;
        TokenBucketDecision decision = rateLimiter.consume(
                result.tenantId(), "task-dispatch",
                quotas.quotaFor(result.tenantId()).policy().dispatchRateLimit(), requested, receivedAt
        );
        if (decision.throttled(requested)) {
            backpressureObserver.taskDispatchThrottled(
                    requested, decision.granted(), decision.retryAfter()
            );
        }
        if (decision.granted() > 0) {
            taskQueue.enqueueReadyTasks(
                    result.tenantId(), result.workflowExecutionId(), decision.granted(), receivedAt
            );
        }
    }

    private void validateAndLockWorkflow(InboundTaskResult result) {
        StoredTask task = jdbc.sql("""
                SELECT te.workflow_execution_id, te.task_key,
                       (SELECT ta.fencing_token FROM task_attempt ta
                            WHERE ta.task_execution_id = te.id
                              AND ta.attempt_number = :attemptNumber
                       ) AS fencing_token,
                       EXISTS (
                           SELECT 1 FROM task_attempt ta
                            WHERE ta.task_execution_id = te.id
                              AND ta.attempt_number = :attemptNumber
                       ) AS attempt_exists
                  FROM task_execution te
                 JOIN workflow_execution we ON we.id = te.workflow_execution_id
                 WHERE te.id = :taskId
                   AND we.tenant_id = :tenantId
                 FOR UPDATE OF we
                """)
                .param("tenantId", result.tenantId().value())
                .param("attemptNumber", result.attemptNumber())
                .param("taskId", result.taskExecutionId())
                .query((rs, rowNum) -> new StoredTask(
                        rs.getObject("workflow_execution_id", java.util.UUID.class),
                        rs.getString("task_key"),
                        rs.getObject("fencing_token", java.util.UUID.class),
                        rs.getBoolean("attempt_exists")
                ))
                .optional()
                .orElseThrow(() -> new ExecutionConflictException(
                        "Unknown task execution " + result.taskExecutionId()
                ));
        if (!task.workflowExecutionId().equals(result.workflowExecutionId())) {
            throw new ExecutionConflictException("Task result references the wrong workflow execution");
        }
        if (!task.taskKey().equals(result.taskKey())) {
            throw new ExecutionConflictException("Task result references the wrong task key");
        }
        if (!task.attemptExists()) {
            throw new ExecutionConflictException(
                    "Task result references unknown attempt " + result.attemptNumber()
            );
        }
        if (!java.util.Objects.equals(task.fencingToken(), result.fencingToken())) {
            throw new ExecutionConflictException("Task result has a stale or missing fencing token");
        }
    }

    private record StoredTask(
            java.util.UUID workflowExecutionId,
            String taskKey,
            java.util.UUID fencingToken,
            boolean attemptExists
    ) {
    }
}
