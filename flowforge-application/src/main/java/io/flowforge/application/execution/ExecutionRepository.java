package io.flowforge.application.execution;

import io.flowforge.application.workflow.PageResult;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.execution.WorkflowExecution;
import io.flowforge.domain.execution.WorkflowRunStatus;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;


public interface ExecutionRepository {
    WorkflowExecution start(TenantId tenantId, UUID workflowId, String idempotencyKey, Instant now);

    Optional<WorkflowExecution> findById(TenantId tenantId, UUID executionId);

    WorkflowExecution cancel(TenantId tenantId, UUID executionId, Instant now);

    PageResult<ExecutionSummary> list(TenantId tenantId, int page, int size, WorkflowRunStatus statusFilter);


    List<TenantId> readyTenants(Instant now, int limit);

    List<TaskWorkItem> claimReadyTasks(TenantId tenantId, int limit, Instant now);

    default ReadyQueueSnapshot readyQueue(TenantId tenantId, Instant now, int requestedLimit) {
        return ReadyQueueSnapshot.unknown(requestedLimit);
    }

    TaskCompletionResult completeTask(TaskCompletion completion, Instant now);

    default boolean applyTaskCompletion(TaskCompletion completion, Instant now) {
        return completeTask(completion, now).applied();
    }
}
