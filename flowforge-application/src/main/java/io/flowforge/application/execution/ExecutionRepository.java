package io.flowforge.application.execution;

import io.flowforge.domain.execution.WorkflowExecution;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ExecutionRepository {
    WorkflowExecution start(UUID workflowId, String idempotencyKey, Instant now);

    Optional<WorkflowExecution> findById(UUID executionId);

    WorkflowExecution cancel(UUID executionId, Instant now);

    List<TaskWorkItem> claimReadyTasks(int limit, Instant now);

    default ReadyQueueSnapshot readyQueue(Instant now, int requestedLimit) {
        return ReadyQueueSnapshot.unknown(requestedLimit);
    }

    TaskCompletionResult completeTask(TaskCompletion completion, Instant now);
}
