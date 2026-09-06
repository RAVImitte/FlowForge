package io.flowforge.domain.execution;

import io.flowforge.domain.tenancy.TenantId;

import java.util.List;
import java.util.Objects;

public record WorkflowExecution(
        TenantId tenantId,
        WorkflowRun workflow,
        List<TaskRun> tasks,
        List<TaskAttempt> attempts,
        List<ExecutionEvent> events
) {
    public WorkflowExecution {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(workflow, "workflow must not be null");
        tasks = List.copyOf(tasks);
        attempts = List.copyOf(attempts);
        events = List.copyOf(events);
    }
}
