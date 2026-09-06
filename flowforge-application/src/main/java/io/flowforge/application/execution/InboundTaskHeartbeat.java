package io.flowforge.application.execution;

import io.flowforge.domain.tenancy.TenantId;

import java.util.Objects;
import java.util.UUID;

public record InboundTaskHeartbeat(
        TenantId tenantId,
        UUID eventId,
        UUID workflowExecutionId,
        UUID taskExecutionId,
        String taskKey,
        int attemptNumber,
        UUID fencingToken,
        String workerId
) {
    public InboundTaskHeartbeat {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(workflowExecutionId, "workflowExecutionId must not be null");
        Objects.requireNonNull(taskExecutionId, "taskExecutionId must not be null");
        Objects.requireNonNull(fencingToken, "fencingToken must not be null");
        if (taskKey == null || taskKey.isBlank()) throw new IllegalArgumentException("taskKey must not be blank");
        if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be positive");
        if (workerId == null || workerId.isBlank()) throw new IllegalArgumentException("workerId must not be blank");
        taskKey = taskKey.trim();
        workerId = workerId.trim();
    }

}
