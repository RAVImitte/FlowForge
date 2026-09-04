package io.flowforge.domain.execution;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ExecutionEvent(
        UUID id,
        UUID workflowRunId,
        UUID taskRunId,
        ExecutionEventType type,
        String fromStatus,
        String toStatus,
        Instant occurredAt
) {
    public ExecutionEvent {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(workflowRunId, "workflowRunId must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    }
}
