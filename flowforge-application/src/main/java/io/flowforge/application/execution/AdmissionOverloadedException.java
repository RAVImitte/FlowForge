package io.flowforge.application.execution;

import java.time.Duration;
import java.util.UUID;

public final class AdmissionOverloadedException extends RuntimeException {
    private final UUID workflowId;
    private final int limit;
    private final Duration retryAfter;

    public AdmissionOverloadedException(UUID workflowId, int limit, Duration retryAfter) {
        super("Workflow " + workflowId + " ready queue has reached its limit of " + limit);
        this.workflowId = workflowId;
        this.limit = limit;
        this.retryAfter = retryAfter;
    }

    public UUID workflowId() {
        return workflowId;
    }

    public int limit() {
        return limit;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
