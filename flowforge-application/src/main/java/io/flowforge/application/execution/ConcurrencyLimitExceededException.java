package io.flowforge.application.execution;

import java.util.UUID;

public final class ConcurrencyLimitExceededException extends RuntimeException {
    private final UUID workflowId;
    private final int limit;

    public ConcurrencyLimitExceededException(UUID workflowId, int limit) {
        super("Workflow " + workflowId + " already has " + limit + " active execution(s)");
        this.workflowId = workflowId;
        this.limit = limit;
    }

    public UUID workflowId() {
        return workflowId;
    }

    public int limit() {
        return limit;
    }
}
