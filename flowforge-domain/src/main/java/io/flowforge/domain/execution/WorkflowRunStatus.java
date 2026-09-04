package io.flowforge.domain.execution;

import java.util.Objects;

public enum WorkflowRunStatus {
    PENDING,
    RUNNING,
    CANCELLING,
    SUCCEEDED,
    FAILED,
    CANCELLED;

    public boolean canTransitionTo(WorkflowRunStatus target) {
        Objects.requireNonNull(target, "target must not be null");
        return switch (this) {
            case PENDING -> target == RUNNING || target == CANCELLED;
            case RUNNING -> target == SUCCEEDED || target == FAILED || target == CANCELLING;
            case CANCELLING -> target == CANCELLED;
            case SUCCEEDED, FAILED, CANCELLED -> false;
        };
    }

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
