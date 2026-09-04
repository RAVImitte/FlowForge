package io.flowforge.domain.execution;

import java.util.Objects;

public enum TaskRunStatus {
    BLOCKED,
    READY,
    RUNNING,
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    CANCELLED;

    public boolean canTransitionTo(TaskRunStatus target) {
        Objects.requireNonNull(target, "target must not be null");
        return switch (this) {
            case BLOCKED -> target == READY || target == CANCELLED;
            case READY -> target == RUNNING || target == CANCELLED;
            case RUNNING -> target == SUCCEEDED
                    || target == FAILED
                    || target == TIMED_OUT
                    || target == CANCELLED;
            case SUCCEEDED, FAILED, TIMED_OUT, CANCELLED -> false;
        };
    }

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == TIMED_OUT || this == CANCELLED;
    }
}
