package io.flowforge.domain.execution;

public enum TaskAttemptStatus {
    RUNNING,
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    CANCELLED;

    public boolean isTerminal() {
        return this != RUNNING;
    }
}
