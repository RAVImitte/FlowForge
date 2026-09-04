package io.flowforge.application.execution;

import io.flowforge.domain.execution.TaskRunStatus;

public enum TaskOutcome {
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    CANCELLED;

    public TaskRunStatus toTaskStatus() {
        return TaskRunStatus.valueOf(name());
    }
}
