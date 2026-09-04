package io.flowforge.application.execution;

import java.util.Objects;
import java.util.UUID;

public record TaskCompletion(
        UUID taskRunId,
        long expectedStateVersion,
        TaskOutcome outcome,
        String errorCode,
        String errorMessage
) {
    public TaskCompletion {
        Objects.requireNonNull(taskRunId, "taskRunId must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        if (expectedStateVersion < 1) throw new IllegalArgumentException("expectedStateVersion must be positive");
    }

    public static TaskCompletion from(TaskWorkItem workItem, TaskResult result) {
        return new TaskCompletion(
                workItem.taskRunId(),
                workItem.stateVersion(),
                result.outcome(),
                result.errorCode(),
                result.errorMessage()
        );
    }
}
