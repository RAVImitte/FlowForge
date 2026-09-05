package io.flowforge.application.execution;

import java.util.Objects;
import java.util.UUID;

public record TaskCompletion(
        UUID taskRunId,
        long expectedStateVersion,
        TaskOutcome outcome,
        String errorCode,
        String errorMessage,
        Boolean retryable,
        UUID fencingToken
) {
    public TaskCompletion {
        Objects.requireNonNull(taskRunId, "taskRunId must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
        if (expectedStateVersion < 1) throw new IllegalArgumentException("expectedStateVersion must be positive");
    }

    public TaskCompletion(
            UUID taskRunId,
            long expectedStateVersion,
            TaskOutcome outcome,
            String errorCode,
            String errorMessage
    ) {
        this(taskRunId, expectedStateVersion, outcome, errorCode, errorMessage, null, null);
    }

    public static TaskCompletion from(TaskWorkItem workItem, TaskResult result) {
        return new TaskCompletion(
                workItem.taskRunId(),
                workItem.stateVersion(),
                result.outcome(),
                result.errorCode(),
                result.errorMessage(),
                result.retryable(),
                workItem.fencingToken()
        );
    }

    public TaskCompletion(
            UUID taskRunId,
            long expectedStateVersion,
            TaskOutcome outcome,
            String errorCode,
            String errorMessage,
            Boolean retryable
    ) {
        this(taskRunId, expectedStateVersion, outcome, errorCode, errorMessage, retryable, null);
    }
}
