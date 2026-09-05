package io.flowforge.application.execution;

import java.util.Objects;

public record TaskResult(
        TaskOutcome outcome,
        String errorCode,
        String errorMessage,
        Boolean retryable
) {
    public TaskResult {
        Objects.requireNonNull(outcome, "outcome must not be null");
        if (outcome == TaskOutcome.SUCCEEDED && (errorCode != null || errorMessage != null)) {
            throw new IllegalArgumentException("successful task results cannot contain errors");
        }
        if (outcome == TaskOutcome.SUCCEEDED && retryable != null) {
            throw new IllegalArgumentException("successful task results cannot be classified as retryable");
        }
    }

    public TaskResult(TaskOutcome outcome, String errorCode, String errorMessage) {
        this(outcome, errorCode, errorMessage, null);
    }

    public static TaskResult succeeded() {
        return new TaskResult(TaskOutcome.SUCCEEDED, null, null, null);
    }

    public static TaskResult failed(String errorCode, String errorMessage) {
        return new TaskResult(TaskOutcome.FAILED, errorCode, errorMessage, false);
    }

    public static TaskResult retryableFailure(String errorCode, String errorMessage) {
        return new TaskResult(TaskOutcome.FAILED, errorCode, errorMessage, true);
    }

    public static TaskResult timedOut(String errorMessage) {
        return new TaskResult(TaskOutcome.TIMED_OUT, "TASK_TIMEOUT", errorMessage, true);
    }

    public static TaskResult cancelled() {
        return new TaskResult(TaskOutcome.CANCELLED, null, null, false);
    }
}
