package io.flowforge.application.execution;

import java.util.Objects;
import java.util.UUID;

public record InboundTaskResult(
        UUID eventId,
        UUID workflowExecutionId,
        UUID taskExecutionId,
        String taskKey,
        long expectedStateVersion,
        int attemptNumber,
        TaskOutcome outcome,
        String errorCode,
        String errorMessage,
        Boolean retryable,
        UUID fencingToken
) {
    public InboundTaskResult {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(workflowExecutionId, "workflowExecutionId must not be null");
        Objects.requireNonNull(taskExecutionId, "taskExecutionId must not be null");
        taskKey = requireNonBlank(taskKey, "taskKey");
        if (expectedStateVersion < 1) {
            throw new IllegalArgumentException("expectedStateVersion must be positive");
        }
        if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be positive");
        Objects.requireNonNull(outcome, "outcome must not be null");
        errorCode = normalize(errorCode);
        errorMessage = normalize(errorMessage);
    }

    public InboundTaskResult(
            UUID eventId,
            UUID workflowExecutionId,
            UUID taskExecutionId,
            String taskKey,
            long expectedStateVersion,
            int attemptNumber,
            TaskOutcome outcome,
            String errorCode,
            String errorMessage
    ) {
        this(
                eventId,
                workflowExecutionId,
                taskExecutionId,
                taskKey,
                expectedStateVersion,
                attemptNumber,
                outcome,
                errorCode,
                errorMessage,
                null,
                null
        );
    }

    public TaskCompletion toCompletion() {
        return new TaskCompletion(
                taskExecutionId,
                expectedStateVersion,
                outcome,
                errorCode,
                errorMessage,
                retryable,
                fencingToken
        );
    }

    public InboundTaskResult(
            UUID eventId, UUID workflowExecutionId, UUID taskExecutionId, String taskKey,
            long expectedStateVersion, int attemptNumber, TaskOutcome outcome,
            String errorCode, String errorMessage, Boolean retryable
    ) {
        this(eventId, workflowExecutionId, taskExecutionId, taskKey, expectedStateVersion,
                attemptNumber, outcome, errorCode, errorMessage, retryable, null);
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
        return value.trim();
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
