package io.flowforge.messaging;

import java.util.Objects;
import java.util.UUID;

public record TaskResultV1(
        UUID workflowExecutionId,
        UUID taskExecutionId,
        String taskKey,
        long expectedStateVersion,
        int attemptNumber,
        TaskResultOutcomeV1 outcome,
        String errorCode,
        String errorMessage
) {
    public static final String EVENT_TYPE = "flowforge.task.result";
    public static final int SCHEMA_VERSION = 1;

    public TaskResultV1 {
        Objects.requireNonNull(workflowExecutionId, "workflowExecutionId must not be null");
        Objects.requireNonNull(taskExecutionId, "taskExecutionId must not be null");
        taskKey = ContractValidation.requireNonBlank(taskKey, "taskKey");
        if (expectedStateVersion < 0) throw new IllegalArgumentException("expectedStateVersion must not be negative");
        if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be positive");
        Objects.requireNonNull(outcome, "outcome must not be null");
        errorCode = normalize(errorCode);
        errorMessage = normalize(errorMessage);
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
