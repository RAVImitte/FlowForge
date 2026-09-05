package io.flowforge.messaging;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record TaskCommandV1(
        UUID workflowExecutionId,
        UUID taskExecutionId,
        String taskKey,
        String taskType,
        Map<String, Object> configuration,
        long expectedStateVersion,
        int attemptNumber,
        UUID fencingToken,
        Long attemptTimeoutMs
) {
    public static final String EVENT_TYPE = "flowforge.task.command";
    public static final int SCHEMA_VERSION = 1;

    public TaskCommandV1 {
        Objects.requireNonNull(workflowExecutionId, "workflowExecutionId must not be null");
        Objects.requireNonNull(taskExecutionId, "taskExecutionId must not be null");
        taskKey = ContractValidation.requireNonBlank(taskKey, "taskKey");
        taskType = ContractValidation.requireNonBlank(taskType, "taskType");
        configuration = ContractValidation.immutableConfiguration(configuration);
        if (expectedStateVersion < 0) throw new IllegalArgumentException("expectedStateVersion must not be negative");
        if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be positive");
        if (attemptTimeoutMs != null && attemptTimeoutMs < 1) {
            throw new IllegalArgumentException("attemptTimeoutMs must be positive when configured");
        }
    }

    public TaskCommandV1(
            UUID workflowExecutionId,
            UUID taskExecutionId,
            String taskKey,
            String taskType,
            Map<String, Object> configuration,
            long expectedStateVersion,
            int attemptNumber
    ) {
        this(
                workflowExecutionId,
                taskExecutionId,
                taskKey,
                taskType,
                configuration,
                expectedStateVersion,
                attemptNumber,
                null,
                null
        );
    }
}
