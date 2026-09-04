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
        int attemptNumber
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
    }
}
