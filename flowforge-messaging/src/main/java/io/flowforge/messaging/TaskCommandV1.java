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
        Map<String, SecretReferenceV1> secretReferences,
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
        secretReferences = secretReferences == null ? Map.of() : Map.copyOf(secretReferences);
        secretReferences.forEach((binding, reference) -> {
            if (binding == null || !binding.matches("[A-Za-z][A-Za-z0-9_.-]{0,99}")) {
                throw new IllegalArgumentException("secret binding must be a valid identifier");
            }
            Objects.requireNonNull(reference, "secret reference must not be null");
        });
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
                Map.of(),
                expectedStateVersion,
                attemptNumber,
                null,
                null
        );
    }

    public TaskCommandV1(
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
        this(workflowExecutionId, taskExecutionId, taskKey, taskType, configuration, Map.of(),
                expectedStateVersion, attemptNumber, fencingToken, attemptTimeoutMs);
    }
}
