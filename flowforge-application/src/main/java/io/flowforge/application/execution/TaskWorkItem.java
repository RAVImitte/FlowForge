package io.flowforge.application.execution;

import io.flowforge.domain.workflow.SecretReference;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record TaskWorkItem(
        UUID workflowRunId,
        UUID taskRunId,
        String taskKey,
        String taskType,
        Map<String, Object> configuration,
        Map<String, SecretReference> secretReferences,
        long stateVersion,
        int attemptNumber,
        UUID fencingToken,
        Long attemptTimeoutMs
) {
    public TaskWorkItem {
        Objects.requireNonNull(workflowRunId, "workflowRunId must not be null");
        Objects.requireNonNull(taskRunId, "taskRunId must not be null");
        if (taskKey == null || taskKey.isBlank()) throw new IllegalArgumentException("taskKey must not be blank");
        if (taskType == null || taskType.isBlank()) throw new IllegalArgumentException("taskType must not be blank");
        configuration = Map.copyOf(configuration);
        secretReferences = secretReferences == null ? Map.of() : Map.copyOf(secretReferences);
        if (stateVersion < 1) throw new IllegalArgumentException("stateVersion must be positive");
        if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be positive");
        if (attemptTimeoutMs != null && attemptTimeoutMs < 1) {
            throw new IllegalArgumentException("attemptTimeoutMs must be positive when configured");
        }
    }

    public TaskWorkItem(
            UUID workflowRunId,
            UUID taskRunId,
            String taskKey,
            String taskType,
            Map<String, Object> configuration,
            long stateVersion,
            int attemptNumber
    ) {
        this(
                workflowRunId,
                taskRunId,
                taskKey,
                taskType,
                configuration,
                Map.of(),
                stateVersion,
                attemptNumber,
                null,
                null
        );
    }

    public TaskWorkItem(
            UUID workflowRunId,
            UUID taskRunId,
            String taskKey,
            String taskType,
            Map<String, Object> configuration,
            long stateVersion,
            int attemptNumber,
            Long attemptTimeoutMs
    ) {
        this(workflowRunId, taskRunId, taskKey, taskType, configuration, Map.of(), stateVersion,
                attemptNumber, null, attemptTimeoutMs);
    }

    public TaskWorkItem(
            UUID workflowRunId,
            UUID taskRunId,
            String taskKey,
            String taskType,
            Map<String, Object> configuration,
            long stateVersion,
            int attemptNumber,
            UUID fencingToken,
            Long attemptTimeoutMs
    ) {
        this(workflowRunId, taskRunId, taskKey, taskType, configuration, Map.of(), stateVersion,
                attemptNumber, fencingToken, attemptTimeoutMs);
    }
}
