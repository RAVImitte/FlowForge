package io.flowforge.application.execution;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record TaskWorkItem(
        UUID workflowRunId,
        UUID taskRunId,
        String taskKey,
        String taskType,
        Map<String, Object> configuration,
        long stateVersion,
        int attemptNumber
) {
    public TaskWorkItem {
        Objects.requireNonNull(workflowRunId, "workflowRunId must not be null");
        Objects.requireNonNull(taskRunId, "taskRunId must not be null");
        if (taskKey == null || taskKey.isBlank()) throw new IllegalArgumentException("taskKey must not be blank");
        if (taskType == null || taskType.isBlank()) throw new IllegalArgumentException("taskType must not be blank");
        configuration = Map.copyOf(configuration);
        if (stateVersion < 1) throw new IllegalArgumentException("stateVersion must be positive");
        if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be positive");
    }
}
