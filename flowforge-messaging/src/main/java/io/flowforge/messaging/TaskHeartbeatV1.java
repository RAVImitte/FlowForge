package io.flowforge.messaging;

import java.util.Objects;
import java.util.UUID;

public record TaskHeartbeatV1(
        UUID workflowExecutionId,
        UUID taskExecutionId,
        String taskKey,
        int attemptNumber,
        UUID fencingToken,
        String workerId
) {
    public static final String EVENT_TYPE = "flowforge.task.heartbeat";
    public static final int SCHEMA_VERSION = 1;

    public TaskHeartbeatV1 {
        Objects.requireNonNull(workflowExecutionId, "workflowExecutionId must not be null");
        Objects.requireNonNull(taskExecutionId, "taskExecutionId must not be null");
        Objects.requireNonNull(fencingToken, "fencingToken must not be null");
        taskKey = ContractValidation.requireNonBlank(taskKey, "taskKey");
        workerId = ContractValidation.requireNonBlank(workerId, "workerId");
        if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be positive");
    }
}
