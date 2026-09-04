package io.flowforge.messaging;

import java.util.Objects;
import java.util.UUID;

public record ExecutionEventV1(
        UUID workflowExecutionId,
        UUID taskExecutionId,
        String transitionType,
        String fromStatus,
        String toStatus
) {
    public static final String EVENT_TYPE = "flowforge.execution.transition";
    public static final int SCHEMA_VERSION = 1;

    public ExecutionEventV1 {
        Objects.requireNonNull(workflowExecutionId, "workflowExecutionId must not be null");
        transitionType = ContractValidation.requireNonBlank(transitionType, "transitionType");
        toStatus = ContractValidation.requireNonBlank(toStatus, "toStatus");
        fromStatus = fromStatus == null || fromStatus.isBlank() ? null : fromStatus.trim();
    }
}
