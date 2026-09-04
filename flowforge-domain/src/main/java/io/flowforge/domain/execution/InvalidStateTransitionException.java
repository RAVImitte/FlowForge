package io.flowforge.domain.execution;

import java.util.Objects;

public final class InvalidStateTransitionException extends RuntimeException {
    private final String aggregateType;
    private final String currentStatus;
    private final String targetStatus;

    public InvalidStateTransitionException(String aggregateType, Enum<?> currentStatus, Enum<?> targetStatus) {
        super("Invalid " + aggregateType + " transition from " + currentStatus + " to " + targetStatus);
        this.aggregateType = Objects.requireNonNull(aggregateType, "aggregateType must not be null");
        this.currentStatus = Objects.requireNonNull(currentStatus, "currentStatus must not be null").name();
        this.targetStatus = Objects.requireNonNull(targetStatus, "targetStatus must not be null").name();
    }

    public String aggregateType() {
        return aggregateType;
    }

    public String currentStatus() {
        return currentStatus;
    }

    public String targetStatus() {
        return targetStatus;
    }
}
