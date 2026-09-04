package io.flowforge.domain.execution;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record WorkflowRun(
        UUID id,
        UUID workflowId,
        int workflowVersion,
        WorkflowRunStatus status,
        long stateVersion,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt
) {
    public WorkflowRun {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(workflowId, "workflowId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        if (workflowVersion < 1) throw new IllegalArgumentException("workflowVersion must be positive");
        if (stateVersion < 0) throw new IllegalArgumentException("stateVersion must not be negative");
        validateTimeline(status, createdAt, startedAt, finishedAt);
    }

    public static WorkflowRun pending(UUID id, UUID workflowId, int workflowVersion, Instant createdAt) {
        return new WorkflowRun(
                id,
                workflowId,
                workflowVersion,
                WorkflowRunStatus.PENDING,
                0,
                createdAt,
                null,
                null
        );
    }

    public WorkflowRun transitionTo(WorkflowRunStatus target, Instant occurredAt) {
        Objects.requireNonNull(target, "target must not be null");
        validateTransitionTime(occurredAt);
        if (!status.canTransitionTo(target)) {
            throw new InvalidStateTransitionException("workflow run", status, target);
        }

        Instant nextStartedAt = target == WorkflowRunStatus.RUNNING ? occurredAt : startedAt;
        Instant nextFinishedAt = target.isTerminal() ? occurredAt : null;
        return new WorkflowRun(
                id,
                workflowId,
                workflowVersion,
                target,
                stateVersion + 1,
                createdAt,
                nextStartedAt,
                nextFinishedAt
        );
    }

    private void validateTransitionTime(Instant occurredAt) {
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        if (occurredAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("transition time must not precede creation time");
        }
        if (startedAt != null && occurredAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("transition time must not precede start time");
        }
    }

    private static void validateTimeline(
            WorkflowRunStatus status,
            Instant createdAt,
            Instant startedAt,
            Instant finishedAt
    ) {
        if (startedAt != null && startedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("startedAt must not precede createdAt");
        }
        if (finishedAt != null && finishedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("finishedAt must not precede createdAt");
        }
        if (startedAt != null && finishedAt != null && finishedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("finishedAt must not precede startedAt");
        }

        switch (status) {
            case PENDING -> requireTimestamps(startedAt == null && finishedAt == null, status);
            case RUNNING, CANCELLING -> requireTimestamps(startedAt != null && finishedAt == null, status);
            case SUCCEEDED, FAILED -> requireTimestamps(startedAt != null && finishedAt != null, status);
            case CANCELLED -> requireTimestamps(finishedAt != null, status);
        }
    }

    private static void requireTimestamps(boolean valid, WorkflowRunStatus status) {
        if (!valid) throw new IllegalArgumentException("timestamps are inconsistent with workflow status " + status);
    }
}
