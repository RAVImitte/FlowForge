package io.flowforge.domain.execution;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record TaskRun(
        UUID id,
        UUID workflowRunId,
        String taskKey,
        TaskRunStatus status,
        long stateVersion,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt
) {
    public TaskRun {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(workflowRunId, "workflowRunId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        taskKey = requireTaskKey(taskKey);
        if (stateVersion < 0) throw new IllegalArgumentException("stateVersion must not be negative");
        validateTimeline(status, createdAt, startedAt, finishedAt);
    }

    public static TaskRun blocked(UUID id, UUID workflowRunId, String taskKey, Instant createdAt) {
        return initial(id, workflowRunId, taskKey, TaskRunStatus.BLOCKED, createdAt);
    }

    public static TaskRun ready(UUID id, UUID workflowRunId, String taskKey, Instant createdAt) {
        return initial(id, workflowRunId, taskKey, TaskRunStatus.READY, createdAt);
    }

    public TaskRun transitionTo(TaskRunStatus target, Instant occurredAt) {
        Objects.requireNonNull(target, "target must not be null");
        validateTransitionTime(occurredAt);
        if (!status.canTransitionTo(target)) {
            throw new InvalidStateTransitionException("task run", status, target);
        }

        Instant nextStartedAt = target == TaskRunStatus.RUNNING ? occurredAt : startedAt;
        Instant nextFinishedAt = target.isTerminal() ? occurredAt : null;
        return new TaskRun(
                id,
                workflowRunId,
                taskKey,
                target,
                stateVersion + 1,
                createdAt,
                nextStartedAt,
                nextFinishedAt
        );
    }

    private static TaskRun initial(
            UUID id,
            UUID workflowRunId,
            String taskKey,
            TaskRunStatus status,
            Instant createdAt
    ) {
        return new TaskRun(id, workflowRunId, taskKey, status, 0, createdAt, null, null);
    }

    private static String requireTaskKey(String taskKey) {
        if (taskKey == null || taskKey.isBlank()) {
            throw new IllegalArgumentException("taskKey must not be blank");
        }
        return taskKey.strip();
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
            TaskRunStatus status,
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
            case BLOCKED, READY -> requireTimestamps(startedAt == null && finishedAt == null, status);
            case RUNNING -> requireTimestamps(startedAt != null && finishedAt == null, status);
            case SUCCEEDED, FAILED, TIMED_OUT -> requireTimestamps(startedAt != null && finishedAt != null, status);
            case CANCELLED -> requireTimestamps(finishedAt != null, status);
        }
    }

    private static void requireTimestamps(boolean valid, TaskRunStatus status) {
        if (!valid) throw new IllegalArgumentException("timestamps are inconsistent with task status " + status);
    }
}
