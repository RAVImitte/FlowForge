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
        Instant finishedAt,
        Instant nextAttemptAt
) {
    public TaskRun {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(workflowRunId, "workflowRunId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        taskKey = requireTaskKey(taskKey);
        if (stateVersion < 0) throw new IllegalArgumentException("stateVersion must not be negative");
        validateTimeline(status, createdAt, startedAt, finishedAt, nextAttemptAt);
    }

    public TaskRun(
            UUID id,
            UUID workflowRunId,
            String taskKey,
            TaskRunStatus status,
            long stateVersion,
            Instant createdAt,
            Instant startedAt,
            Instant finishedAt
    ) {
        this(id, workflowRunId, taskKey, status, stateVersion, createdAt, startedAt, finishedAt, null);
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

        if (target == TaskRunStatus.RETRY_SCHEDULED) {
            throw new IllegalArgumentException("use scheduleRetry to supply the next attempt time");
        }
        Instant nextStartedAt = target == TaskRunStatus.RUNNING && startedAt == null ? occurredAt : startedAt;
        Instant nextFinishedAt = target.isTerminal() ? occurredAt : null;
        return new TaskRun(
                id,
                workflowRunId,
                taskKey,
                target,
                stateVersion + 1,
                createdAt,
                nextStartedAt,
                nextFinishedAt,
                null
        );
    }

    public TaskRun scheduleRetry(Instant occurredAt, Instant retryAt) {
        validateTransitionTime(occurredAt);
        Objects.requireNonNull(retryAt, "retryAt must not be null");
        if (status != TaskRunStatus.RUNNING) {
            throw new InvalidStateTransitionException("task run", status, TaskRunStatus.RETRY_SCHEDULED);
        }
        if (retryAt.isBefore(occurredAt)) {
            throw new IllegalArgumentException("retryAt must not precede the scheduling time");
        }
        return new TaskRun(
                id,
                workflowRunId,
                taskKey,
                TaskRunStatus.RETRY_SCHEDULED,
                stateVersion + 1,
                createdAt,
                startedAt,
                null,
                retryAt
        );
    }

    private static TaskRun initial(
            UUID id,
            UUID workflowRunId,
            String taskKey,
            TaskRunStatus status,
            Instant createdAt
    ) {
        return new TaskRun(id, workflowRunId, taskKey, status, 0, createdAt, null, null, null);
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
            Instant finishedAt,
            Instant nextAttemptAt
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
        if (nextAttemptAt != null && nextAttemptAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("nextAttemptAt must not precede createdAt");
        }
        if (startedAt != null && nextAttemptAt != null && nextAttemptAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("nextAttemptAt must not precede startedAt");
        }

        switch (status) {
            case BLOCKED -> requireTimestamps(
                    startedAt == null && finishedAt == null && nextAttemptAt == null, status);
            case READY -> requireTimestamps(finishedAt == null && nextAttemptAt == null, status);
            case RUNNING -> requireTimestamps(
                    startedAt != null && finishedAt == null && nextAttemptAt == null, status);
            case RETRY_SCHEDULED -> requireTimestamps(
                    startedAt != null && finishedAt == null && nextAttemptAt != null, status);
            case SUCCEEDED, FAILED, TIMED_OUT -> requireTimestamps(
                    startedAt != null && finishedAt != null && nextAttemptAt == null, status);
            case CANCELLED -> requireTimestamps(finishedAt != null && nextAttemptAt == null, status);
        }
    }

    private static void requireTimestamps(boolean valid, TaskRunStatus status) {
        if (!valid) throw new IllegalArgumentException("timestamps are inconsistent with task status " + status);
    }
}
