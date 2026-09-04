package io.flowforge.domain.execution;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record TaskAttempt(
        UUID id,
        UUID taskRunId,
        int attemptNumber,
        TaskAttemptStatus status,
        Instant startedAt,
        Instant finishedAt,
        String errorCode,
        String errorMessage
) {
    public TaskAttempt {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(taskRunId, "taskRunId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(startedAt, "startedAt must not be null");
        if (attemptNumber < 1) throw new IllegalArgumentException("attemptNumber must be positive");
        if (status.isTerminal() && finishedAt == null) {
            throw new IllegalArgumentException("finishedAt is required for a terminal attempt");
        }
        if (!status.isTerminal() && finishedAt != null) {
            throw new IllegalArgumentException("finishedAt must be absent for a running attempt");
        }
        if (finishedAt != null && finishedAt.isBefore(startedAt)) {
            throw new IllegalArgumentException("finishedAt must not precede startedAt");
        }
    }
}
