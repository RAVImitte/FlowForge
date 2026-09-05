package io.flowforge.application.schedule;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ClaimedScheduleFire(
        UUID triggerId,
        UUID scheduleId,
        UUID workflowId,
        Instant scheduledFireAt,
        String idempotencyKey,
        int attemptCount,
        UUID claimToken
) {
    public ClaimedScheduleFire {
        Objects.requireNonNull(triggerId, "triggerId must not be null");
        Objects.requireNonNull(scheduleId, "scheduleId must not be null");
        Objects.requireNonNull(workflowId, "workflowId must not be null");
        Objects.requireNonNull(scheduledFireAt, "scheduledFireAt must not be null");
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        if (attemptCount < 1) throw new IllegalArgumentException("attemptCount must be positive");
        Objects.requireNonNull(claimToken, "claimToken must not be null");
    }
}
