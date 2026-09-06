package io.flowforge.application.recovery;

import java.time.Instant;
import java.util.UUID;

public record DeadLetterReplayClaim(
        UUID idempotencyKey,
        State state,
        Instant publishedAt
) {
    public enum State {
        ACQUIRED,
        IN_PROGRESS,
        ALREADY_PUBLISHED
    }
}
