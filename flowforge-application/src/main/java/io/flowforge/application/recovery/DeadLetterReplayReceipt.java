package io.flowforge.application.recovery;

import java.time.Instant;
import java.util.UUID;

public record DeadLetterReplayReceipt(
        UUID idempotencyKey,
        String recordId,
        String sourceTopic,
        Status status,
        Instant publishedAt
) {
    public enum Status {
        PUBLISHED,
        ALREADY_PUBLISHED
    }
}
