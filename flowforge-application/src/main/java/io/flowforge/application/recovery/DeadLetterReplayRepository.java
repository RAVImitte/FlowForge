package io.flowforge.application.recovery;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public interface DeadLetterReplayRepository {
    DeadLetterReplayClaim claim(
            TenantId tenantId,
            UUID idempotencyKey,
            DeadLetterRecord record,
            String actor,
            String reason,
            Instant now,
            Duration leaseDuration
    );

    void markPublished(TenantId tenantId, UUID idempotencyKey, Instant publishedAt);

    void markFailed(TenantId tenantId, UUID idempotencyKey, String failureClass, Instant failedAt);
}
