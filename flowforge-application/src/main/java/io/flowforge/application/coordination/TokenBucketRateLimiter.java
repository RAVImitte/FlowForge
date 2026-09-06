package io.flowforge.application.coordination;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;

public interface TokenBucketRateLimiter {
    TokenBucketDecision consume(
            TenantId tenantId,
            String bucketKey,
            TokenBucketPolicy policy,
            int requested,
            Instant now
    );
}
