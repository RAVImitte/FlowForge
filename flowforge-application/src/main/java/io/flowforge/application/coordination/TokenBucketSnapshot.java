package io.flowforge.application.coordination;

import java.time.Instant;
import java.util.Objects;

public record TokenBucketSnapshot(
        String bucketKey,
        TokenBucketPolicy policy,
        double availableTokens,
        Instant lastRefillAt,
        long stateVersion
) {
    public TokenBucketSnapshot {
        if (bucketKey == null || bucketKey.isBlank()) {
            throw new IllegalArgumentException("bucketKey must not be blank");
        }
        Objects.requireNonNull(policy, "policy must not be null");
        Objects.requireNonNull(lastRefillAt, "lastRefillAt must not be null");
        if (!Double.isFinite(availableTokens)
                || availableTokens < 0
                || availableTokens > policy.capacity()) {
            throw new IllegalArgumentException("availableTokens must be within bucket capacity");
        }
        if (stateVersion < 1) throw new IllegalArgumentException("stateVersion must be positive");
    }
}
