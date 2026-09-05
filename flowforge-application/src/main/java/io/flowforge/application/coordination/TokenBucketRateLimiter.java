package io.flowforge.application.coordination;

import java.time.Instant;

public interface TokenBucketRateLimiter {
    TokenBucketDecision consume(
            String bucketKey,
            TokenBucketPolicy policy,
            int requested,
            Instant now
    );
}
