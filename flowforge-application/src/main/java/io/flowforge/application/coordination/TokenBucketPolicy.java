package io.flowforge.application.coordination;

import java.time.Duration;
import java.util.Objects;

public record TokenBucketPolicy(int capacity, int refillTokens, Duration refillPeriod) {
    public TokenBucketPolicy {
        if (capacity < 1 || capacity > 1_000_000) {
            throw new IllegalArgumentException("capacity must be between 1 and 1000000");
        }
        if (refillTokens < 1 || refillTokens > 1_000_000) {
            throw new IllegalArgumentException("refillTokens must be between 1 and 1000000");
        }
        Objects.requireNonNull(refillPeriod, "refillPeriod must not be null");
        if (refillPeriod.isNegative() || refillPeriod.toMillis() < 1) {
            throw new IllegalArgumentException("refillPeriod must be at least 1 millisecond");
        }
    }
}
