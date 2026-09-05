package io.flowforge.application.coordination;

import java.time.Duration;
import java.util.Objects;

public record TokenBucketDecision(int granted, Duration retryAfter) {
    public TokenBucketDecision {
        if (granted < 0) throw new IllegalArgumentException("granted must not be negative");
        Objects.requireNonNull(retryAfter, "retryAfter must not be null");
        if (retryAfter.isNegative()) throw new IllegalArgumentException("retryAfter must not be negative");
    }

    public boolean throttled(int requested) {
        return granted < requested;
    }
}
