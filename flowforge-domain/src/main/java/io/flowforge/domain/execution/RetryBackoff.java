package io.flowforge.domain.execution;

import io.flowforge.domain.workflow.TaskReliabilityPolicy;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/** Computes restart-stable exponential backoff with deterministic bounded jitter. */
public final class RetryBackoff {
    private RetryBackoff() {
    }

    public static Duration delayAfter(
            TaskReliabilityPolicy policy,
            UUID taskRunId,
            int completedAttempt
    ) {
        Objects.requireNonNull(policy, "policy must not be null");
        Objects.requireNonNull(taskRunId, "taskRunId must not be null");
        if (completedAttempt < 1) {
            throw new IllegalArgumentException("completedAttempt must be positive");
        }

        long initialMillis = policy.initialBackoff().toMillis();
        long maximumMillis = policy.maxBackoff().toMillis();
        if (initialMillis == 0 || maximumMillis == 0) return Duration.ZERO;

        double exponential = initialMillis * Math.pow(policy.backoffMultiplier(), completedAttempt - 1.0);
        double capped = Math.min(maximumMillis, exponential);
        double jitter = 1.0 + ((unitInterval(taskRunId, completedAttempt) * 2.0) - 1.0)
                * policy.jitterFactor();
        long delayMillis = Math.max(0L, Math.round(capped * jitter));
        return Duration.ofMillis(delayMillis);
    }

    private static double unitInterval(UUID taskRunId, int completedAttempt) {
        long value = taskRunId.getMostSignificantBits()
                ^ Long.rotateLeft(taskRunId.getLeastSignificantBits(), 23)
                ^ (0x9E3779B97F4A7C15L * completedAttempt);
        value += 0x9E3779B97F4A7C15L;
        value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
        value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
        value ^= value >>> 31;
        return (value >>> 11) * 0x1.0p-53;
    }
}
