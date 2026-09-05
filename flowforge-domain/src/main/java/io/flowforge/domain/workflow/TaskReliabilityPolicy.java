package io.flowforge.domain.workflow;

import java.time.Duration;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Immutable reliability policy captured with a workflow version.
 *
 * <p>A single-attempt default preserves the execution semantics of workflows
 * created before retry support was introduced.</p>
 */
public record TaskReliabilityPolicy(
        int maxAttempts,
        Duration initialBackoff,
        double backoffMultiplier,
        Duration maxBackoff,
        double jitterFactor,
        Duration attemptTimeout,
        Set<String> retryableErrorCodes
) {
    private static final int MAX_ATTEMPTS_LIMIT = 100;
    private static final int MAX_ERROR_CODES = 100;
    private static final int MAX_ERROR_CODE_LENGTH = 100;
    private static final TaskReliabilityPolicy DEFAULT = new TaskReliabilityPolicy(
            1, Duration.ZERO, 2.0, Duration.ZERO, 0.0, null, Set.of()
    );

    public TaskReliabilityPolicy {
        if (maxAttempts < 1 || maxAttempts > MAX_ATTEMPTS_LIMIT) {
            throw new IllegalArgumentException("maxAttempts must be between 1 and 100");
        }
        initialBackoff = requireNonNegative(initialBackoff, "initialBackoff");
        maxBackoff = requireNonNegative(maxBackoff, "maxBackoff");
        requireDurableMilliseconds(initialBackoff, "initialBackoff");
        requireDurableMilliseconds(maxBackoff, "maxBackoff");
        if (!Double.isFinite(backoffMultiplier) || backoffMultiplier < 1.0) {
            throw new IllegalArgumentException("backoffMultiplier must be finite and at least 1.0");
        }
        if (!Double.isFinite(jitterFactor) || jitterFactor < 0.0 || jitterFactor > 1.0) {
            throw new IllegalArgumentException("jitterFactor must be between 0.0 and 1.0");
        }
        if (maxAttempts > 1 && maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException("maxBackoff must not be less than initialBackoff");
        }
        if (attemptTimeout != null && (attemptTimeout.isZero() || attemptTimeout.isNegative())) {
            throw new IllegalArgumentException("attemptTimeout must be positive when configured");
        }
        if (attemptTimeout != null) requireDurableMilliseconds(attemptTimeout, "attemptTimeout");
        retryableErrorCodes = normalizeErrorCodes(retryableErrorCodes);
    }

    public static TaskReliabilityPolicy defaults() {
        return DEFAULT;
    }

    public boolean isDefault() {
        return equals(DEFAULT);
    }

    public boolean isRetryable(String errorCode, Boolean workerClassification) {
        if (retryableErrorCodes.isEmpty()) return Boolean.TRUE.equals(workerClassification);
        if (errorCode == null || errorCode.isBlank()) return false;
        return retryableErrorCodes.contains(errorCode.strip().toUpperCase(Locale.ROOT));
    }

    private static Duration requireNonNegative(Duration value, String field) {
        if (value == null) throw new IllegalArgumentException(field + " must not be null");
        if (value.isNegative()) throw new IllegalArgumentException(field + " must not be negative");
        return value;
    }

    private static void requireDurableMilliseconds(Duration value, String field) {
        try {
            value.toMillis();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(field + " must fit in signed millisecond precision", overflow);
        }
    }

    private static Set<String> normalizeErrorCodes(Set<String> values) {
        if (values == null || values.isEmpty()) return Set.of();
        if (values.size() > MAX_ERROR_CODES) {
            throw new IllegalArgumentException("retryableErrorCodes cannot contain more than 100 values");
        }
        return values.stream()
                .map(value -> {
                    if (value == null || value.isBlank()) {
                        throw new IllegalArgumentException("retryableErrorCodes must not contain blank values");
                    }
                    String normalized = value.strip().toUpperCase(Locale.ROOT);
                    if (normalized.length() > MAX_ERROR_CODE_LENGTH) {
                        throw new IllegalArgumentException("retryable error codes cannot exceed 100 characters");
                    }
                    return normalized;
                })
                .collect(Collectors.toUnmodifiableSet());
    }
}
