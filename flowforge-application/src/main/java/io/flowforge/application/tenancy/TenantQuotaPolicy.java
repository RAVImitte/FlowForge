package io.flowforge.application.tenancy;

import io.flowforge.application.coordination.TokenBucketPolicy;

import java.util.Objects;

public record TenantQuotaPolicy(
        int maxActiveExecutions,
        int maxRunningTasks,
        int maxPendingScheduleFires,
        int maxReadyTasks,
        TokenBucketPolicy scheduleRateLimit,
        TokenBucketPolicy dispatchRateLimit
) {
    public TenantQuotaPolicy {
        bounded(maxActiveExecutions, "maxActiveExecutions");
        bounded(maxRunningTasks, "maxRunningTasks");
        bounded(maxPendingScheduleFires, "maxPendingScheduleFires");
        bounded(maxReadyTasks, "maxReadyTasks");
        Objects.requireNonNull(scheduleRateLimit, "scheduleRateLimit must not be null");
        Objects.requireNonNull(dispatchRateLimit, "dispatchRateLimit must not be null");
    }

    private static void bounded(int value, String name) {
        if (value < 1 || value > 1_000_000) {
            throw new IllegalArgumentException(name + " must be between 1 and 1000000");
        }
    }
}
