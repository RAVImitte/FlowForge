package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.tenancy.TenantQuota;

import java.time.Instant;

public record TenantQuotaResponse(
        String tenantId,
        long version,
        boolean configured,
        int maxActiveExecutions,
        int maxRunningTasks,
        int maxPendingScheduleFires,
        int maxReadyTasks,
        RateLimitResponse scheduleRateLimit,
        RateLimitResponse dispatchRateLimit,
        Instant createdAt,
        Instant updatedAt
) {
    static TenantQuotaResponse from(TenantQuota quota) {
        var policy = quota.policy();
        return new TenantQuotaResponse(
                quota.tenantId().value(), quota.version(), quota.configured(),
                policy.maxActiveExecutions(), policy.maxRunningTasks(),
                policy.maxPendingScheduleFires(), policy.maxReadyTasks(),
                RateLimitResponse.from(policy.scheduleRateLimit()),
                RateLimitResponse.from(policy.dispatchRateLimit()),
                quota.createdAt(), quota.updatedAt()
        );
    }

    public record RateLimitResponse(int capacity, int refillTokens, long refillPeriodMillis) {
        static RateLimitResponse from(TokenBucketPolicy policy) {
            return new RateLimitResponse(
                    policy.capacity(), policy.refillTokens(), policy.refillPeriod().toMillis()
            );
        }
    }
}
