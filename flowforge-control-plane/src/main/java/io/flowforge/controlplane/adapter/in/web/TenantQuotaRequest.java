package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.tenancy.TenantQuotaPolicy;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public record TenantQuotaRequest(
        @Min(1) @Max(1_000_000) int maxActiveExecutions,
        @Min(1) @Max(1_000_000) int maxRunningTasks,
        @Min(1) @Max(1_000_000) int maxPendingScheduleFires,
        @Min(1) @Max(1_000_000) int maxReadyTasks,
        @NotNull @Valid RateLimitRequest scheduleRateLimit,
        @NotNull @Valid RateLimitRequest dispatchRateLimit
) {
    TenantQuotaPolicy toPolicy() {
        return new TenantQuotaPolicy(
                maxActiveExecutions,
                maxRunningTasks,
                maxPendingScheduleFires,
                maxReadyTasks,
                scheduleRateLimit.toPolicy(),
                dispatchRateLimit.toPolicy()
        );
    }

    public record RateLimitRequest(
            @Min(1) @Max(1_000_000) int capacity,
            @Min(1) @Max(1_000_000) int refillTokens,
            @Min(1) @Max(86_400_000) long refillPeriodMillis
    ) {
        TokenBucketPolicy toPolicy() {
            return new TokenBucketPolicy(capacity, refillTokens, Duration.ofMillis(refillPeriodMillis));
        }
    }
}
