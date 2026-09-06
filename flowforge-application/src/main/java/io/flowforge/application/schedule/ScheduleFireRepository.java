package io.flowforge.application.schedule;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface ScheduleFireRepository {
    ScheduleMaterializationResult materializeDue(
            int limit,
            int maxPending,
            Instant now,
            Duration misfireThreshold
    );

    default ScheduleMaterializationResult materializeDue(
            int limit,
            Instant now,
            Duration misfireThreshold
    ) {
        return materializeDue(limit, Integer.MAX_VALUE, now, misfireThreshold);
    }

    List<TenantId> pendingTenants(Instant now, int limit);

    QueueSnapshot pendingQueue(TenantId tenantId, Instant now);

    List<ClaimedScheduleFire> claimPending(
            TenantId tenantId,
            int limit,
            String claimant,
            Instant now,
            Duration leaseDuration
    );

    boolean markStarted(
            TenantId tenantId,
            UUID triggerId,
            UUID claimToken,
            UUID workflowExecutionId,
            Instant now
    );

    boolean markFailed(
            TenantId tenantId,
            UUID triggerId,
            UUID claimToken,
            String errorMessage,
            Instant now
    );

    boolean release(
            TenantId tenantId,
            UUID triggerId,
            UUID claimToken,
            String errorMessage,
            Instant availableAt
    );
}
