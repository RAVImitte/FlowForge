package io.flowforge.application.execution;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface DurableTaskQueue {
    List<TenantId> readyTenants(Instant now, int limit);

    default ReadyQueueSnapshot readyQueue(TenantId tenantId, Instant now, int requestedLimit) {
        return ReadyQueueSnapshot.unknown(requestedLimit);
    }

    int enqueueReadyTasks(TenantId tenantId, int limit, Instant now);

    int enqueueReadyTasks(TenantId tenantId, UUID workflowExecutionId, int limit, Instant now);

    int releaseDueRetries(int limit, Instant now);
}
