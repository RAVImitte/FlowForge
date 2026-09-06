package io.flowforge.application.coordination;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface EphemeralPermitStore {
    boolean tryAcquire(
            CoordinationPermit permit,
            int limit,
            Instant now,
            Duration ttlPadding
    );

    boolean renew(CoordinationPermit permit, Instant now, Duration ttlPadding);

    boolean release(
            TenantId tenantId,
            String resourceKey,
            UUID token,
            Instant now,
            Duration ttlPadding
    );

    void replace(
            TenantId tenantId,
            String resourceKey,
            List<CoordinationPermit> permits,
            Instant now,
            Duration ttlPadding
    );

    long activeCount(TenantId tenantId, String resourceKey, Instant now);
}
