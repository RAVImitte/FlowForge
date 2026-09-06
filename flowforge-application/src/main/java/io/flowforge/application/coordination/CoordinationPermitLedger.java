package io.flowforge.application.coordination;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface CoordinationPermitLedger {
    Optional<CoordinationPermit> tryAcquire(
            TenantId tenantId,
            String resourceKey,
            String holderId,
            UUID proposedToken,
            int limit,
            Instant now,
            Duration leaseDuration
    );

    Optional<CoordinationPermit> renew(
            TenantId tenantId,
            UUID token,
            Instant now,
            Duration leaseDuration
    );

    Optional<CoordinationPermit> release(TenantId tenantId, UUID token, Instant now);

    List<CoordinationPermit> findActive(TenantId tenantId, String resourceKey, Instant now);
}
