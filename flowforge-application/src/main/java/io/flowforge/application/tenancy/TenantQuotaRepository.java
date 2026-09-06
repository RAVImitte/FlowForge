package io.flowforge.application.tenancy;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.Optional;

public interface TenantQuotaRepository {
    Optional<TenantQuota> find(TenantId tenantId);

    TenantQuota save(TenantId tenantId, TenantQuotaPolicy policy, long expectedVersion, Instant now);
}
