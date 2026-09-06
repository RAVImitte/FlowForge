package io.flowforge.application.tenancy;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Clock;
import java.util.Objects;

public final class TenantQuotaService {
    private final TenantQuotaRepository repository;
    private final TenantQuotaProvider provider;
    private final Clock clock;

    public TenantQuotaService(
            TenantQuotaRepository repository,
            TenantQuotaProvider provider,
            Clock clock
    ) {
        this.repository = Objects.requireNonNull(repository);
        this.provider = Objects.requireNonNull(provider);
        this.clock = Objects.requireNonNull(clock);
    }

    public TenantQuota get(TenantId tenantId) {
        return provider.quotaFor(Objects.requireNonNull(tenantId));
    }

    public TenantQuota update(TenantId tenantId, TenantQuotaPolicy policy, long expectedVersion) {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        Objects.requireNonNull(policy, "policy must not be null");
        if (expectedVersion < 0) {
            throw new IllegalArgumentException("expectedVersion must not be negative");
        }
        return repository.save(tenantId, policy, expectedVersion, clock.instant());
    }
}
