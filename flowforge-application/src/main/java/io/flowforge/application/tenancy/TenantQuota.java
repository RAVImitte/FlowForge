package io.flowforge.application.tenancy;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.Objects;

public record TenantQuota(
        TenantId tenantId,
        long version,
        TenantQuotaPolicy policy,
        boolean configured,
        Instant createdAt,
        Instant updatedAt
) {
    public TenantQuota {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        if (version < 0) throw new IllegalArgumentException("version must not be negative");
        Objects.requireNonNull(policy, "policy must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (updatedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("updatedAt must not precede createdAt");
        }
        if (!configured && version != 0) {
            throw new IllegalArgumentException("an inherited quota must have version 0");
        }
    }

    public static TenantQuota inherited(TenantId tenantId, TenantQuotaPolicy defaults) {
        return new TenantQuota(tenantId, 0, defaults, false, Instant.EPOCH, Instant.EPOCH);
    }
}
