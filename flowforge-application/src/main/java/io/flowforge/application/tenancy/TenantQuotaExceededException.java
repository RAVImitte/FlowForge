package io.flowforge.application.tenancy;

import io.flowforge.domain.tenancy.TenantId;

public final class TenantQuotaExceededException extends RuntimeException {
    private final TenantId tenantId;
    private final String quota;
    private final int limit;

    public TenantQuotaExceededException(TenantId tenantId, String quota, int limit) {
        super("Tenant " + tenantId.value() + " exceeded quota " + quota + " (limit " + limit + ")");
        this.tenantId = tenantId;
        this.quota = quota;
        this.limit = limit;
    }

    public TenantId tenantId() {
        return tenantId;
    }

    public String quota() {
        return quota;
    }

    public int limit() {
        return limit;
    }
}
