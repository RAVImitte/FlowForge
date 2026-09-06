package io.flowforge.application.tenancy;

import io.flowforge.domain.tenancy.TenantId;

public final class TenantQuotaConflictException extends RuntimeException {
    public TenantQuotaConflictException(TenantId tenantId, long expectedVersion) {
        super("Tenant quota " + tenantId.value() + " is not at version " + expectedVersion);
    }
}
