package io.flowforge.application.tenancy;

import io.flowforge.domain.tenancy.TenantId;

public interface TenantQuotaProvider {
    TenantQuota quotaFor(TenantId tenantId);
}
