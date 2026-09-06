package io.flowforge.application.audit;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Instant;
import java.util.List;

public interface SecurityAuditRepository {
    void append(SecurityAuditEvent event);

    List<SecurityAuditEvent> findByTenant(TenantId tenantId, Instant notBefore, int offset, int limit);

    long countByTenant(TenantId tenantId, Instant notBefore);
}
