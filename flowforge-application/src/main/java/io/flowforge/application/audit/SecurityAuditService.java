package io.flowforge.application.audit;

import io.flowforge.domain.tenancy.TenantId;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class SecurityAuditService {
    private final SecurityAuditRepository repository;
    private final Clock clock;
    private final Duration onlineRetention;

    public SecurityAuditService(SecurityAuditRepository repository, Clock clock, Duration onlineRetention) {
        this.repository = repository;
        this.clock = clock;
        if (onlineRetention == null || onlineRetention.isNegative() || onlineRetention.isZero()) {
            throw new IllegalArgumentException("onlineRetention must be positive");
        }
        this.onlineRetention = onlineRetention;
    }

    public void record(
            TenantId tenantId,
            String actor,
            String action,
            String targetType,
            String targetId,
            AuditOutcome outcome,
            String httpMethod,
            String httpPath,
            Integer statusCode,
            UUID requestId
    ) {
        repository.append(new SecurityAuditEvent(
                UUID.randomUUID(), clock.instant(), tenantId, actor, action, targetType, targetId,
                outcome, httpMethod, httpPath, statusCode, requestId
        ));
    }

    public SecurityAuditPage history(TenantId tenantId, int page, int size) {
        if (page < 0) throw new IllegalArgumentException("page must not be negative");
        if (size < 1 || size > 200) throw new IllegalArgumentException("size must be between 1 and 200");
        Instant notBefore = clock.instant().minus(onlineRetention);
        return new SecurityAuditPage(
                repository.findByTenant(tenantId, notBefore, Math.multiplyExact(page, size), size),
                page,
                size,
                repository.countByTenant(tenantId, notBefore)
        );
    }
}
