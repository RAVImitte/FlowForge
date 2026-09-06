package io.flowforge.controlplane.adapter.out.persistence;

import io.flowforge.application.audit.AuditOutcome;
import io.flowforge.application.audit.SecurityAuditEvent;
import io.flowforge.application.audit.SecurityAuditRepository;
import io.flowforge.domain.tenancy.TenantId;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
public class JdbcSecurityAuditRepository implements SecurityAuditRepository {
    private final JdbcClient jdbc;

    public JdbcSecurityAuditRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void append(SecurityAuditEvent event) {
        jdbc.sql("""
                INSERT INTO security_audit_event(
                    id, occurred_at, tenant_id, actor, action, target_type, target_id,
                    outcome, http_method, http_path, status_code, request_id
                ) VALUES (
                    :id, :occurredAt, :tenantId, :actor, :action, :targetType, :targetId,
                    :outcome, :httpMethod, :httpPath, :statusCode, :requestId
                )
                """)
                .param("id", event.id())
                .param("occurredAt", Timestamp.from(event.occurredAt()))
                .param("tenantId", event.tenantId().value())
                .param("actor", event.actor())
                .param("action", event.action())
                .param("targetType", event.targetType())
                .param("targetId", event.targetId())
                .param("outcome", event.outcome().name())
                .param("httpMethod", event.httpMethod())
                .param("httpPath", event.httpPath())
                .param("statusCode", event.statusCode())
                .param("requestId", event.requestId())
                .update();
    }

    @Override
    public List<SecurityAuditEvent> findByTenant(TenantId tenantId, Instant notBefore, int offset, int limit) {
        return jdbc.sql("""
                SELECT id, occurred_at, tenant_id, actor, action, target_type, target_id,
                       outcome, http_method, http_path, status_code, request_id
                  FROM security_audit_event
                 WHERE tenant_id = :tenantId
                   AND occurred_at >= :notBefore
                 ORDER BY occurred_at DESC, id DESC
                 OFFSET :offset LIMIT :limit
                """)
                .param("tenantId", tenantId.value())
                .param("notBefore", Timestamp.from(notBefore))
                .param("offset", offset)
                .param("limit", limit)
                .query((rs, rowNum) -> new SecurityAuditEvent(
                        rs.getObject("id", java.util.UUID.class),
                        rs.getTimestamp("occurred_at").toInstant(),
                        new TenantId(rs.getString("tenant_id")),
                        rs.getString("actor"),
                        rs.getString("action"),
                        rs.getString("target_type"),
                        rs.getString("target_id"),
                        AuditOutcome.valueOf(rs.getString("outcome")),
                        rs.getString("http_method"),
                        rs.getString("http_path"),
                        (Integer) rs.getObject("status_code"),
                        rs.getObject("request_id", java.util.UUID.class)
                ))
                .list();
    }

    @Override
    public long countByTenant(TenantId tenantId, Instant notBefore) {
        return jdbc.sql("""
                SELECT COUNT(*) FROM security_audit_event
                 WHERE tenant_id = :tenantId AND occurred_at >= :notBefore
                """)
                .param("tenantId", tenantId.value())
                .param("notBefore", Timestamp.from(notBefore))
                .query(Long.class)
                .single();
    }
}
