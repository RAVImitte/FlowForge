package io.flowforge.controlplane.adapter.in.web;

import io.flowforge.application.audit.SecurityAuditEvent;
import io.flowforge.application.audit.SecurityAuditPage;
import io.flowforge.application.audit.SecurityAuditService;
import io.flowforge.controlplane.config.TenantContextFilter;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/audit-events")
public class SecurityAuditController {
    private final SecurityAuditService audit;

    public SecurityAuditController(SecurityAuditService audit) {
        this.audit = audit;
    }

    @GetMapping
    AuditPageResponse history(
            HttpServletRequest request,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size
    ) {
        return AuditPageResponse.from(audit.history(TenantContextFilter.requireTenant(request), page, size));
    }

    record AuditPageResponse(List<AuditEventResponse> items, int page, int size, long totalElements) {
        static AuditPageResponse from(SecurityAuditPage page) {
            return new AuditPageResponse(
                    page.items().stream().map(AuditEventResponse::from).toList(),
                    page.page(), page.size(), page.totalElements()
            );
        }
    }

    record AuditEventResponse(
            UUID id,
            Instant occurredAt,
            String tenantId,
            String actor,
            String action,
            String targetType,
            String targetId,
            String outcome,
            String httpMethod,
            String httpPath,
            Integer statusCode,
            UUID requestId
    ) {
        static AuditEventResponse from(SecurityAuditEvent event) {
            return new AuditEventResponse(
                    event.id(), event.occurredAt(), event.tenantId().value(), event.actor(), event.action(),
                    event.targetType(), event.targetId(), event.outcome().name(), event.httpMethod(),
                    event.httpPath(), event.statusCode(), event.requestId()
            );
        }
    }
}
