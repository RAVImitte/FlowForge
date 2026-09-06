package io.flowforge.controlplane.config;

import io.flowforge.application.audit.AuditOutcome;
import io.flowforge.application.audit.SecurityAuditService;
import io.flowforge.domain.tenancy.TenantId;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Locale;
import java.util.UUID;

public final class AdministrativeAuditFilter extends OncePerRequestFilter {
    public static final String REQUEST_ID_HEADER = "X-Request-Id";
    private static final Logger LOGGER = LoggerFactory.getLogger(AdministrativeAuditFilter.class);
    private final SecurityAuditService audit;
    private final ObjectMapper objectMapper;

    public AdministrativeAuditFilter(SecurityAuditService audit, ObjectMapper objectMapper) {
        this.audit = audit;
        this.objectMapper = objectMapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String method = request.getMethod();
        String apiPrefix = request.getContextPath() + "/api/v1/";
        boolean mutation = method.equals("POST") || method.equals("PUT")
                || method.equals("PATCH") || method.equals("DELETE");
        boolean deadLetterInspection = method.equals("GET")
                && request.getRequestURI().startsWith(apiPrefix + "dead-letters/");
        return !request.getRequestURI().startsWith(apiPrefix) || (!mutation && !deadLetterInspection);
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain
    ) throws ServletException, IOException {
        TenantId tenantId = TenantContextFilter.requireTenant(request);
        UUID requestId = requestId(request.getHeader(REQUEST_ID_HEADER));
        response.setHeader(REQUEST_ID_HEADER, requestId.toString());
        AuditTarget target = AuditTarget.from(request, tenantId);
        String actor = actor();

        try {
            record(tenantId, actor, target, AuditOutcome.ATTEMPTED, request, null, requestId);
        } catch (RuntimeException unavailable) {
            LOGGER.error("Security audit storage is unavailable; mutation {} rejected", requestId);
            SecurityProblemWriter.write(
                    objectMapper, request, response, 503, "Service Unavailable", "AUDIT_UNAVAILABLE",
                    "Security audit storage is unavailable"
            );
            return;
        }

        try {
            chain.doFilter(request, response);
            AuditOutcome outcome = response.getStatus() < 400
                    ? AuditOutcome.SUCCEEDED
                    : response.getStatus() == 401 || response.getStatus() == 403
                            ? AuditOutcome.DENIED
                            : AuditOutcome.FAILED;
            recordTerminal(tenantId, actor, target, outcome, request, response.getStatus(), requestId);
        } catch (IOException | ServletException | RuntimeException failure) {
            recordTerminal(tenantId, actor, target, AuditOutcome.FAILED, request, 500, requestId);
            throw failure;
        }
    }

    private void recordTerminal(
            TenantId tenantId,
            String actor,
            AuditTarget target,
            AuditOutcome outcome,
            HttpServletRequest request,
            int status,
            UUID requestId
    ) {
        try {
            record(tenantId, actor, target, outcome, request, status, requestId);
        } catch (RuntimeException unavailable) {
            LOGGER.error("Could not append terminal security audit event for request {}", requestId);
        }
    }

    private void record(
            TenantId tenantId,
            String actor,
            AuditTarget target,
            AuditOutcome outcome,
            HttpServletRequest request,
            Integer status,
            UUID requestId
    ) {
        audit.record(tenantId, actor, target.action(), target.type(), target.id(), outcome,
                request.getMethod(), request.getRequestURI(), status, requestId);
    }

    private static String actor() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        return authentication == null || !authentication.isAuthenticated()
                ? "local-development"
                : authentication.getName();
    }

    private static UUID requestId(String value) {
        try {
            return value == null ? UUID.randomUUID() : UUID.fromString(value);
        } catch (IllegalArgumentException ignored) {
            return UUID.randomUUID();
        }
    }

    private record AuditTarget(String action, String type, String id) {
        static AuditTarget from(HttpServletRequest request, TenantId tenantId) {
            String relative = request.getRequestURI().substring(
                    (request.getContextPath() + "/api/v1/").length()
            );
            String[] segments = relative.split("/");
            String resource = segments.length == 0 || segments[0].isBlank() ? "api" : segments[0];
            if (resource.equals("dead-letters")) {
                boolean replay = segments.length > 0 && segments[segments.length - 1].equals("replay");
                String id = segments.length > 1
                        ? String.join("/", java.util.Arrays.copyOfRange(segments, 1, segments.length - (replay ? 1 : 0)))
                        : null;
                return new AuditTarget(
                        request.getMethod() + "_DEAD_LETTER_" + (replay ? "REPLAY" : "INSPECT"),
                        "TRANSPORT_DLQ_RECORD",
                        id
                );
            }
            String type = resource.equals("tenant") && segments.length > 1
                    ? "tenant_" + segments[1]
                    : resource.endsWith("s") ? resource.substring(0, resource.length() - 1) : resource;
            String id = resource.equals("tenant") ? tenantId.value() : segments.length > 1 ? segments[1] : null;
            StringBuilder action = new StringBuilder(request.getMethod()).append('_').append(resource);
            if (segments.length > 2) action.append('_').append(segments[2]);
            return new AuditTarget(
                    action.toString().replaceAll("[^A-Za-z0-9]+", "_").toUpperCase(Locale.ROOT),
                    type.replaceAll("[^A-Za-z0-9]+", "_").toUpperCase(Locale.ROOT),
                    id
            );
        }
    }
}
