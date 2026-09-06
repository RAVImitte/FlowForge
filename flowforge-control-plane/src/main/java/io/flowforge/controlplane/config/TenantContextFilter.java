package io.flowforge.controlplane.config;

import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.observability.LogContext;
import io.flowforge.observability.LogFields;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Optional;

public final class TenantContextFilter extends OncePerRequestFilter {
    private static final String REQUEST_ATTRIBUTE = TenantContextFilter.class.getName() + ".tenantId";

    private final FlowForgeSecurityProperties properties;
    private final ObjectMapper objectMapper;

    public TenantContextFilter(FlowForgeSecurityProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public static TenantId requireTenant(HttpServletRequest request) {
        Object value = request.getAttribute(REQUEST_ATTRIBUTE);
        if (value instanceof TenantId tenantId) {
            return tenantId;
        }
        throw new IllegalStateException("No tenant identity is associated with this request");
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith(request.getContextPath() + "/api/v1/")
                && !request.getRequestURI().equals(request.getContextPath() + "/api/v1");
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        Optional<String> rawTenant = tenantClaim();
        if (properties.isEnabled() && rawTenant.isEmpty()) {
            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            if (authentication == null || !authentication.isAuthenticated()) {
                filterChain.doFilter(request, response);
                return;
            }
            SecurityProblemWriter.write(objectMapper, request, response, 403,
                    "Forbidden", "TENANT_REQUIRED",
                    "The bearer token does not contain a tenant identity");
            return;
        }

        String value = rawTenant.orElse(properties.getLocalTenantId());
        TenantId tenantId;
        try {
            tenantId = new TenantId(value);
        } catch (IllegalArgumentException exception) {
            SecurityProblemWriter.write(objectMapper, request, response, 403,
                    "Forbidden", "INVALID_TENANT",
                    "The tenant identity in the bearer token is invalid");
            return;
        }

        request.setAttribute(REQUEST_ATTRIBUTE, tenantId);
        try (LogContext ignored = LogContext.open(LogFields.TENANT_ID, tenantId.value())) {
            filterChain.doFilter(request, response);
        } finally {
            request.removeAttribute(REQUEST_ATTRIBUTE);
        }
    }

    private Optional<String> tenantClaim() {
        if (!properties.isEnabled()) {
            return Optional.empty();
        }
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (!(authentication instanceof JwtAuthenticationToken jwtAuthentication)) {
            return Optional.empty();
        }
        Object claim = jwtAuthentication.getToken().getClaim(properties.normalizedTenantIdClaim());
        return claim instanceof String value && !value.isBlank()
                ? Optional.of(value)
                : Optional.empty();
    }
}
