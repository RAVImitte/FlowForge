package io.flowforge.controlplane.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;

@ConfigurationProperties("flowforge.security")
public class FlowForgeSecurityProperties {
    private boolean enabled;
    private String issuerUri;
    private String jwkSetUri;
    private String rolesClaim = "roles";
    private String tenantIdClaim = "tenant_id";
    private String localTenantId = "local";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getIssuerUri() {
        return issuerUri;
    }

    public void setIssuerUri(String issuerUri) {
        this.issuerUri = issuerUri;
    }

    public String getJwkSetUri() {
        return jwkSetUri;
    }

    public void setJwkSetUri(String jwkSetUri) {
        this.jwkSetUri = jwkSetUri;
    }

    public String getRolesClaim() {
        return rolesClaim;
    }

    public void setRolesClaim(String rolesClaim) {
        this.rolesClaim = rolesClaim;
    }

    public String getTenantIdClaim() {
        return tenantIdClaim;
    }

    public void setTenantIdClaim(String tenantIdClaim) {
        this.tenantIdClaim = tenantIdClaim;
    }

    public String getLocalTenantId() {
        return localTenantId;
    }

    public void setLocalTenantId(String localTenantId) {
        this.localTenantId = localTenantId;
    }

    String requiredIssuerUri() {
        return requiredAbsoluteUri(issuerUri, "FLOWFORGE_OIDC_ISSUER_URI");
    }

    String requiredJwkSetUri() {
        return requiredAbsoluteUri(jwkSetUri, "FLOWFORGE_OIDC_JWK_SET_URI");
    }

    private static String requiredAbsoluteUri(String value, String environmentVariable) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(environmentVariable
                    + " is required when FlowForge security is enabled");
        }
        URI issuer;
        try {
            issuer = URI.create(value.strip());
        } catch (IllegalArgumentException exception) {
            throw new IllegalStateException(environmentVariable + " must be a valid URI", exception);
        }
        if (!issuer.isAbsolute() || issuer.getHost() == null) {
            throw new IllegalStateException(environmentVariable + " must be an absolute URI");
        }
        return issuer.toString();
    }

    String normalizedRolesClaim() {
        if (rolesClaim == null || rolesClaim.isBlank()) {
            throw new IllegalStateException("FlowForge JWT roles claim must not be blank");
        }
        return rolesClaim.strip();
    }

    String normalizedTenantIdClaim() {
        if (tenantIdClaim == null || tenantIdClaim.isBlank()) {
            throw new IllegalStateException("FlowForge JWT tenant ID claim must not be blank");
        }
        return tenantIdClaim.strip();
    }
}
