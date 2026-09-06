package io.flowforge.controlplane.config;

import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

final class FlowForgeJwtAuthenticationConverter implements Converter<Jwt, JwtAuthenticationToken> {
    private static final Set<String> ROLES = Set.of("VIEWER", "OPERATOR", "ADMIN", "MONITOR");

    private final String rolesClaim;
    private final JwtGrantedAuthoritiesConverter scopeConverter = new JwtGrantedAuthoritiesConverter();

    FlowForgeJwtAuthenticationConverter(String rolesClaim) {
        this.rolesClaim = rolesClaim;
    }

    @Override
    public JwtAuthenticationToken convert(Jwt jwt) {
        Set<GrantedAuthority> authorities = new LinkedHashSet<>();
        Collection<GrantedAuthority> scopes = scopeConverter.convert(jwt);
        if (scopes != null) {
            authorities.addAll(scopes);
        }
        roleValues(jwt.getClaim(rolesClaim)).stream()
                .map(FlowForgeJwtAuthenticationConverter::normalizeRole)
                .filter(ROLES::contains)
                .map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                .forEach(authorities::add);

        String principal = jwt.getClaimAsString("preferred_username");
        if (principal == null || principal.isBlank()) {
            principal = jwt.getSubject();
        }
        return new JwtAuthenticationToken(jwt, authorities, principal);
    }

    private static Collection<?> roleValues(Object claim) {
        if (claim instanceof Collection<?> values) {
            return values;
        }
        if (claim instanceof String value) {
            return List.of(value.split("[ ,]+"));
        }
        return Set.of();
    }

    private static String normalizeRole(Object role) {
        if (!(role instanceof String value)) {
            return "";
        }
        String normalized = value.strip().toUpperCase(Locale.ROOT);
        return normalized.startsWith("ROLE_") ? normalized.substring(5) : normalized;
    }
}
