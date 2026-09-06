package io.flowforge.controlplane.config;

import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FlowForgeJwtAuthenticationConverterTest {
    private final FlowForgeJwtAuthenticationConverter converter =
            new FlowForgeJwtAuthenticationConverter("roles");

    @Test
    void mapsOnlyBoundedRolesAndPreservesScopes() {
        Jwt jwt = jwtBuilder()
                .claim("preferred_username", "alice")
                .claim("scope", "workflow.read custom")
                .claim("roles", List.of("viewer", "ROLE_OPERATOR", "SUPERUSER", 42))
                .build();

        var authentication = converter.convert(jwt);

        assertThat(authentication.getName()).isEqualTo("alice");
        assertThat(authentication.getAuthorities())
                .extracting("authority")
                .containsExactlyInAnyOrder(
                        "SCOPE_workflow.read", "SCOPE_custom", "ROLE_VIEWER", "ROLE_OPERATOR"
                );
    }

    @Test
    void supportsDelimitedRoleClaimsAndFallsBackToSubject() {
        Jwt jwt = jwtBuilder()
                .claim("roles", "admin admin, monitor")
                .build();

        var authentication = converter.convert(jwt);

        assertThat(authentication.getName()).isEqualTo("subject-123");
        assertThat(authentication.getAuthorities())
                .extracting("authority")
                .containsExactlyInAnyOrder("ROLE_ADMIN", "ROLE_MONITOR");
    }

    private static Jwt.Builder jwtBuilder() {
        Instant now = Instant.parse("2026-09-06T12:00:00Z");
        return Jwt.withTokenValue("test-token")
                .header("alg", "RS256")
                .subject("subject-123")
                .issuedAt(now)
                .expiresAt(now.plusSeconds(300));
    }
}
