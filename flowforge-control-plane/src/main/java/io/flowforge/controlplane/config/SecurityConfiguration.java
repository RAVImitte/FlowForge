package io.flowforge.controlplane.config;

import io.flowforge.application.audit.SecurityAuditService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.context.NullSecurityContextRepository;
import org.springframework.security.web.savedrequest.NullRequestCache;
import tools.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
@EnableWebSecurity
@EnableConfigurationProperties(FlowForgeSecurityProperties.class)
public class SecurityConfiguration {
    private static final String VIEWER = "VIEWER";
    private static final String OPERATOR = "OPERATOR";
    private static final String ADMIN = "ADMIN";
    private static final String MONITOR = "MONITOR";

    @Bean
    @ConditionalOnProperty(name = "flowforge.security.enabled", havingValue = "false", matchIfMissing = true)
    SecurityFilterChain localDevelopmentSecurity(
            HttpSecurity http,
            TenantContextFilter tenantContextFilter,
            ObjectProvider<AdministrativeAuditFilter> auditFilter
    ) throws Exception {
        HttpSecurity configured = stateless(http)
                .authorizeHttpRequests(authorize -> authorize.anyRequest().permitAll())
                .addFilterAfter(tenantContextFilter, BearerTokenAuthenticationFilter.class);
        auditFilter.ifAvailable(filter -> configured.addFilterAfter(filter, TenantContextFilter.class));
        return configured.build();
    }

    @Bean
    TenantContextFilter tenantContextFilter(
            FlowForgeSecurityProperties properties,
            ObjectMapper objectMapper
    ) {
        return new TenantContextFilter(properties, objectMapper);
    }

    @Bean
    @ConditionalOnBean(SecurityAuditService.class)
    AdministrativeAuditFilter administrativeAuditFilter(SecurityAuditService audit, ObjectMapper objectMapper) {
        return new AdministrativeAuditFilter(audit, objectMapper);
    }

    @Bean
    @ConditionalOnProperty(name = "flowforge.security.enabled", havingValue = "true")
    FlowForgeJwtAuthenticationConverter flowForgeJwtAuthenticationConverter(
            FlowForgeSecurityProperties properties
    ) {
        return new FlowForgeJwtAuthenticationConverter(properties.normalizedRolesClaim());
    }

    @Bean
    @ConditionalOnProperty(name = "flowforge.security.enabled", havingValue = "true")
    JwtDecoder flowForgeJwtDecoder(FlowForgeSecurityProperties properties) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(properties.requiredJwkSetUri()).build();
        decoder.setJwtValidator(JwtValidators.createDefaultWithIssuer(properties.requiredIssuerUri()));
        return decoder;
    }

    @Bean
    @ConditionalOnProperty(name = "flowforge.security.enabled", havingValue = "true")
    SecurityFilterChain productionSecurity(
            HttpSecurity http,
            FlowForgeJwtAuthenticationConverter authenticationConverter,
            TenantContextFilter tenantContextFilter,
            ObjectMapper objectMapper,
            ObjectProvider<AdministrativeAuditFilter> auditFilter
    ) throws Exception {
        HttpSecurity configured = stateless(http)
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers("/actuator/health", "/actuator/health/**", "/actuator/info").permitAll()
                        .requestMatchers("/actuator/prometheus").hasAnyRole(MONITOR, ADMIN)
                        .requestMatchers(HttpMethod.GET, "/api/v1/audit-events", "/api/v1/audit-events/**")
                                .hasRole(ADMIN)
                        .requestMatchers("/api/v1/dead-letters/**").hasRole(ADMIN)
                        .requestMatchers(HttpMethod.GET, "/api/v1/**").hasAnyRole(VIEWER, OPERATOR, ADMIN)
                        .requestMatchers(
                                HttpMethod.POST,
                                "/api/v1/workflows/*/executions",
                                "/api/v1/executions/*/cancel",
                                "/api/v1/schedules/*/pause",
                                "/api/v1/schedules/*/resume"
                        ).hasAnyRole(OPERATOR, ADMIN)
                        .requestMatchers("/api/v1/**", "/actuator/**").hasRole(ADMIN)
                        .anyRequest().denyAll())
                .oauth2ResourceServer(resourceServer -> resourceServer
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(authenticationConverter))
                        .authenticationEntryPoint((request, response, exception) -> {
                            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
                            SecurityProblemWriter.write(objectMapper, request, response, 401,
                                    "Unauthorized", "AUTHENTICATION_REQUIRED",
                                    "A valid bearer token is required");
                        })
                        .accessDeniedHandler((request, response, exception) -> SecurityProblemWriter.write(
                                objectMapper, request, response, 403,
                                "Forbidden", "ACCESS_DENIED",
                                "The authenticated principal is not authorized for this operation"
                        )))
                .addFilterAfter(tenantContextFilter, BearerTokenAuthenticationFilter.class);
        auditFilter.ifAvailable(filter -> configured.addFilterAfter(filter, TenantContextFilter.class));
        return configured.build();
    }

    private static HttpSecurity stateless(HttpSecurity http) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .formLogin(form -> form.disable())
                .httpBasic(basic -> basic.disable())
                .logout(logout -> logout.disable())
                .requestCache(cache -> cache.requestCache(new NullRequestCache()))
                .securityContext(context -> context.securityContextRepository(
                        new NullSecurityContextRepository()
                ))
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
    }

}
