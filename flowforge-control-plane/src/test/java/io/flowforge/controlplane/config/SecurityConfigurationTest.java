package io.flowforge.controlplane.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mock.web.MockServletContext;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ResponseStatus;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class SecurityConfigurationTest {
    private AnnotationConfigWebApplicationContext context;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        context = new AnnotationConfigWebApplicationContext();
        context.setServletContext(new MockServletContext());
        TestPropertyValues.of(
                "flowforge.security.enabled=true",
                "flowforge.security.issuer-uri=https://identity.example.test/realms/flowforge",
                "flowforge.security.jwk-set-uri=https://identity.example.test/realms/flowforge/jwks",
                "flowforge.security.roles-claim=roles"
        ).applyTo(context);
        context.register(
                WebConfiguration.class,
                SecurityConfiguration.class,
                TestDependencies.class,
                SecurityProbeController.class
        );
        context.refresh();
        mvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(springSecurity())
                .build();
    }

    @AfterEach
    void tearDown() {
        context.close();
    }

    @Test
    void keepsHealthProbesPublic() throws Exception {
        mvc.perform(get("/actuator/health/readiness"))
                .andExpect(status().isOk())
                .andExpect(content().string("ready"));
    }

    @Test
    void rejectsAnonymousApiRequestsWithAStableProblem() throws Exception {
        mvc.perform(get("/api/v1/workflows"))
                .andExpect(status().isUnauthorized())
                .andExpect(header().string("WWW-Authenticate", "Bearer"))
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(jsonPath("$.code").value("AUTHENTICATION_REQUIRED"));
    }

    @Test
    void rejectsAuthenticatedRequestsWithoutAValidTenantClaim() throws Exception {
        mvc.perform(get("/api/v1/workflows")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_VIEWER"))))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("TENANT_REQUIRED"));

        mvc.perform(get("/api/v1/workflows")
                        .with(jwt()
                                .jwt(token -> token.claim("tenant_id", "Unsafe_Tenant"))
                                .authorities(new SimpleGrantedAuthority("ROLE_VIEWER"))))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("INVALID_TENANT"));
    }

    @Test
    void derivesTenantOnlyFromTheJwtAndIgnoresSpoofedHeaders() throws Exception {
        mvc.perform(get("/api/v1/tenant")
                        .header("X-Tenant-Id", "attacker")
                        .with(role("VIEWER")))
                .andExpect(status().isOk())
                .andExpect(content().string("orders-eu"));
    }

    @Test
    void allowsViewersToReadButNotOperate() throws Exception {
        mvc.perform(get("/api/v1/workflows").with(role("VIEWER")))
                .andExpect(status().isOk());

        mvc.perform(post("/api/v1/workflows/123/executions").with(role("VIEWER")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.code").value("ACCESS_DENIED"));
    }

    @Test
    void restrictsTenantAuditHistoryToAdministrators() throws Exception {
        mvc.perform(get("/api/v1/audit-events").with(role("VIEWER")))
                .andExpect(status().isForbidden());
        mvc.perform(get("/api/v1/audit-events").with(role("ADMIN")))
                .andExpect(status().isOk())
                .andExpect(content().string("audit"));
    }

    @Test
    void restrictsDeadLetterInspectionAndReplayToAdministrators() throws Exception {
        String record = "/api/v1/dead-letters/flowforge.task-results.dlq/partitions/0/offsets/42";

        mvc.perform(get(record).with(role("VIEWER")))
                .andExpect(status().isForbidden());
        mvc.perform(post(record + "/replay").with(role("OPERATOR")))
                .andExpect(status().isForbidden());
        mvc.perform(get(record).with(role("ADMIN")))
                .andExpect(status().isOk())
                .andExpect(content().string("dead-letter"));
        mvc.perform(post(record + "/replay").with(role("ADMIN")))
                .andExpect(status().isOk())
                .andExpect(content().string("replayed"));
    }

    @Test
    void allowsOperatorsToExecuteButNotAdministerDefinitions() throws Exception {
        mvc.perform(post("/api/v1/workflows/123/executions").with(role("OPERATOR")))
                .andExpect(status().isAccepted());

        mvc.perform(post("/api/v1/workflows").with(role("OPERATOR")))
                .andExpect(status().isForbidden());
    }

    @Test
    void allowsAdministratorsToMutateDefinitions() throws Exception {
        mvc.perform(post("/api/v1/workflows").with(role("ADMIN")))
                .andExpect(status().isCreated());
    }

    @Test
    void restrictsMetricsToMonitoringAndAdministrativeRoles() throws Exception {
        mvc.perform(get("/actuator/prometheus").with(role("VIEWER")))
                .andExpect(status().isForbidden());
        mvc.perform(get("/actuator/prometheus").with(role("MONITOR")))
                .andExpect(status().isOk());
        mvc.perform(get("/actuator/prometheus").with(role("ADMIN")))
                .andExpect(status().isOk());
    }

    private static org.springframework.test.web.servlet.request.RequestPostProcessor role(String role) {
        return jwt()
                .jwt(token -> token.claim("tenant_id", "orders-eu"))
                .authorities(new SimpleGrantedAuthority("ROLE_" + role));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableWebMvc
    static class WebConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    static class TestDependencies {
        @Bean
        ObjectMapper objectMapper() {
            return JsonMapper.builder().build();
        }
    }

    @RestController
    static class SecurityProbeController {
        @GetMapping("/actuator/health/readiness")
        String health() {
            return "ready";
        }

        @GetMapping("/actuator/prometheus")
        String metrics() {
            return "metrics";
        }

        @GetMapping("/api/v1/workflows")
        String workflows() {
            return "workflows";
        }

        @GetMapping("/api/v1/tenant")
        String tenant(HttpServletRequest request) {
            return TenantContextFilter.requireTenant(request).value();
        }

        @GetMapping("/api/v1/audit-events")
        String audit() {
            return "audit";
        }

        @GetMapping("/api/v1/dead-letters/{topic}/partitions/{partition}/offsets/{offset}")
        String deadLetter() {
            return "dead-letter";
        }

        @PostMapping("/api/v1/dead-letters/{topic}/partitions/{partition}/offsets/{offset}/replay")
        String replayDeadLetter() {
            return "replayed";
        }

        @PostMapping("/api/v1/workflows/{id}/executions")
        @ResponseStatus(HttpStatus.ACCEPTED)
        String execute() {
            return "accepted";
        }

        @PostMapping("/api/v1/workflows")
        @ResponseStatus(HttpStatus.CREATED)
        String create() {
            return "created";
        }
    }
}
