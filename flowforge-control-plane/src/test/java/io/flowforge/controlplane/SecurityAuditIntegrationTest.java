package io.flowforge.controlplane;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "flowforge.kafka.enabled=false",
        "flowforge.coordination.enabled=false",
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.outbox.command-dispatch-enabled=false",
        "flowforge.scheduling.trigger-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class SecurityAuditIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired WebApplicationContext context;
    @Autowired JdbcClient jdbc;
    MockMvc mvc;

    @BeforeEach
    void configureMvc() {
        mvc = MockMvcBuilders.webAppContextSetup(context)
                .apply(org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity())
                .build();
    }

    @Test
    void persistsReferencesAndAppendOnlyAttemptAndOutcomeWithoutRequestBodies() throws Exception {
        String workflow = """
                {
                  "name": "Payment workflow",
                  "tasks": [{
                    "key": "PAY",
                    "name": "Pay",
                    "type": "PAYMENT",
                    "configuration": {"endpoint": "https://payments.test"},
                    "secretReferences": {
                      "apiKey": {"provider": "vault", "name": "tenants/local/payment", "version": "9"}
                    }
                  }],
                  "dependencies": []
                }
                """;

        String requestId = mvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(workflow))
                .andExpect(status().isCreated())
                .andExpect(header().exists("X-Request-Id"))
                .andExpect(jsonPath("$.tasks[0].secretReferences.apiKey.provider").value("vault"))
                .andReturn().getResponse().getHeader("X-Request-Id");

        assertThat(jdbc.sql("SELECT secret_references::text FROM workflow_task")
                .query(String.class).single()).contains("tenants/local/payment", "vault");
        assertThat(jdbc.sql("SELECT configuration::text FROM workflow_task")
                .query(String.class).single()).doesNotContain("apiKey");
        List<String> outcomes = jdbc.sql("""
                SELECT outcome FROM security_audit_event
                 WHERE request_id = :requestId ORDER BY occurred_at, outcome
                """)
                .param("requestId", UUID.fromString(requestId))
                .query(String.class).list();
        assertThat(outcomes).containsExactlyInAnyOrder("ATTEMPTED", "SUCCEEDED");
        assertThat(jdbc.sql("""
                SELECT COUNT(*) FROM security_audit_event
                 WHERE request_id = :requestId
                   AND tenant_id = 'local'
                   AND actor = 'local-development'
                   AND action = 'POST_WORKFLOWS'
                   AND target_type = 'WORKFLOW'
                """).param("requestId", UUID.fromString(requestId)).query(Long.class).single()).isEqualTo(2);
        mvc.perform(get("/api/v1/audit-events").param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(org.hamcrest.Matchers.greaterThanOrEqualTo(2)))
                .andExpect(jsonPath("$.items[0].tenantId").value("local"));

        UUID eventId = jdbc.sql("SELECT id FROM security_audit_event LIMIT 1")
                .query(UUID.class).single();
        assertThatThrownBy(() -> jdbc.sql("UPDATE security_audit_event SET actor = 'changed' WHERE id = :id")
                .param("id", eventId).update()).hasMessageContaining("append-only");
        assertThatThrownBy(() -> jdbc.sql("DELETE FROM security_audit_event WHERE id = :id")
                .param("id", eventId).update()).hasMessageContaining("append-only");
    }

    @Test
    void rejectsInlineSecretMaterialAndNeverCopiesItIntoAuditStorage() throws Exception {
        String material = "do-not-persist-this-value";
        mvc.perform(post("/api/v1/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "name": "Unsafe",
                                  "tasks": [{
                                    "key": "PAY", "name": "Pay", "type": "PAYMENT",
                                    "configuration": {"apiKey": "%s"}
                                  }],
                                  "dependencies": []
                                }
                                """.formatted(material)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"));

        List<String> auditText = jdbc.sql("""
                SELECT actor || action || target_type || COALESCE(target_id, '') || http_path
                  FROM security_audit_event
                """).query(String.class).list();
        assertThat(auditText).allSatisfy(value -> assertThat(value).doesNotContain(material));
    }
}
