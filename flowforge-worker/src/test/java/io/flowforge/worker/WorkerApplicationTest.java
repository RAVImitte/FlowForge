package io.flowforge.worker;

import io.flowforge.messaging.MessageEnvelope;
import io.flowforge.messaging.TaskCommandV1;
import io.flowforge.worker.application.WorkerTaskResult;
import io.flowforge.worker.persistence.WorkerCommandRepository;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "flowforge.kafka.enabled=false"
)
@Testcontainers(disabledWithoutDocker = true)
class WorkerApplicationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    ApplicationContext context;

    @Autowired
    PrometheusMeterRegistry prometheus;

    @Autowired
    WorkerCommandRepository commands;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void startsAsAnIndependentWorkerProcess() {
        HealthIndicator worker = context.getBean("worker", HealthIndicator.class);

        assertThat(worker.health().getStatus().getCode()).isEqualTo("UP");
        assertThat(worker.health().getDetails()).containsEntry("consumerGroup", "flowforge-workers-v1");
        assertThat(context.containsBean("kafka")).isFalse();
        assertThat(prometheus.scrape())
                .contains("flowforge_worker_commands_received_total")
                .contains("application=\"flowforge-worker\"")
                .contains("environment=\"local\"");
    }

    @Test
    void scopesCommandDeduplicationAndResultsByTenant() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID workflowId = UUID.randomUUID();
        UUID taskId = UUID.randomUUID();
        TaskCommandV1 payload = new TaskCommandV1(
                workflowId, taskId, "ROOT", "NOOP", Map.of(), 1, 1
        );
        MessageEnvelope<TaskCommandV1> tenantA = new MessageEnvelope<>(
                eventId,
                TaskCommandV1.EVENT_TYPE,
                TaskCommandV1.SCHEMA_VERSION,
                Instant.parse("2026-09-06T12:00:00Z"),
                workflowId,
                "merchant-a",
                payload
        );
        MessageEnvelope<TaskCommandV1> tenantB = new MessageEnvelope<>(
                eventId,
                TaskCommandV1.EVENT_TYPE,
                TaskCommandV1.SCHEMA_VERSION,
                Instant.parse("2026-09-06T12:00:00Z"),
                workflowId,
                "merchant-b",
                payload
        );

        assertThat(commands.receive(tenantA, objectMapper.writeValueAsString(tenantA), Instant.now()).newlyReceived())
                .isTrue();
        assertThat(commands.receive(tenantB, objectMapper.writeValueAsString(tenantB), Instant.now()).newlyReceived())
                .isTrue();
        UUID resultA = commands.complete(tenantA, WorkerTaskResult.succeeded(), "worker-a", Instant.now());
        UUID resultB = commands.complete(tenantB, WorkerTaskResult.succeeded(), "worker-b", Instant.now());

        assertThat(resultA).isNotEqualTo(resultB);
        assertThat(jdbc.sql("SELECT COUNT(*) FROM worker_command_inbox WHERE event_id = :eventId")
                .param("eventId", eventId).query(Long.class).single()).isEqualTo(2);
        assertThat(jdbc.sql("""
                SELECT COUNT(*) FROM worker_result_outbox
                 WHERE tenant_id IN ('merchant-a', 'merchant-b')
                   AND payload ->> 'tenantId' = tenant_id
                """).query(Long.class).single()).isEqualTo(2);
    }
}
