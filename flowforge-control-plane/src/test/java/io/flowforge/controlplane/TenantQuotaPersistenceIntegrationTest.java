package io.flowforge.controlplane;

import io.flowforge.application.coordination.TokenBucketPolicy;
import io.flowforge.application.execution.ExecutionRepository;
import io.flowforge.application.tenancy.TenantQuota;
import io.flowforge.application.tenancy.TenantQuotaConflictException;
import io.flowforge.application.tenancy.TenantQuotaExceededException;
import io.flowforge.application.tenancy.TenantQuotaPolicy;
import io.flowforge.application.tenancy.TenantQuotaService;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.scheduling.enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false",
        "flowforge.leases.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class TenantQuotaPersistenceIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired TenantQuotaService quotas;
    @Autowired WorkflowService workflows;
    @Autowired ExecutionRepository executions;
    @Autowired JdbcClient jdbc;

    @Test
    void persistsVersionedOverridesAndFencesConcurrentStaleWriters() throws Exception {
        TenantId tenant = registerTenant("quota-versioned", "Quota Versioned");
        assertThat(quotas.get(tenant))
                .extracting(TenantQuota::version, TenantQuota::configured)
                .containsExactly(0L, false);

        TenantQuota created = quotas.update(tenant, policy(10, 10, 100, 100), 0);
        assertThat(created.version()).isEqualTo(1);
        assertThat(created.configured()).isTrue();

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> updateOutcome(tenant, 4, ready, start));
            var second = executor.submit(() -> updateOutcome(tenant, 6, ready, start));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(List.of(first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder("UPDATED", "CONFLICT");
        }
        assertThat(quotas.get(tenant).version()).isEqualTo(2);
    }

    @Test
    void enforcesTenantExecutionAndRunningTaskCapsIndependentlyAcrossReplicas() throws Exception {
        TenantId tenantA = registerTenant("quota-runtime-a", "Quota Runtime A");
        TenantId tenantB = registerTenant("quota-runtime-b", "Quota Runtime B");
        quotas.update(tenantA, policy(1, 1, 100, 100), 0);
        quotas.update(tenantB, policy(1, 1, 100, 100), 0);
        WorkflowDefinition workflowA = publish(tenantA, "Quota A");
        WorkflowDefinition workflowB = publish(tenantB, "Quota B");

        CountDownLatch ready = new CountDownLatch(4);
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(4)) {
            var a1 = executor.submit(() -> startOutcome(tenantA, workflowA, "a-1", ready, start));
            var a2 = executor.submit(() -> startOutcome(tenantA, workflowA, "a-2", ready, start));
            var b1 = executor.submit(() -> startOutcome(tenantB, workflowB, "b-1", ready, start));
            var b2 = executor.submit(() -> startOutcome(tenantB, workflowB, "b-2", ready, start));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(List.of(a1.get(), a2.get())).containsExactlyInAnyOrder("STARTED", "QUOTA");
            assertThat(List.of(b1.get(), b2.get())).containsExactlyInAnyOrder("STARTED", "QUOTA");
        }

        assertThat(executions.claimReadyTasks(tenantA, 10, Instant.now())).hasSize(1);
        assertThat(executions.claimReadyTasks(tenantA, 10, Instant.now())).isEmpty();
        assertThat(executions.claimReadyTasks(tenantB, 10, Instant.now())).hasSize(1);
        assertThat(executions.claimReadyTasks(tenantB, 10, Instant.now())).isEmpty();
    }

    private String updateOutcome(
            TenantId tenant, int activeLimit, CountDownLatch ready, CountDownLatch start
    ) throws InterruptedException {
        ready.countDown();
        start.await(5, TimeUnit.SECONDS);
        try {
            quotas.update(tenant, policy(activeLimit, 10, 100, 100), 1);
            return "UPDATED";
        } catch (TenantQuotaConflictException conflict) {
            return "CONFLICT";
        }
    }

    private String startOutcome(
            TenantId tenant, WorkflowDefinition workflow, String key,
            CountDownLatch ready, CountDownLatch start
    ) throws InterruptedException {
        ready.countDown();
        start.await(5, TimeUnit.SECONDS);
        try {
            executions.start(tenant, workflow.id(), key, Instant.now());
            return "STARTED";
        } catch (TenantQuotaExceededException exceeded) {
            return "QUOTA";
        }
    }

    private WorkflowDefinition publish(TenantId tenant, String name) {
        WorkflowDefinition draft = workflows.create(tenant, new WorkflowDraft(
                name, null,
                List.of(
                        new TaskDefinition("ROOT_A", "Root A", "NOOP", Map.of()),
                        new TaskDefinition("ROOT_B", "Root B", "NOOP", Map.of())
                ),
                List.of()
        ));
        return workflows.publish(tenant, draft.id(), draft.lockVersion());
    }

    private TenantId registerTenant(String value, String displayName) {
        jdbc.sql("""
                INSERT INTO tenant_registry(tenant_id, display_name, status)
                VALUES (:tenantId, :displayName, 'ACTIVE')
                ON CONFLICT (tenant_id) DO NOTHING
                """)
                .param("tenantId", value)
                .param("displayName", displayName)
                .update();
        return new TenantId(value);
    }

    private static TenantQuotaPolicy policy(
            int active, int running, int pending, int ready
    ) {
        TokenBucketPolicy rate = new TokenBucketPolicy(100, 100, Duration.ofSeconds(1));
        return new TenantQuotaPolicy(active, running, pending, ready, rate, rate);
    }
}
