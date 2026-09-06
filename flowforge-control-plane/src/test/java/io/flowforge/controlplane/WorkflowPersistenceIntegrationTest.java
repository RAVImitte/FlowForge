package io.flowforge.controlplane;

import io.flowforge.application.workflow.WorkflowConflictException;
import io.flowforge.application.workflow.WorkflowNotFoundException;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.tenancy.TenantId;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.TaskReliabilityPolicy;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.domain.workflow.WorkflowVersionStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
@Transactional
class WorkflowPersistenceIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    WorkflowService service;

    @Autowired
    JdbcClient jdbc;

    @Test
    void isolatesWorkflowReadsAndMutationsByTenant() {
        TenantId tenantA = registerTenant("merchant-a", "Merchant A");
        TenantId tenantB = registerTenant("merchant-b", "Merchant B");
        WorkflowDefinition workflowA = service.create(tenantA, orderWorkflow("Tenant A workflow"));
        WorkflowDefinition workflowB = service.create(tenantB, orderWorkflow("Tenant B workflow"));

        assertThat(workflowA.tenantId()).isEqualTo(tenantA);
        assertThat(workflowB.tenantId()).isEqualTo(tenantB);
        assertThat(service.list(tenantA, 0, 20).items())
                .extracting(WorkflowDefinition::id)
                .containsExactly(workflowA.id());
        assertThat(service.list(tenantB, 0, 20).items())
                .extracting(WorkflowDefinition::id)
                .containsExactly(workflowB.id());

        assertThatThrownBy(() -> service.get(tenantB, workflowA.id()))
                .isInstanceOf(WorkflowNotFoundException.class);
        assertThatThrownBy(() -> service.update(
                tenantB, workflowA.id(), workflowA.lockVersion(), orderWorkflow("Cross-tenant update")
        )).isInstanceOf(WorkflowNotFoundException.class);
        assertThatThrownBy(() -> service.publish(tenantB, workflowA.id(), workflowA.lockVersion()))
                .isInstanceOf(WorkflowNotFoundException.class);
        assertThatThrownBy(() -> service.archive(tenantB, workflowA.id(), workflowA.lockVersion()))
                .isInstanceOf(WorkflowNotFoundException.class);

        assertThat(service.get(tenantA, workflowA.id()).description()).isEqualTo("Tenant A workflow");
    }

    @Test
    void assignsTheTransitionalLocalTenantToNewWorkflowWrites() {
        WorkflowDefinition workflow = service.create(TenantId.LOCAL, orderWorkflow("Tenant foundation"));

        String tenantId = jdbc.sql("SELECT tenant_id FROM workflow_definition WHERE id = :id")
                .param("id", workflow.id())
                .query(String.class)
                .single();
        long registryRows = jdbc.sql("SELECT COUNT(*) FROM tenant_registry WHERE tenant_id = 'local'")
                .query(Long.class)
                .single();

        assertThat(tenantId).isEqualTo("local");
        assertThat(registryRows).isEqualTo(1);
    }

    @Test
    void persistsPublishesAndCreatesANewDraftWithoutMutatingPublishedVersion() {
        WorkflowDefinition draft = service.create(TenantId.LOCAL, orderWorkflow("Initial"));
        assertThat(draft.definitionVersion()).isEqualTo(1);
        assertThat(draft.versionStatus()).isEqualTo(WorkflowVersionStatus.DRAFT);

        WorkflowDefinition published = service.publish(TenantId.LOCAL, draft.id(), draft.lockVersion());
        assertThat(published.versionStatus()).isEqualTo(WorkflowVersionStatus.PUBLISHED);
        assertThat(published.publishedAt()).isNotNull();

        WorkflowDefinition nextDraft = service.update(
                TenantId.LOCAL,
                published.id(),
                published.lockVersion(),
                orderWorkflow("Second version")
        );
        assertThat(nextDraft.definitionVersion()).isEqualTo(2);
        assertThat(nextDraft.versionStatus()).isEqualTo(WorkflowVersionStatus.DRAFT);
        assertThat(nextDraft.description()).isEqualTo("Second version");
        assertThat(service.list(TenantId.LOCAL, 0, 20).totalElements()).isEqualTo(1);
    }

    @Test
    void rejectsAStaleWriterAndArchivesWithoutDeletingHistory() {
        WorkflowDefinition created = service.create(TenantId.LOCAL, orderWorkflow("Archive me"));

        assertThatThrownBy(() -> service.update(
                TenantId.LOCAL, created.id(), 99, orderWorkflow("stale")))
                .isInstanceOf(WorkflowConflictException.class);

        service.archive(TenantId.LOCAL, created.id(), created.lockVersion());
        assertThatThrownBy(() -> service.get(TenantId.LOCAL, created.id()))
                .hasMessageContaining("Workflow not found");
    }

    @Test
    void roundTripsTaskReliabilityPolicyThroughPostgres() {
        TaskReliabilityPolicy policy = new TaskReliabilityPolicy(
                5,
                Duration.ofSeconds(1),
                2.0,
                Duration.ofMinutes(1),
                0.25,
                Duration.ofSeconds(30),
                Set.of("TIMEOUT", "GATEWAY_UNAVAILABLE")
        );
        WorkflowDraft draft = new WorkflowDraft(
                "Reliable payment",
                null,
                List.of(new TaskDefinition(
                        "PROCESS_PAYMENT", "Process payment", "HTTP", Map.of(), policy)),
                List.of()
        );

        WorkflowDefinition stored = service.create(TenantId.LOCAL, draft);
        WorkflowDefinition reloaded = service.get(TenantId.LOCAL, stored.id());

        assertThat(reloaded.tasks().getFirst().reliabilityPolicy()).isEqualTo(policy);
    }

    private static WorkflowDraft orderWorkflow(String description) {
        return new WorkflowDraft(
                "Order processing",
                description,
                List.of(
                        new TaskDefinition("VALIDATE_ORDER", "Validate order", "NOOP", Map.of()),
                        new TaskDefinition("PROCESS_PAYMENT", "Process payment", "HTTP", Map.of("uri", "/payments"))
                ),
                List.of(new TaskDependency("PROCESS_PAYMENT", "VALIDATE_ORDER"))
        );
    }

    private TenantId registerTenant(String tenantId, String displayName) {
        jdbc.sql("""
                INSERT INTO tenant_registry(tenant_id, display_name, status)
                VALUES (:tenantId, :displayName, 'ACTIVE')
                """)
                .param("tenantId", tenantId)
                .param("displayName", displayName)
                .update();
        return new TenantId(tenantId);
    }
}
