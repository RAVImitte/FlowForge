package io.flowforge.controlplane;

import io.flowforge.application.workflow.WorkflowConflictException;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.TaskDependency;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.domain.workflow.WorkflowVersionStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
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

    @Test
    void persistsPublishesAndCreatesANewDraftWithoutMutatingPublishedVersion() {
        WorkflowDefinition draft = service.create(orderWorkflow("Initial"));
        assertThat(draft.definitionVersion()).isEqualTo(1);
        assertThat(draft.versionStatus()).isEqualTo(WorkflowVersionStatus.DRAFT);

        WorkflowDefinition published = service.publish(draft.id(), draft.lockVersion());
        assertThat(published.versionStatus()).isEqualTo(WorkflowVersionStatus.PUBLISHED);
        assertThat(published.publishedAt()).isNotNull();

        WorkflowDefinition nextDraft = service.update(
                published.id(),
                published.lockVersion(),
                orderWorkflow("Second version")
        );
        assertThat(nextDraft.definitionVersion()).isEqualTo(2);
        assertThat(nextDraft.versionStatus()).isEqualTo(WorkflowVersionStatus.DRAFT);
        assertThat(nextDraft.description()).isEqualTo("Second version");
        assertThat(service.list(0, 20).totalElements()).isEqualTo(1);
    }

    @Test
    void rejectsAStaleWriterAndArchivesWithoutDeletingHistory() {
        WorkflowDefinition created = service.create(orderWorkflow("Archive me"));

        assertThatThrownBy(() -> service.update(created.id(), 99, orderWorkflow("stale")))
                .isInstanceOf(WorkflowConflictException.class);

        service.archive(created.id(), created.lockVersion());
        assertThatThrownBy(() -> service.get(created.id()))
                .hasMessageContaining("Workflow not found");
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
}
