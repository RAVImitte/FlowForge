package io.flowforge.controlplane;

import io.flowforge.application.execution.WorkflowNotPublishedException;
import io.flowforge.application.schedule.ScheduleConflictException;
import io.flowforge.application.schedule.ScheduleNotFoundException;
import io.flowforge.application.schedule.WorkflowScheduleService;
import io.flowforge.application.workflow.WorkflowService;
import io.flowforge.application.workflow.WorkflowNotFoundException;
import io.flowforge.domain.schedule.CronSchedule;
import io.flowforge.domain.schedule.MisfirePolicy;
import io.flowforge.domain.schedule.OneTimeSchedule;
import io.flowforge.domain.schedule.ScheduleStatus;
import io.flowforge.domain.schedule.WorkflowSchedule;
import io.flowforge.domain.schedule.WorkflowScheduleDraft;
import io.flowforge.domain.workflow.TaskDefinition;
import io.flowforge.domain.workflow.WorkflowDefinition;
import io.flowforge.domain.workflow.WorkflowDraft;
import io.flowforge.domain.tenancy.TenantId;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "flowforge.execution.dispatch-enabled=false",
        "flowforge.retries.scheduler-enabled=false",
        "flowforge.timeouts.reaper-enabled=false"
})
@Testcontainers(disabledWithoutDocker = true)
class SchedulePersistenceIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine");

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    WorkflowService workflows;

    @Autowired
    WorkflowScheduleService schedules;

    @Autowired
    JdbcClient jdbc;

    @Test
    void isolatesScheduleOwnershipAndWorkflowReferencesByTenant() {
        TenantId tenantA = registerTenant("schedule-merchant-a", "Schedule Merchant A");
        TenantId tenantB = registerTenant("schedule-merchant-b", "Schedule Merchant B");
        WorkflowDefinition workflowA = publishedWorkflow(tenantA, "Tenant A schedule target");
        WorkflowDefinition workflowB = publishedWorkflow(tenantB, "Tenant B schedule target");
        Instant fireAt = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MICROS);
        WorkflowSchedule scheduleA = schedules.create(tenantA, new WorkflowScheduleDraft(
                workflowA.id(), new OneTimeSchedule(fireAt), MisfirePolicy.FIRE_ONCE
        ));
        WorkflowSchedule scheduleB = schedules.create(tenantB, new WorkflowScheduleDraft(
                workflowB.id(), new OneTimeSchedule(fireAt), MisfirePolicy.FIRE_ONCE
        ));

        assertThat(scheduleA.tenantId()).isEqualTo(tenantA);
        assertThat(scheduleB.tenantId()).isEqualTo(tenantB);
        assertThat(schedules.list(tenantA, 0, 20).items())
                .extracting(WorkflowSchedule::id)
                .contains(scheduleA.id())
                .doesNotContain(scheduleB.id());
        assertThatThrownBy(() -> schedules.get(tenantB, scheduleA.id()))
                .isInstanceOf(ScheduleNotFoundException.class);
        assertThatThrownBy(() -> schedules.pause(tenantB, scheduleA.id(), scheduleA.lockVersion()))
                .isInstanceOf(ScheduleNotFoundException.class);
        assertThatThrownBy(() -> schedules.create(tenantB, new WorkflowScheduleDraft(
                workflowA.id(), new OneTimeSchedule(fireAt), MisfirePolicy.FIRE_ONCE
        ))).isInstanceOf(WorkflowNotFoundException.class);
    }

    @Test
    void persistsMutatesAndSoftDeletesSchedulesWithOptimisticConcurrency() {
        WorkflowDefinition workflow = publishedWorkflow("Schedule target");
        Instant fireAt = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MICROS);
        WorkflowSchedule created = schedules.create(TenantId.LOCAL, new WorkflowScheduleDraft(
                workflow.id(), new OneTimeSchedule(fireAt), MisfirePolicy.SKIP
        ));

        assertThat(schedules.get(TenantId.LOCAL, created.id())).isEqualTo(created);
        assertThat(created.nextFireAt()).isEqualTo(fireAt);
        assertThat(created.status()).isEqualTo(ScheduleStatus.ACTIVE);
        assertThat(schedules.list(TenantId.LOCAL, 0, 20).items()).contains(created);

        WorkflowSchedule paused = schedules.pause(TenantId.LOCAL, created.id(), created.lockVersion());
        assertThat(paused.status()).isEqualTo(ScheduleStatus.PAUSED);
        assertThat(paused.lockVersion()).isEqualTo(1);
        assertThatThrownBy(() -> schedules.resume(TenantId.LOCAL, created.id(), created.lockVersion()))
                .isInstanceOf(ScheduleConflictException.class);

        WorkflowSchedule changed = schedules.update(
                TenantId.LOCAL,
                paused.id(),
                paused.lockVersion(),
                new WorkflowScheduleDraft(
                        workflow.id(),
                        new CronSchedule("0 0 * * * *", ZoneId.of("UTC")),
                        MisfirePolicy.FIRE_ONCE
                )
        );
        assertThat(changed.spec()).isInstanceOf(CronSchedule.class);
        assertThat(changed.status()).isEqualTo(ScheduleStatus.PAUSED);

        WorkflowSchedule resumed = schedules.resume(TenantId.LOCAL, changed.id(), changed.lockVersion());
        assertThat(resumed.status()).isEqualTo(ScheduleStatus.ACTIVE);
        assertThat(resumed.nextFireAt()).isAfter(Instant.now());

        schedules.delete(TenantId.LOCAL, resumed.id(), resumed.lockVersion());
        assertThatThrownBy(() -> schedules.get(TenantId.LOCAL, resumed.id()))
                .isInstanceOf(ScheduleNotFoundException.class);
        assertThat(schedules.list(TenantId.LOCAL, 0, 20).items()).doesNotContain(resumed);
    }

    @Test
    void requiresAnActiveWorkflowWithAPublishedVersion() {
        WorkflowDefinition draft = workflows.create(TenantId.LOCAL, workflowDraft("Unpublished"));

        assertThatThrownBy(() -> schedules.create(TenantId.LOCAL, new WorkflowScheduleDraft(
                draft.id(), new OneTimeSchedule(Instant.now().plusSeconds(60)), null
        ))).isInstanceOf(WorkflowNotPublishedException.class);
    }

    private WorkflowDefinition publishedWorkflow(String name) {
        return publishedWorkflow(TenantId.LOCAL, name);
    }

    private WorkflowDefinition publishedWorkflow(TenantId tenantId, String name) {
        WorkflowDefinition draft = workflows.create(tenantId, workflowDraft(name));
        return workflows.publish(tenantId, draft.id(), draft.lockVersion());
    }

    private TenantId registerTenant(String tenantId, String displayName) {
        jdbc.sql("""
                INSERT INTO tenant_registry(tenant_id, display_name, status)
                VALUES (:tenantId, :displayName, 'ACTIVE')
                ON CONFLICT (tenant_id) DO NOTHING
                """)
                .param("tenantId", tenantId)
                .param("displayName", displayName)
                .update();
        return new TenantId(tenantId);
    }

    private static WorkflowDraft workflowDraft(String name) {
        return new WorkflowDraft(
                name,
                null,
                List.of(new TaskDefinition("ROOT", "Root", "NOOP", Map.of())),
                List.of()
        );
    }
}
